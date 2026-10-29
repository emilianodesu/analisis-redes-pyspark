import json
import os
import sys
import shutil
import math
from pathlib import Path
from operator import add
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, Window
from .data import ROOT, write_json
from .graphs import TOPICS
from .ranking import pagerank, hits


def session():
    if 'JAVA_HOME' not in os.environ:
        java = sorted((ROOT/'.runtime').glob('jdk-17*/Contents/Home'))
        if not java:
            candidates = [ROOT/'.runtime/jdk17', Path('/home/alanmagno/.runtime/jdk17')]
            java = [c for c in candidates if (c/'bin/java').exists()]
        if java:
            os.environ['JAVA_HOME'] = str(java[-1])
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
    env_tmp = os.environ.get('SPARK_LOCAL_DIRS') or os.environ.get('TMPDIR')
    if env_tmp and Path(env_tmp).exists():
        local_dir = Path(env_tmp)
    elif Path('/mnt/ssd/spark-tmp').exists():
        local_dir = Path('/mnt/ssd/spark-tmp')
    else:
        local_dir = ROOT/'results/spark-tmp'

    n_slots = os.environ.get('NSLOTS')
    master_str = f'local[{n_slots}]' if n_slots else 'local[4]'
    driver_mem = os.environ.get('SPARK_DRIVER_MEMORY') or (f'{min(100, int(n_slots) * 2)}g' if n_slots else '8g')
    parallelism = str(int(n_slots) * 2) if n_slots else '8'
    shuffle_parts = str(max(32, int(n_slots) * 2)) if n_slots else '32'

    extra_java = "-Djava.io.tmpdir=/var/tmp -Dorg.xerial.snappy.tempdir=/var/tmp"

    spark = (SparkSession.builder.master(master_str).appName('T3-AnalisisRedes')
             .config('spark.driver.memory', driver_mem).config('spark.sql.shuffle.partitions', shuffle_parts)
             .config('spark.default.parallelism', parallelism).config('spark.ui.showConsoleProgress', 'false')
             .config('spark.network.timeout', '800s')
             .config('spark.executor.heartbeatInterval', '60s')
             .config('spark.cleaner.referenceTracking.cleanCheckpoints', 'true')
             .config('spark.file.transferTo', 'false')
             .config('spark.driver.extraJavaOptions', extra_java)
             .config('spark.executor.extraJavaOptions', extra_java)
             .config('spark.local.dir', str(local_dir)).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    spark.sparkContext.setCheckpointDir(str(ROOT/'results/checkpoints'))
    return spark


def analyze(spark, name, max_iter=200, tol=1e-8, target=None):
    source = ROOT/'data/processed'/name
    output = ROOT/'results'/name; output.mkdir(parents=True, exist_ok=True)
    signature = [(str(p.relative_to(source)), p.stat().st_size, p.stat().st_mtime_ns)
                 for kind in ['nodes', 'edges'] for p in sorted((source/kind).glob('*.parquet'))]
    config = {'max_iter': max_iter, 'tol': tol, 'alpha': .85, 'audit': json.loads((source/'audit.json').read_text()), 'topics': TOPICS.get(name, TOPICS['wikipedia']),
              'graph_signature': [list(v) for v in signature]}
    if (output/'config.json').exists():
        old = json.loads((output/'config.json').read_text())
        for k in ['max_iter', 'tol', 'alpha', 'topics', 'audit']:
            if old.get(k) != config.get(k):
                raise RuntimeError(f'Parámetro {k} cambió. Usa otra carpeta de resultados antes de recalcular.')
    write_json(output/'config.json', config)
    ndf = spark.read.parquet(str(source/'nodes')).cache()
    edf = spark.read.parquet(str(source/'edges'))
    nodes = ndf.select('id').rdd.map(lambda r: r.id).persist(StorageLevel.MEMORY_AND_DISK)
    partitions = max(8, math.ceil(config['audit']['edges']/1_000_000))
    edges = edf.select('src', 'dst').rdd.map(lambda r: (r.src, r.dst)).partitionBy(partitions).persist(StorageLevel.MEMORY_AND_DISK)
    nodes.count(); edges.count()
    status = {}

    def save(label, scores, history):
        tgt = output/label
        spark.createDataFrame(scores, 'id string, score double').write.mode('overwrite').parquet(str(tgt))
        write_json(output/f'{label}.json', {'history': history, 'converged': history[-1]['residual'] < tol})
        scores.unpersist()

    def ready(label):
        return (output/label/'_SUCCESS').exists() and (output/f'{label}.json').exists()

    def progress(label):
        prog_file = output/f'running_{label}.json' if target else output/'running.json'
        return lambda entry: write_json(prog_file, {'algorithm': label, **entry})

    if target in [None, 'pagerank']:
        if not ready('pagerank'):
            save('pagerank', *pagerank(nodes, edges, max_iter=max_iter, tol=tol, on_iteration=progress('pagerank')))
    if target == 'pagerank':
        return output

    if target in [None, 'hits']:
        if not ready('authority') or not ready('hub'):
            ckpt_dir = output/'_checkpoint_hits'
            initial_a, initial_h, start_iter, hist_init = None, None, 1, None
            if (ckpt_dir/'meta.json').exists() and (ckpt_dir/'a/_SUCCESS').exists() and (ckpt_dir/'h/_SUCCESS').exists():
                try:
                    meta = json.loads((ckpt_dir/'meta.json').read_text())
                    hist_init = meta.get('history', [])
                    start_iter = int(meta.get('iteration', 0)) + 1
                    initial_a = spark.read.parquet(str(ckpt_dir/'a')).rdd.map(lambda r: (r.id, float(r.score))).partitionBy(partitions).persist(StorageLevel.MEMORY_AND_DISK)
                    initial_h = spark.read.parquet(str(ckpt_dir/'h')).rdd.map(lambda r: (r.id, float(r.score))).partitionBy(partitions).persist(StorageLevel.MEMORY_AND_DISK)
                    initial_a.count(); initial_h.count()
                except Exception:
                    initial_a, initial_h, start_iter, hist_init = None, None, 1, None

            def save_checkpoint(iteration, cur_a, cur_h, cur_history):
                try:
                    spark.createDataFrame(cur_a, 'id string, score double').write.mode('overwrite').parquet(str(ckpt_dir/'a'))
                    spark.createDataFrame(cur_h, 'id string, score double').write.mode('overwrite').parquet(str(ckpt_dir/'h'))
                    write_json(ckpt_dir/'meta.json', {'iteration': iteration, 'history': cur_history})
                except Exception as e:
                    print(f"Advertencia al guardar checkpoint HITS: {e}", file=sys.stderr)

            a, h, history = hits(nodes, edges, max_iter=max_iter, tol=tol, checkpoint_every=5, on_iteration=progress('hits'),
                                 initial_a=initial_a, initial_h=initial_h, start_iter=start_iter,
                                 history_init=hist_init, on_checkpoint=save_checkpoint)
            save('authority', a, history); save('hub', h, history)
            if ckpt_dir.exists():
                shutil.rmtree(ckpt_dir, ignore_errors=True)
    if target == 'hits':
        return output

    labels = ['pagerank', 'authority', 'hub']
    for topic, pattern in config['topics'].items():
        label = 'topic_'+topic
        if target in [None, label]:
            seeds = ndf.filter(F.col('topic_text').rlike(pattern)).select('id')
            count = seeds.count()
            status[topic] = {'seeds': count, 'rule': pattern,
                             'examples': [r.asDict() for r in ndf.join(seeds, 'id').select('id', 'title').orderBy('id').limit(5).collect()]}
            if not count:
                status[topic]['status'] = 'PENDIENTE: no hay semillas; no se fabrican resultados'
                continue
            if not ready(label):
                save(label, *pagerank(nodes, edges, seeds.rdd.map(lambda r: (r.id, 1.0)), max_iter=max_iter, tol=tol, on_iteration=progress(label)))
        if target == label:
            return output
        labels.append(label)

    topic_labels = [l for l in labels if l.startswith('topic_')]
    if len(topic_labels) == len(config['topics']):
        combined = None
        for label in topic_labels:
            df = spark.read.parquet(str(output/label))
            combined = df if combined is None else combined.union(df)
        combined.groupBy('id').agg((F.sum('score')/len(topic_labels)).alias('score')).write.mode('overwrite').parquet(str(output/'topic_mixture'))
        labels.append('topic_mixture')
    write_json(output/'topics.json', status)
    tables = {}
    wide = ndf.select('id', 'title', 'topic_text')
    for label in labels:
        scores = spark.read.parquet(str(output/label))
        invalid = scores.filter(F.isnan('score') | F.col('score').isNull() | (F.col('score') < 0) | (F.abs('score') == float('inf'))).count()
        assert invalid == 0 and scores.count() == nodes.count()
        if label not in ['authority', 'hub']:
            assert abs(scores.agg(F.sum('score')).first()[0]-1) < 1e-7
        top = scores.join(ndf, 'id').orderBy(F.desc('score'), 'id').limit(20).toPandas()
        top.to_csv(output/f'top20_{label}.csv', index=False)
        tables[label] = top
        wide = wide.join(scores.withColumnRenamed('score', label), 'id')
    correlations = []
    for i, a in enumerate(labels):
        for b in labels[i+1:]:
            correlations.append({'a': a, 'b': b, 'pearson_scores': wide.stat.corr(a, b),
                                 'top20_overlap': len(set(tables[a].id)&set(tables[b].id))/min(20, nodes.count())})
    write_json(output/'comparisons.json', correlations)
    # Rango de los resultados temáticos dentro de PageRank base; sin ordenar todo el grafo en el driver.
    base = spark.read.parquet(str(output/'pagerank')).cache()
    for label in topic_labels:
        rows = []
        pattern = config['topics'][label.removeprefix('topic_')]
        topic_top = tables[label]
        selected_ids = spark.createDataFrame([(str(x),) for x in topic_top.id], 'id string')
        reference = {r.id: r.score for r in base.join(selected_ids, 'id').collect()}
        expressions = []
        for rank, row in enumerate(topic_top.itertuples(), 1):
            score = reference[row.id]
            expressions.append(F.sum(F.when((F.col('score') > score) | ((F.col('score') == score) & (F.col('id') < row.id)), 1).otherwise(0)).alias(f'r{rank}'))
        positions = base.agg(*expressions).first()
        for rank, row in enumerate(topic_top.itertuples(), 1):
            global_rank = positions[f'r{rank}']+1
            rows.append({'id': row.id, 'title': row.title, 'topic_rank': rank, 'pagerank_rank': global_rank, 'gain': global_rank-rank})
        write_json(output/f'changes_{label}.json', rows)
        seed_ids = ndf.filter(F.col('topic_text').rlike(pattern)).select('id')
        selected = spark.createDataFrame([(str(x),) for x in topic_top.id], 'id string')
        status[label.removeprefix('topic_')]['top20_seed_fraction'] = selected.join(seed_ids, 'id').count()/len(topic_top)
    write_json(output/'topics.json', status)
    for direction, column in [('in', 'dst'), ('out', 'src')]:
        degree = edf.groupBy(column).count().select(F.col(column).alias('id'), F.col('count').alias('degree'))
        distribution = ndf.select('id').join(degree, 'id', 'left').fillna({'degree': 0}).groupBy('degree').count().orderBy('degree')
        distribution.write.mode('overwrite').parquet(str(output/f'degree_{direction}'))
    write_json(output/'complete.json', {'labels': labels, 'all_topics': len(topic_labels) == len(config['topics']),
                                      'all_converged': all(json.loads((output/f'{l}.json').read_text())['converged'] for l in labels if l != 'topic_mixture')})
    if (output/'running.json').exists(): (output/'running.json').unlink()
    base.unpersist(); ndf.unpersist(); edf.unpersist(); nodes.unpersist(); edges.unpersist()
    return output
