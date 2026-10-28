import json
import os
import sys
import math
from pathlib import Path
from operator import add
from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, Window
from .data import ROOT, write_json
from .graphs import TOPICS
from .ranking import pagerank, hits


def session():
    java = sorted((ROOT/'.runtime').glob('jdk-17*/Contents/Home'))
    if java:
        os.environ['JAVA_HOME'] = str(java[-1])
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ.setdefault('SPARK_LOCAL_IP', '127.0.0.1')
    spark = (SparkSession.builder.master('local[4]').appName('T3-AnalisisRedes')
             .config('spark.driver.memory', '10g').config('spark.sql.shuffle.partitions', '32')
             .config('spark.default.parallelism', '8').config('spark.ui.showConsoleProgress', 'false')
             .config('spark.cleaner.referenceTracking.cleanCheckpoints', 'true')
             .config('spark.local.dir', str(ROOT/'results/spark-tmp')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    spark.sparkContext.setCheckpointDir(str(ROOT/'results/checkpoints'))
    return spark


def analyze(spark, name, max_iter=200, tol=1e-8):
    source = ROOT/'data/processed'/name
    output = ROOT/'results'/name; output.mkdir(parents=True, exist_ok=True)
    signature = [(str(p.relative_to(source)), p.stat().st_size, p.stat().st_mtime_ns)
                 for kind in ['nodes', 'edges'] for p in sorted((source/kind).glob('*.parquet'))]
    config = {'max_iter': max_iter, 'tol': tol, 'alpha': .85, 'audit': json.loads((source/'audit.json').read_text()), 'topics': TOPICS.get(name, TOPICS['wikipedia']),
              'graph_signature': [list(v) for v in signature]}
    if (output/'config.json').exists() and json.loads((output/'config.json').read_text()) != config:
        raise RuntimeError('Configuración cambió. Usa otra carpeta de resultados antes de recalcular.')
    write_json(output/'config.json', config)
    ndf = spark.read.parquet(str(source/'nodes')).cache()
    edf = spark.read.parquet(str(source/'edges')).cache()
    nodes = ndf.select('id').rdd.map(lambda r: r.id).persist(StorageLevel.MEMORY_AND_DISK)
    partitions = max(8, math.ceil(config['audit']['edges']/1_000_000))
    edges = edf.select('src', 'dst').rdd.map(lambda r: (r.src, r.dst)).partitionBy(partitions).persist(StorageLevel.MEMORY_AND_DISK)
    nodes.count(); edges.count()
    status = {}

    def save(label, scores, history):
        target = output/label
        spark.createDataFrame(scores, 'id string, score double').write.mode('overwrite').parquet(str(target))
        write_json(output/f'{label}.json', {'history': history, 'converged': history[-1]['residual'] < tol})
        scores.unpersist()

    def ready(label):
        return (output/label/'_SUCCESS').exists() and (output/f'{label}.json').exists()

    def progress(label):
        return lambda entry: write_json(output/'running.json', {'algorithm': label, **entry})

    if not ready('pagerank'):
        save('pagerank', *pagerank(nodes, edges, max_iter=max_iter, tol=tol, on_iteration=progress('pagerank')))
    if not ready('authority') or not ready('hub'):
        a, h, history = hits(nodes, edges, max_iter=max_iter, tol=tol, on_iteration=progress('hits'))
        save('authority', a, history); save('hub', h, history)
    labels = ['pagerank', 'authority', 'hub']
    for topic, pattern in config['topics'].items():
        seeds = ndf.filter(F.col('topic_text').rlike(pattern)).select('id')
        count = seeds.count()
        status[topic] = {'seeds': count, 'rule': pattern,
                         'examples': [r.asDict() for r in ndf.join(seeds, 'id').select('id', 'title').orderBy('id').limit(5).collect()]}
        if not count:
            status[topic]['status'] = 'PENDIENTE: no hay semillas; no se fabrican resultados'
            continue
        label = 'topic_'+topic
        if not ready(label):
            save(label, *pagerank(nodes, edges, seeds.rdd.map(lambda r: (r.id, 1.0)), max_iter=max_iter, tol=tol, on_iteration=progress(label)))
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
