import csv
import gzip
import json
from pathlib import Path
from pyspark.sql import functions as F
from .data import ROOT, write_json

TOPICS = {
    'wikipedia': {'ciencia': r'(?i)\b(science|scientific|physics|chemistry|biology|mathematics|astronomy)\b',
                  'musica': r'(?i)\b(music|musical|musician|album|song|singer)\b',
                  'deportes': r'(?i)\b(sport|sports|football|basketball|olympic|athlete|tennis)\b'},
    'hepph': {'neutrinos': r'(?i)\bneutrinos?\b', 'higgs': r'(?i)\bhiggs\b',
              'supersimetria': r'(?i)\b(supersymmetr\w*|susy|supergravity)\b'},
    'spotify': {'rock': r'(?i)\brock\b', 'pop': r'(?i)\bpop\b',
                'hiphop': r'(?i)\b(hip[ -]?hop|rap)\b'}}


def save_graph(nodes, edges, name, audit):
    out = ROOT/'data/processed'/name
    out.mkdir(parents=True, exist_ok=True)
    nodes = nodes.select('id', 'title', 'topic_text').dropDuplicates(['id']).cache()
    raw_count = edges.count()
    edges = edges.filter(F.col('src') != F.col('dst')).dropDuplicates(['src', 'dst'])
    valid = nodes.select(F.col('id').alias('src'))
    edges = edges.join(valid, 'src').join(nodes.select(F.col('id').alias('dst')), 'dst').cache()
    audit.update(nodes=nodes.count(), raw_edges=raw_count, edges=edges.count())
    nodes.write.mode('overwrite').parquet(str(out/'nodes'))
    edges.select('src', 'dst').write.mode('overwrite').parquet(str(out/'edges'))
    write_json(out/'audit.json', audit)
    nodes.unpersist(); edges.unpersist()
    return out


def hepph(spark, source, metadata=None):
    edges = spark.read.text(str(source)).filter(~F.col('value').startswith('#')).select(
        F.split(F.trim('value'), r'\s+').alias('v')).select(F.col('v')[0].alias('src'), F.col('v')[1].alias('dst'))
    nodes = edges.select(F.col('src').alias('id')).union(edges.select(F.col('dst').alias('id'))).distinct()
    if nodes.count() != 34546 or edges.count() != 421578:
        raise ValueError('HepPh no coincide con los conteos oficiales; comprobar integridad del archivo.')
    if metadata:
        meta = spark.createDataFrame(metadata, 'id string, title string, topic_text string')
        nodes = nodes.join(meta, 'id', 'left').withColumn('title', F.coalesce('title', 'id')).fillna({'topic_text': ''})
    else:
        nodes = nodes.withColumn('title', F.col('id')).withColumn('topic_text', F.lit(''))
    coverage = len({r['id'] for r in (metadata or [])})
    return save_graph(nodes, edges, 'hepph', {'directed': True, 'metadata_count': coverage, 'metadata_coverage': coverage/34546})


def spotify(spark, node_file, edge_file):
    # Los nombres exactos se validan contra los archivos reales antes de ejecutar.
    nodes = spark.read.option('header', True).option('multiLine', True).option('escape', '"').csv(str(node_file))
    edges = spark.read.option('header', True).csv(str(edge_file))
    id_col = next((c for c in ['spotify_id', 'id'] if c in nodes.columns), None)
    name_col = next((c for c in ['name', 'artist_name'] if c in nodes.columns), None)
    pair = next((p for p in [('id_0', 'id_1'), ('source', 'target'), ('src', 'dst')] if all(c in edges.columns for c in p)), None)
    if not id_col or not name_col or not pair:
        raise ValueError(f'Esquema Spotify no reconocido: {nodes.columns}; {edges.columns}')
    genres = 'genres' if 'genres' in nodes.columns else None
    nodes = nodes.select(F.col(id_col).alias('id'), F.col(name_col).alias('title'),
                         (F.col(genres) if genres else F.lit('')).alias('topic_text')).fillna({'topic_text': ''})
    edges = edges.select(F.col(pair[0]).alias('src'), F.col(pair[1]).alias('dst'))
    endpoints = edges.select(F.col('src').alias('id')).union(edges.select(F.col('dst').alias('id'))).distinct()
    missing = endpoints.join(nodes.select('id'), 'id', 'left_anti')
    missing_count = missing.count()
    nodes = nodes.union(missing.withColumn('title', F.col('id')).withColumn('topic_text', F.lit('')))
    # Colaboraciones sin dirección documentada: grafo no dirigido.
    edges = edges.union(edges.select(F.col('dst').alias('src'), F.col('src').alias('dst')))
    return save_graph(nodes, edges, 'spotify', {'directed': False, 'genres_available': bool(genres), 'nodes_without_metadata': missing_count, 'model': 'colaboración recíproca sin pesos'})


def wikipedia(spark, extracted, name='wikipedia'):
    if name == 'wikipedia':
        summary = json.loads((Path(extracted)/'complete.json').read_text())
        if not summary.get('full'):
            raise ValueError('Un piloto no puede registrarse como Wikipedia completa.')
        spark.conf.set('spark.sql.shuffle.partitions', '256')
    pages = spark.read.parquet(str(Path(extracted)/'*.parquet')).cache()
    nodes = pages.filter(F.col('redirect') == '').select('id', 'title', 'topic_text')
    canonical = nodes.select(F.col('title').alias('target'), F.col('id').alias('dst')).cache()
    pending = pages.filter(F.col('redirect') != '').select(F.col('title').alias('alias'), F.col('redirect').alias('target')).cache()
    redirects = pending.count(); resolved = 0
    # Cada vuelta resuelve un nivel. Cuando no hay avance quedan ciclos o destinos inexistentes.
    while pending.count():
        found = pending.join(canonical, 'target').select('alias', 'dst').localCheckpoint(eager=True)
        count = found.count()
        if not count:
            break
        resolved += count
        new_canonical = canonical.union(found.select(F.col('alias').alias('target'), 'dst')).localCheckpoint(eager=True)
        new_pending = pending.join(found.select('alias'), 'alias', 'left_anti').localCheckpoint(eager=True)
        canonical.unpersist(); pending.unpersist(); found.unpersist()
        canonical, pending = new_canonical, new_pending
    raw = pages.filter(F.col('redirect') == '').select(F.col('id').alias('src'), F.explode('targets').alias('target'))
    joined = raw.join(canonical, 'target', 'left').cache()
    unresolved = joined.filter(F.col('dst').isNull()).count()
    out = save_graph(nodes, joined.filter(F.col('dst').isNotNull()).select('src', 'dst'), name,
                     {'directed': True, 'redirects': redirects, 'resolved_redirects': resolved,
                      'unresolved_redirects_cycles_or_missing': redirects-resolved, 'unresolved_links': unresolved,
                      'explicit_wikitext_links_only': True})
    pages.unpersist(); canonical.unpersist(); pending.unpersist(); joined.unpersist()
    return out
