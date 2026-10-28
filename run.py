"""CLI: las mismas etapas invocadas por el notebook."""
import argparse
import gzip
import json
import shutil
import zipfile
import fcntl
from pathlib import Path
from src.data import ROOT, RAW, HEADERS, download, manifest_file, extract_wikipedia, enrich_hepph, write_json

WIKI_URL = 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles-multistream.xml.bz2'
HEP_URL = 'https://snap.stanford.edu/data/cit-HepPh.txt.gz'
SPOTIFY_URL = 'https://www.kaggle.com/api/v1/datasets/download/jfreyberg/spotify-artist-feature-collaboration-network'


def prepare_hepph(spark):
    from src.graphs import hepph
    source = RAW/'cit-HepPh.txt.gz'
    if not source.exists(): download(HEP_URL, source)
    manifest_file(source, HEP_URL)
    with gzip.open(source, 'rt') as f:
        ids = {i for line in f if not line.startswith('#') for i in line.split()}
    try:
        metadata = enrich_hepph(ids, RAW/'arxiv')
    except Exception as exc:
        write_json(ROOT/'results/hepph_metadata_error.json', {'error': str(exc), 'status': 'INCOMPLETO'})
        metadata = [row for p in sorted((RAW/'arxiv').glob('batch-*.json')) for row in json.loads(p.read_text())]
    return hepph(spark, source, metadata)


def prepare_spotify(spark):
    from src.graphs import spotify
    archive = RAW/'spotify.zip'
    target = RAW/'spotify'
    if (target/'nodes.csv').exists() and (target/'edges.csv').exists():
        return spotify(spark, target/'nodes.csv', target/'edges.csv')
    if not archive.exists(): download(SPOTIFY_URL, archive)
    manifest_file(archive, SPOTIFY_URL)
    target.mkdir(exist_ok=True)
    with zipfile.ZipFile(archive) as z:
        for item in z.infolist():
            dest = (target/item.filename).resolve()
            if not dest.is_relative_to(target.resolve()): raise ValueError('Ruta ZIP insegura')
            z.extract(item, target)
    files = sorted(target.rglob('*.csv'))
    node = next((p for p in files if 'node' in p.name.lower()), None)
    edge = next((p for p in files if 'edge' in p.name.lower()), None)
    if node is None or edge is None: raise ValueError(f'Archivos Spotify no reconocidos: {files}')
    return spotify(spark, node, edge)


def pilot():
    import requests
    source = RAW/'wiki-pilot.xml.bz2'
    if not source.exists():
        with requests.get(WIKI_URL, headers={**HEADERS, 'Range': 'bytes=0-67108863'}, stream=True, timeout=90) as r:
            r.raise_for_status()
            if r.status_code != 206: raise RuntimeError('Servidor no admite descarga parcial del piloto')
            with source.open('wb') as out:
                for block in r.iter_content(1024*1024): out.write(block)
    summary = extract_wikipedia(source, ROOT/'data/pilot/wiki-pages', limit=1000)
    response = requests.head(WIKI_URL, headers=HEADERS, timeout=30); response.raise_for_status()
    size = int(response.headers['Content-Length'])
    factor = size / summary['compressed_read']
    # Margen explícito: extracción + grafo, joins, shuffle y checkpoints. Estimación, no garantía.
    parquet_est = summary['parquet_bytes']*factor
    estimate = size + 8*parquet_est + 20*1024**3
    report = dict(summary, full_download_bytes=size, projected_parquet_bytes=parquet_est,
                  projected_seconds=summary['seconds']*factor,
                  estimated_peak_disk_bytes=estimate, free_bytes=shutil.disk_usage(ROOT).free,
                  safety_factor=8, limitation='Primeras 1000 páginas; sesgo por orden y tamaño, estimación aproximada.')
    report['disk_ok'] = estimate < report['free_bytes']
    write_json(ROOT/'results/wiki_pilot.json', report)
    return report


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('stage', choices=['pilot', 'hepph', 'spotify', 'wikipedia', 'all', 'report'])
    args = parser.parse_args()
    (ROOT/'results').mkdir(exist_ok=True)
    if args.stage == 'pilot':
        print(json.dumps(pilot(), indent=2)); return
    if args.stage == 'report':
        from src.report import execute_notebook
        execute_notebook(); return
    from src.pipeline import session, analyze
    from src.graphs import wikipedia
    lock_file = (ROOT/'results/pipeline.lock').open('a+')
    try:
        fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        raise RuntimeError('Ya hay otro pipeline activo en este proyecto.')
    spark = session()
    failures = []
    try:
        for dataset in (['hepph', 'spotify', 'wikipedia'] if args.stage == 'all' else [args.stage]):
            write_json(ROOT/'results/progress.json', {'stage': dataset, 'state': 'running'})
            try:
                processed = ROOT/'data/processed'/dataset
                if not (processed/'audit.json').exists():
                    if dataset == 'hepph': prepare_hepph(spark)
                    elif dataset == 'spotify': prepare_spotify(spark)
                    else:
                        report = pilot()
                        if not report['disk_ok']:
                            raise RuntimeError('Estimación de disco excede espacio libre. Ver results/wiki_pilot.json. No se sustituye por muestra.')
                        source = download(WIKI_URL, RAW/'enwiki-latest-pages-articles-multistream.xml.bz2')
                        extract_wikipedia(source, ROOT/'data/processed/wiki-pages')
                        wikipedia(spark, ROOT/'data/processed/wiki-pages')
                analyze(spark, dataset)
                error_path = ROOT/'results'/f'{dataset}_error.json'
                if error_path.exists(): error_path.unlink()
                from src.report import execute_notebook
                execute_notebook()
            except Exception as exc:
                failures.append(dataset)
                write_json(ROOT/'results'/f'{dataset}_error.json', {'error': str(exc), 'status': 'INCOMPLETO'})
                print(f'{dataset}: {exc}', flush=True)
                if args.stage != 'all': raise
    finally:
        spark.stop()
        lock_file.close()
    write_json(ROOT/'results/progress.json', {'state': 'incomplete' if failures else 'finished', 'failed_datasets': failures})
    if failures:
        raise SystemExit(1)


if __name__ == '__main__': main()
