"""Adquisición reproducible y extracción incremental."""
import bz2
import hashlib
import json
import os
import re
import shutil
import time
import xml.etree.ElementTree as ET
from pathlib import Path
import requests
import pyarrow as pa
import pyarrow.parquet as pq
import mwparserfromhell

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT/'data/raw'
HEADERS = {'User-Agent': 'AcademicGraphAnalysis/1.0 (university coursework)'}


def write_json(path, value):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp.{os.getpid()}_{time.time_ns()}")
    tmp.write_text(json.dumps(value, indent=2, ensure_ascii=False))
    tmp.replace(path)


def download(url, path):
    path = Path(path); path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.with_suffix(path.suffix+'.json').exists():
        return path
    partial = path.with_suffix(path.suffix+'.part')
    identity_path = path.with_suffix(path.suffix+'.part.json')
    head = requests.head(url, headers=HEADERS, allow_redirects=True, timeout=30)
    if head.status_code in (404, 405):
        # Algunos endpoints de Kaggle aceptan GET pero no HEAD.
        head = requests.get(url, headers=HEADERS, stream=True, timeout=30)
        head.close()
    head.raise_for_status()
    identity = {'url': url, 'etag': head.headers.get('ETag'), 'last_modified': head.headers.get('Last-Modified'),
                'length': head.headers.get('Content-Length')}
    offset = partial.stat().st_size if partial.exists() else 0
    if offset and (not identity_path.exists() or json.loads(identity_path.read_text()) != identity):
        raise RuntimeError('Cambió la identidad remota o falta manifiesto parcial; no se mezclan dumps. Conserva el parcial por separado.')
    write_json(identity_path, identity)
    headers = dict(HEADERS)
    if offset: headers['Range'] = f'bytes={offset}-'
    started = time.monotonic()
    last_progress = started
    with requests.get(url, stream=True, timeout=(30, 180), headers=headers) as response:
        response.raise_for_status()
        append = offset > 0 and response.status_code == 206
        if offset and not append:
            raise RuntimeError('El servidor no respetó la reanudación; se conserva el parcial sin truncarlo.')
        if append and not response.headers.get('Content-Range', '').startswith(f'bytes {offset}-'):
            raise RuntimeError('Rango de descarga inconsistente')
        with partial.open('ab' if append else 'wb') as out:
            for block in response.iter_content(4*1024*1024):
                out.write(block)
                now = time.monotonic()
                if now-last_progress >= 15:
                    out.flush()
                    downloaded = out.tell()
                    write_json(path.with_suffix(path.suffix+'.progress.json'), {
                        'downloaded_bytes': downloaded, 'total_bytes': int(identity['length']) if identity['length'] else None,
                        'resumed_from_bytes': offset, 'bytes_per_second': (downloaded-offset)/(now-started),
                        'updated_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())})
                    last_progress = now
        headers = dict(response.headers)
    if identity['length'] and partial.stat().st_size != int(identity['length']):
        raise RuntimeError('Tamaño final distinto del publicado; se conserva el parcial para inspección.')
    partial.replace(path)
    manifest_file(path, url, headers)
    return path


def manifest_file(path, url, headers=None):
    path = Path(path); digest = hashlib.sha256()
    with path.open('rb') as source:
        for block in iter(lambda: source.read(8*1024*1024), b''):
            digest.update(block)
    write_json(path.with_suffix(path.suffix+'.json'), dict(url=url, bytes=path.stat().st_size,
               sha256=digest.hexdigest(), observed_at=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), headers=headers or {}))


def title_key(title):
    title = str(title).replace('_', ' ').split('#', 1)[0].strip()
    return title[:1].upper()+title[1:]


def page_records(page):
    def field(name):
        return page.findtext('{*}'+name) or ''
    if field('ns') != '0':
        return None
    redir = page.find('{*}redirect')
    text = page.findtext('{*}revision/{*}text') or ''
    links, categories = set(), set()
    for link in mwparserfromhell.parse(text).filter_wikilinks():
        raw = str(link.title).strip()
        if raw.lower().startswith('category:'):
            categories.add(raw.split(':', 1)[1]); continue
        key = title_key(raw.lstrip(':'))
        if key:
            links.add(key)
    return dict(id=field('id'), title=title_key(field('title')),
                redirect=title_key(redir.attrib['title']) if redir is not None else '',
                topic_text=field('title')+' '+' '.join(sorted(categories)), targets=sorted(links))


PAGE_SCHEMA = pa.schema([('id', pa.string()), ('title', pa.string()), ('redirect', pa.string()),
                         ('topic_text', pa.string()), ('targets', pa.list_(pa.string()))])


def extract_wikipedia(source, destination, limit=None, batch_size=2000):
    """Reanudar salida por lotes: se vuelve a recorrer XML, omitiendo lotes confirmados."""
    destination = Path(destination); destination.mkdir(parents=True, exist_ok=True)
    source = Path(source)
    identity = {'source': str(source.resolve()), 'bytes': source.stat().st_size, 'mtime_ns': source.stat().st_mtime_ns,
                'limit': limit, 'batch_size': batch_size, 'parser_version': 1}
    identity_file = destination/'identity.json'
    if identity_file.exists() and json.loads(identity_file.read_text()) != identity:
        raise RuntimeError('Cambió la fuente o configuración de extracción; usa una carpeta nueva.')
    write_json(identity_file, identity)
    done = destination/'complete.json'
    if done.exists():
        return json.loads(done.read_text())
    count = total = batch = 0; rows = []; started = time.perf_counter()
    # Solo saltar lotes completos y contiguos ya confirmados. Sigue siendo
    # necesario descomprimir y leer XML, pero no volver a analizar wikitext.
    saved_batches = 0
    while (destination/f'part-{saved_batches:07d}.parquet').exists():
        if pq.ParquetFile(destination/f'part-{saved_batches:07d}.parquet').metadata.num_rows != batch_size:
            break
        saved_batches += 1
    skip_records = saved_batches*batch_size
    progress_time = started
    def flush():
        nonlocal batch, rows
        target = destination/f'part-{batch:07d}.parquet'
        if not target.exists():
            tmp = target.with_suffix('.tmp')
            pq.write_table(pa.Table.from_pylist(rows, schema=PAGE_SCHEMA), tmp, compression='zstd')
            tmp.replace(target)
        rows = []; batch += 1
        if shutil.disk_usage(destination).free < 15*1024**3:
            raise RuntimeError('Menos de 15 GiB libres: extracción detenida; salida conservada.')
    with source.open('rb') as compressed, bz2.BZ2File(compressed) as xml:
        events = ET.iterparse(xml, events=('start', 'end'))
        _, root = next(events)
        for event, elem in events:
            if event != 'end' or elem.tag.rsplit('}', 1)[-1] != 'page':
                continue
            total += 1
            if count < skip_records and elem.findtext('{*}ns') == '0':
                count += 1
                if count == skip_records:
                    batch = saved_batches
                record = None
            else:
                record = page_records(elem)
            if record:
                rows.append(record); count += 1
                if len(rows) >= batch_size:
                    flush()
            elem.clear(); root.clear()
            now = time.perf_counter()
            if now-progress_time >= 15:
                write_json(destination/'progress.json', {
                    'pages_scanned': total, 'namespace0_seen': count,
                    'saved_records_at_restart': skip_records,
                    'catching_up': count < skip_records,
                    'compressed_bytes_read': compressed.tell(), 'source_bytes': source.stat().st_size,
                    'elapsed_seconds_this_run': now-started,
                    'updated_utc': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())})
                progress_time = now
            if limit and total >= limit:
                break
        compressed_read = compressed.tell()
    if rows:
        flush()
    summary = dict(pages_scanned=total, namespace0=count, seconds=time.perf_counter()-started,
                   compressed_read=compressed_read, parquet_bytes=sum(p.stat().st_size for p in destination.glob('*.parquet')),
                   full=limit is None, source=str(source.resolve()), source_bytes=source.stat().st_size)
    write_json(done, summary)
    return summary


def enrich_hepph(ids, directory, batch_size=100):
    directory = Path(directory); directory.mkdir(parents=True, exist_ok=True)
    ids = sorted(ids)
    for start in range(0, len(ids), batch_size):
        output = directory/f'batch-{start:07d}.json'
        if output.exists():
            continue
        batch = ids[start:start+batch_size]
        mapping = {'hep-ph/'+str(i).zfill(7): i for i in batch}
        response = requests.get('https://export.arxiv.org/api/query',
                                params={'id_list': ','.join(mapping), 'max_results': batch_size},
                                headers={'User-Agent': 'AcademicGraphAnalysis/1.0'}, timeout=90)
        response.raise_for_status()
        root = ET.fromstring(response.content); rows = []
        for entry in root.findall('{http://www.w3.org/2005/Atom}entry'):
            identifier = entry.findtext('{http://www.w3.org/2005/Atom}id') or ''
            key = re.sub(r'v\d+$', '', identifier.split('/abs/')[-1])
            if key in mapping:
                title = entry.findtext('{http://www.w3.org/2005/Atom}title') or ''
                abstract = entry.findtext('{http://www.w3.org/2005/Atom}summary') or ''
                rows.append(dict(id=mapping[key], title=' '.join(title.split()), topic_text=title+' '+abstract))
        write_json(output, rows)
        time.sleep(3.1)
    files = sorted(directory.glob('batch-*.json'))
    rows = [row for p in files for row in json.loads(p.read_text())]
    digest = hashlib.sha256()
    for p in files: digest.update(p.read_bytes())
    write_json(directory/'manifest.json', {'source': 'https://export.arxiv.org/api/query',
               'requested_ids': len(ids), 'matched_ids': len({r['id'] for r in rows}),
               'batch_size': batch_size, 'sha256_concatenated_batches': digest.hexdigest(),
               'observed_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())})
    return rows
