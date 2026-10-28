"""Descarga pública verificada del estado de migración; usa sólo Python estándar."""
import hashlib
import json
from pathlib import Path
import shutil
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
BASE = 'https://github.com/emilianodesu/analisis-redes-pyspark/releases/download/migration-v1/'

def digest(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        for b in iter(lambda:f.read(8*1024**2),b''): h.update(b)
    return h.hexdigest()

def main():
    manifest=json.loads((ROOT/'handoff/transfer-manifest.json').read_text())
    dest=ROOT/'results/download'; dest.mkdir(parents=True,exist_ok=True)
    archive=ROOT/manifest['archive']
    if archive.exists() and digest(archive)==manifest['archive_sha256']:
        archive.with_suffix('.tar.sha256').write_text(manifest['archive_sha256']+'\n')
        print('El paquete completo ya está verificado.'); return
    for entry in manifest['parts']:
        p=dest/entry['name']
        if p.exists() and p.stat().st_size==entry['bytes'] and digest(p)==entry['sha256']:
            print('Reutilizando',p.name,flush=True); continue
        print('Descargando',p.name,flush=True)
        tmp=p.with_suffix(p.suffix+'.partial')
        with urlopen(Request(BASE+p.name,headers={'User-Agent':'GraphAnalysisTransfer/1.0'}),timeout=120) as response, tmp.open('wb') as f:
            shutil.copyfileobj(response,f,8*1024**2)
        if tmp.stat().st_size!=entry['bytes'] or digest(tmp)!=entry['sha256']:
            raise ValueError('La parte no coincide con el checksum publicado: '+p.name)
        tmp.replace(p)
    tmp=archive.with_suffix('.tar.partial')
    with tmp.open('wb') as out:
        for entry in manifest['parts']:
            with (dest/entry['name']).open('rb') as f: shutil.copyfileobj(f,out,8*1024**2)
    if tmp.stat().st_size!=manifest['archive_bytes'] or digest(tmp)!=manifest['archive_sha256']:
        raise ValueError('Checksum del paquete completo incorrecto')
    tmp.replace(archive)
    archive.with_suffix('.tar.sha256').write_text(manifest['archive_sha256']+'\n')
    print('Paquete completo verificado:',archive)
    print('Importar con: python scripts/transfer.py import transferencia_redes.tar')

if __name__=='__main__': main()
