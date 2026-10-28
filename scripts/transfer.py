"""Paquete local de datos/resultados; conserva mtimes ns usados por config.json."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import tarfile

ROOT = Path(__file__).resolve().parents[1]

def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for b in iter(lambda: f.read(8*1024**2), b''):
            h.update(b)
    return h.hexdigest()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('mode', choices=['export', 'import'])
    parser.add_argument('archive', type=Path)
    args = parser.parse_args()
    archive = args.archive.resolve()
    if args.mode == 'export':
        if archive.exists():
            raise FileExistsError(archive)
        roots = [ROOT/'data/processed'/n for n in ['hepph','spotify','wikipedia']]
        roots += [ROOT/'results'/n for n in ['hepph','spotify','wikipedia']]
        files = sorted(p for r in roots for p in r.rglob('*') if p.is_file())
        manifest = []
        with tarfile.open(archive, 'w', format=tarfile.PAX_FORMAT) as tar:
            for p in files:
                if p.is_symlink():
                    raise ValueError('No se admiten enlaces')
                rel = p.relative_to(ROOT).as_posix()
                manifest.append({'path':rel, 'sha256':sha(p), 'mtime_ns':p.stat().st_mtime_ns})
                tar.add(p, arcname=rel, recursive=False)
            import io
            b = json.dumps(manifest).encode()
            info = tarfile.TarInfo('transfer-manifest.json'); info.size = len(b)
            tar.addfile(info, io.BytesIO(b))
        archive.with_suffix(archive.suffix+'.sha256').write_text(sha(archive)+'\n')
        print(json.dumps({'archive':str(archive),'bytes':archive.stat().st_size,'files':len(files)}))
    else:
        expected = archive.with_suffix(archive.suffix+'.sha256').read_text().strip()
        if sha(archive) != expected:
            raise ValueError('SHA256 del paquete no coincide')
        with tarfile.open(archive) as tar:
            manifest = json.load(tar.extractfile('transfer-manifest.json'))
            members = tar.getmembers()
            if len({m.name for m in members}) != len(members):
                raise ValueError('Miembros duplicados')
            entries = {x['path']:x for x in manifest}
            if set(entries) != {m.name for m in members if m.name != 'transfer-manifest.json'}:
                raise ValueError('Inventario no coincide')
            for m in members:
                if m.name == 'transfer-manifest.json': continue
                path = ROOT/m.name
                if not m.isfile() or not path.resolve().is_relative_to(ROOT) or not m.name.startswith(('data/processed/','results/')):
                    raise ValueError('Ruta no admitida')
                if path.exists(): raise FileExistsError(path)
            for m in members:
                if m.name == 'transfer-manifest.json': continue
                path=ROOT/m.name; path.parent.mkdir(parents=True,exist_ok=True)
                import shutil
                with path.open('xb') as f: shutil.copyfileobj(tar.extractfile(m),f)
                entry=entries[m.name]
                if sha(path)!=entry['sha256']: raise ValueError('Archivo corrupto: '+m.name)
                os.utime(path, ns=(entry['mtime_ns'],entry['mtime_ns']))
        print('Importación verificada; ejecutar run.py wikipedia para continuar.')

if __name__ == '__main__': main()
