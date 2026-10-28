"""Estado local sin iniciar cálculos ni descargas."""
import json
import psutil
from pathlib import Path
from src.data import ROOT

state = ROOT/'results/background_status.json'
if state.exists():
    obj = json.loads(state.read_text())
    try:
        process = psutil.Process(obj['pid'])
        active = process.is_running() and process.status() != psutil.STATUS_ZOMBIE
        if obj.get('created'):
            active = active and process.create_time() == obj['created']
    except psutil.NoSuchProcess:
        active = False
    if obj.get('state') == 'running' and not active:
        obj['state'] = 'INTERRUMPIDO (estado guardado sin proceso activo)'
    print('Proceso coordinador:', obj.get('pid'), '|', obj.get('state'), '|', obj.get('stage'))
for dataset in ['hepph', 'spotify', 'wikipedia']:
    result = ROOT/'results'/dataset
    if (result/'complete.json').exists():
        print(dataset, json.loads((result/'complete.json').read_text()))
    elif (result/'running.json').exists():
        print(dataset, json.loads((result/'running.json').read_text()))
    else:
        print(dataset, 'pendiente o preparando datos')
partial = ROOT/'data/raw/enwiki-latest-pages-articles-multistream.xml.bz2.part'
if partial.exists():
    print('Wikipedia descargado:', round(partial.stat().st_size/2**30, 2), 'GiB (archivo parcial)')
cached = list((ROOT/'data/raw/arxiv').glob('batch-*.json'))
print('Lotes arXiv en caché:', len(cached))
