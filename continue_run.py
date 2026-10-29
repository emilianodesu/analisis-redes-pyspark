"""Continúa el trabajo largo sin ejecutar dos motores Spark simultáneamente.

La configuración de dependencias se genera al iniciar el proceso. Cada PID lleva
su fecha de creación, para no confundirlo con otro proceso que reutilice el PID.
"""
import json
import subprocess
import sys
import time
from pathlib import Path
import psutil
from src.data import ROOT, write_json


def main():
    config = json.loads(Path(sys.argv[1]).read_text())
    state = {'state': 'running', 'steps': [], 'pid': psutil.Process().pid,
             'created': psutil.Process().create_time()}
    target = ROOT/'results/background_status.json'

    def update(stage):
        state['stage'] = stage
        state['updated_utc'] = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        write_json(target, state)

    def wait_for(entry, label):
        update('Esperando '+label)
        while True:
            try:
                process = psutil.Process(entry['pid'])
                if process.create_time() != entry['created'] or process.status() == psutil.STATUS_ZOMBIE:
                    break
            except psutil.NoSuchProcess:
                break
            time.sleep(15)

    def run(label, args):
        update(label)
        with (ROOT/'results'/f'background_{label}.log').open('a') as log:
            result = subprocess.run([sys.executable, *args], cwd=ROOT, stdout=log, stderr=subprocess.STDOUT)
        state['steps'].append({'stage': label, 'returncode': result.returncode})
        write_json(target, state)
        return result.returncode

    for entry in config.get('before_spark', []):
        wait_for(entry, entry['label'])
    for dataset in config.get('stages', ['hepph', 'spotify', 'wikipedia']):
        if dataset not in ['hepph', 'spotify', 'wikipedia']:
            raise ValueError('Dataset no reconocido: '+dataset)
        if dataset == 'wikipedia' and config.get('download'):
            wait_for(config['download'], 'descarga completa de Wikipedia')
        while True:
            ret = run(dataset, ['run.py', dataset])
            if ret == 0:
                break
            update(f'Fallo en {dataset} (codigo {ret}). Reintentando en 15s...')
            time.sleep(15)
    run('notebook', ['run.py', 'report'])
    state['state'] = 'finished' if all(s['returncode'] == 0 for s in state['steps']) else 'incomplete'
    update('Terminado; revisar resultados y convergencia en el notebook')


if __name__ == '__main__':
    main()
