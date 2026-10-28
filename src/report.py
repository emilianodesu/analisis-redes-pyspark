"""Presentación de resultados existentes: no fabrica ni oculta etapas pendientes."""
import json
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
import pyarrow.parquet as pq
import pyarrow.dataset as ds
import heapq
from IPython.display import display, Markdown
from .data import ROOT


def show_dataset(name):
    folder = ROOT/'results'/name
    audit = ROOT/'data/processed'/name/'audit.json'
    if not audit.exists():
        display(Markdown('**PENDIENTE: no hay un grafo completo procesado.**'))
        error = ROOT/'results'/f'{name}_error.json'
        if error.exists(): print(json.loads(error.read_text())['error'])
        return
    display(pd.DataFrame([json.loads(audit.read_text())]))
    completed = folder/'complete.json'
    if not completed.exists():
        display(Markdown('**Ejecución incompleta: se muestran únicamente etapas terminadas.**'))
    else:
        display(pd.DataFrame([json.loads(completed.read_text())]))
    # Durante una ejecución larga mostrar rankings ya terminados, sin cargar
    # vectores completos en Pandas ni esperar a los demás algoritmos.
    for label in ['pagerank', 'authority', 'hub']:
        scores_path = folder/label
        if (scores_path/'_SUCCESS').exists() and not (folder/f'top20_{label}.csv').exists():
            best = []
            for batch in ds.dataset(scores_path, format='parquet').to_batches(columns=['id', 'score'], batch_size=65536):
                rows = zip(batch.column('id').to_pylist(), batch.column('score').to_pylist())
                best = heapq.nsmallest(20, best+[(i, s) for i, s in rows], key=lambda v: (-v[1], v[0]))
            selected = [i for i, _ in best]
            metadata = ds.dataset(ROOT/'data/processed'/name/'nodes', format='parquet').to_table(
                columns=['id', 'title'], filter=ds.field('id').isin(selected)).to_pandas()
            display(Markdown('### '+label+' — ranking terminado; otras etapas pendientes'))
            display(pd.DataFrame(best, columns=['id', 'score']).merge(metadata, on='id', how='left'))
    for path in sorted(folder.glob('top20_*.csv')):
        display(Markdown('### '+path.stem.replace('top20_', '')))
        display(pd.read_csv(path, dtype={'id': str}).drop(columns=['topic_text'], errors='ignore'))
    histories = []
    for path in folder.glob('*.json'):
        obj = json.loads(path.read_text())
        if isinstance(obj, dict) and 'history' in obj:
            histories.append((path.stem, obj))
    if histories:
        fig, ax = plt.subplots(figsize=(8, 4))
        timing = []
        for label, obj in histories:
            df = pd.DataFrame(obj['history'])
            ax.semilogy(df.iteration, df.residual.clip(lower=1e-16), label=label)
            timing.append(dict(algoritmo=label, iteraciones=len(df), segundos=df.seconds.iloc[-1], convergio=obj['converged'],
                               rss_muestreado_max_GiB=df.rss_process_tree_bytes.max()/2**30 if 'rss_process_tree_bytes' in df else None))
        ax.axhline(1e-8, color='black', linestyle='--', linewidth=.7)
        ax.set(xlabel='Iteración', ylabel='Residual L1', title=f'Convergencia — {name}')
        ax.legend(); fig.tight_layout(); display(fig); plt.close(fig)
        display(pd.DataFrame(timing))
    if (folder/'comparisons.json').exists():
        display(Markdown('### Comparación: correlación de puntuaciones y coincidencia del top 20'))
        comparison = pd.read_json(folder/'comparisons.json')
        display(comparison)
        if completed.exists():
            leaders = {label: pd.read_csv(folder/f'top20_{label}.csv').iloc[0]['title']
                       for label in ['pagerank', 'authority', 'hub']}
            pair = comparison[(comparison.a == 'pagerank') & (comparison.b == 'authority')].iloc[0]
            display(Markdown(
                f"**Interpretación de los resultados obtenidos.** PageRank coloca primero a **{leaders['pagerank']}**; "
                f"HITS identifica a **{leaders['authority']}** como primera autoridad y a **{leaders['hub']}** como primer hub. "
                f"PageRank y autoridades comparten {round(pair.top20_overlap*20)} de los primeros 20 nodos "
                f"y su correlación de puntuaciones es {pair.pearson_scores:.3f}. "
                "Las clasificaciones reflejan criterios estructurales distintos; no son medidas intercambiables de calidad."
            ))
    if (folder/'topics.json').exists():
        display(Markdown('### Cobertura temática y reglas'))
        display(pd.DataFrame(json.loads((folder/'topics.json').read_text())).T)
    for path in sorted(folder.glob('changes_*.json')):
        display(Markdown('### Cambio de posición: '+path.stem))
        display(pd.read_json(path))
    for direction in ['in', 'out']:
        path = folder/f'degree_{direction}'
        if path.exists():
            # Solo distribución agregada, nunca el grafo completo.
            df = pq.read_table(path).to_pandas().sort_values('degree')
            positive = df[df.degree > 0]
            fig, ax = plt.subplots(figsize=(7, 3))
            ax.loglog(positive.degree, positive['count'], '.', alpha=.6)
            ax.set(xlabel=f'Grado {direction}', ylabel='Número de nodos', title=f'{name}: distribución de grado')
            display(fig); plt.close(fig)


def execute_notebook():
    import nbformat
    from nbclient import NotebookClient
    path = ROOT/'analisis_redes.ipynb'
    nb = nbformat.read(path, as_version=4)
    client = NotebookClient(nb, timeout=None, kernel_name='python3', resources={'metadata': {'path': str(ROOT)}})
    client.execute()
    nbformat.write(nb, path)
