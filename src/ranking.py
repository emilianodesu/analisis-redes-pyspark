"""Implementaciones Map/Reduce; nunca recogen el grafo en el driver."""
import math
import time
import shutil
from pathlib import Path
from operator import add
from pyspark import StorageLevel


def resource_sample():
    import psutil
    process = psutil.Process()
    rss = 0
    for p in [process]+process.children(recursive=True):
        try:
            rss += p.memory_info().rss
        except psutil.Error:
            pass
    free = shutil.disk_usage(Path.cwd()).free
    if free < 15*1024**3:
        raise RuntimeError('Menos de 15 GiB libres: cálculo detenido para proteger almacenamiento.')
    return {'rss_process_tree_bytes': rss, 'disk_free_bytes': free}


def pagerank(nodes, edges, personalization=None, alpha=.85, tol=1e-8,
             max_iter=200, checkpoint_every=10, on_iteration=None):
    n = nodes.count()
    if not n:
        raise ValueError('Grafo vacío')
    partitions = edges.getNumPartitions()
    base = nodes.map(lambda x: (x, 0.0)).partitionBy(partitions)
    if personalization is None:
        p = base.mapValues(lambda _: 1/n)
    else:
        mass = personalization.values().sum()
        if mass <= 0 or personalization.filter(lambda x: x[1] < 0).take(1):
            raise ValueError('Personalización inválida o sin semillas')
        if personalization.keys().subtract(nodes).take(1):
            raise ValueError('Semilla fuera del grafo')
        p = base.leftOuterJoin(personalization, numPartitions=partitions).mapValues(lambda v: (v[1] or 0)/mass)
    p = p.persist(StorageLevel.MEMORY_AND_DISK)
    degree = edges.mapValues(lambda _: 1).reduceByKey(add, numPartitions=partitions)
    transitions = edges.join(degree, numPartitions=partitions).mapValues(lambda v: (v[0], v[1])).persist(StorageLevel.MEMORY_AND_DISK)
    dangling = base.subtractByKey(degree).persist(StorageLevel.MEMORY_AND_DISK)
    ranks = p.mapValues(float).persist(StorageLevel.MEMORY_AND_DISK)
    history = []
    started = time.perf_counter()
    for iteration in range(1, max_iter+1):
        dm = dangling.join(ranks, numPartitions=partitions).values().map(lambda v: v[1]).sum()
        # Map: repartir rango por arista. Reduce: sumar por destino.
        incoming = transitions.join(ranks, numPartitions=partitions).map(lambda kv: (kv[1][0][0], kv[1][1]/kv[1][0][1])).reduceByKey(add, numPartitions=partitions)
        nxt = p.leftOuterJoin(incoming, numPartitions=partitions).mapValues(lambda v: (1-alpha+alpha*dm)*v[0]+alpha*(v[1] or 0.0)).persist(StorageLevel.MEMORY_AND_DISK)
        if iteration % checkpoint_every == 0:
            nxt.checkpoint()
            nxt.count()  # Materializar el checkpoint del RDD, no solo un descendiente.
        residual = ranks.join(nxt, numPartitions=partitions).values().map(lambda v: abs(v[0]-v[1])).sum()
        history.append(dict(iteration=iteration, residual=residual, seconds=time.perf_counter()-started, **resource_sample()))
        if on_iteration:
            on_iteration(history[-1])
        ranks.unpersist()
        ranks = nxt
        if residual < tol:
            break
    transitions.unpersist(); dangling.unpersist(); p.unpersist()
    return ranks, history


def hits(nodes, edges, tol=1e-8, max_iter=200, checkpoint_every=10, on_iteration=None,
         initial_a=None, initial_h=None, start_iter=1, history_init=None, on_checkpoint=None):
    n = nodes.count()
    if not n:
        raise ValueError('Grafo vacío')
    partitions = edges.getNumPartitions()
    base = nodes.map(lambda x: (x, 0.0)).partitionBy(partitions).persist(StorageLevel.MEMORY_AND_DISK)
    reverse = edges.map(lambda e: (e[1], e[0])).persist(StorageLevel.MEMORY_AND_DISK)
    if initial_h is not None and initial_a is not None:
        h = initial_h
        a = initial_a
    else:
        h = base.mapValues(lambda _: 1/math.sqrt(n)).persist(StorageLevel.MEMORY_AND_DISK)
        a = h.mapValues(float).persist(StorageLevel.MEMORY_AND_DISK)
    history = list(history_init) if history_init else []
    started = time.perf_counter()

    def update(links, scores):
        raw = links.join(scores, numPartitions=partitions).map(lambda kv: (kv[1][0], kv[1][1])).reduceByKey(add, numPartitions=partitions).persist(StorageLevel.MEMORY_AND_DISK)
        norm = math.sqrt(raw.values().map(lambda x: x*x).sum())
        out = base.leftOuterJoin(raw, numPartitions=partitions).mapValues(lambda v: (v[1] or 0.0)/norm if norm else 0.0).persist(StorageLevel.MEMORY_AND_DISK)
        out.count(); raw.unpersist()
        return out

    for iteration in range(start_iter, max_iter+1):
        na = update(edges, h)
        nh = update(reverse, na)
        if iteration % checkpoint_every == 0:
            na.checkpoint(); nh.checkpoint()
            na.count(); nh.count()
        residual = max(a.join(na, numPartitions=partitions).values().map(lambda v: abs(v[0]-v[1])).sum(), h.join(nh, numPartitions=partitions).values().map(lambda v: abs(v[0]-v[1])).sum())
        history.append(dict(iteration=iteration, residual=residual, seconds=time.perf_counter()-started, **resource_sample()))
        if on_iteration:
            on_iteration(history[-1])
        a.unpersist(); h.unpersist()
        a, h = na, nh
        if on_checkpoint and iteration % checkpoint_every == 0:
            on_checkpoint(iteration, a, h, history)
        if residual < tol:
            break
    base.unpersist(); reverse.unpersist()
    return a, h, history
