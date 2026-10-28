import csv
import json
from pathlib import Path
from src.pipeline import session, analyze
from src.graphs import spotify, save_graph
from src.data import ROOT


def test_missing_artist_and_end_to_end(tmp_path):
    spark = session()
    try:
        node = tmp_path/'nodes.csv'; edge = tmp_path/'edges.csv'
        node.write_text('spotify_id,name,genres\na,A,rock\nb,B,pop\n')
        edge.write_text('id_0,id_1\na,b\nb,c\n')
        # Guardar el fixture en su propio nombre, sin tocar Spotify real.
        import src.graphs as graphs
        original_save = graphs.save_graph
        graphs.save_graph = lambda nodes, edges, name, audit: original_save(nodes, edges, '_test_spotify', audit)
        try:
            out = spotify(spark, node, edge)
        finally:
            graphs.save_graph = original_save
        audit = json.loads((out/'audit.json').read_text())
        assert audit['nodes'] == 3 and audit['nodes_without_metadata'] == 1 and audit['edges'] == 4
        name = '_test_e2e'
        nodes = spark.createDataFrame([('a', 'A', 'Science'), ('b', 'B', 'Music'), ('c', 'C', 'Sports'), ('d', 'D', '')], 'id string, title string, topic_text string')
        edges = spark.createDataFrame([('a', 'b'), ('b', 'c'), ('c', 'a'), ('c', 'b')], 'src string, dst string')
        import shutil
        result = ROOT/'results'/name
        if result.exists(): shutil.rmtree(result)  # Solo artefactos de esta prueba.
        save_graph(nodes, edges, name, {})
        output = analyze(spark, name, max_iter=3)
        status = json.loads((output/'complete.json').read_text())
        assert status['all_topics']
        assert not status['all_converged']
        assert len(list(output.glob('top20_*.csv'))) == 7
        assert len(json.loads((output/'changes_topic_ciencia.json').read_text())) == 4
        # Una segunda ejecución debe reutilizar rankings terminados, sin cambiar sus timestamps.
        before = (output/'pagerank.json').stat().st_mtime_ns
        analyze(spark, name, max_iter=3)
        assert (output/'pagerank.json').stat().st_mtime_ns == before
    finally:
        spark.stop()
