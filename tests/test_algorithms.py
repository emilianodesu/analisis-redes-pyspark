import bz2
import math
import xml.etree.ElementTree as ET
import networkx as nx
import numpy as np
import pytest
from src.pipeline import session
from src.ranking import pagerank, hits
from src.data import page_records, title_key, extract_wikipedia


@pytest.fixture(scope='module')
def spark():
    s = session()
    yield s
    s.stop()


def test_pagerank_and_personalization(spark):
    g = nx.DiGraph([(0, 1), (1, 2), (2, 0), (2, 3), (4, 5), (5, 4)])
    g.add_node(6)
    sc = spark.sparkContext
    nodes = sc.parallelize(list(g), 2)
    edges = sc.parallelize(list(g.edges), 2)
    results = []
    for p in [None, {x: 1 for x in g}, {0: 1, 4: 2}]:
        scores, hist = pagerank(nodes, edges, sc.parallelize(list(p.items()), 2) if p else None, tol=1e-9)
        actual = dict(scores.collect()); scores.unpersist()
        expected = nx.pagerank(g, personalization=p, dangling=p, tol=1e-12, max_iter=1000)
        assert sum(abs(actual[k]-expected[k]) for k in g) < 1e-7
        assert abs(sum(actual.values())-1) < 1e-10
        assert hist[-1]['residual'] < 1e-9
        results.append(actual)
    assert results[0] == pytest.approx(results[1])


def test_hits(spark):
    g = nx.DiGraph([(0, 1), (0, 2), (1, 2), (2, 0), (3, 2)])
    g.add_node(4)
    sc = spark.sparkContext
    a, h, history = hits(sc.parallelize(list(g), 2), sc.parallelize(list(g.edges), 2), checkpoint_every=2)
    actual_a, actual_h = dict(a.collect()), dict(h.collect())
    expected_h, expected_a = nx.hits(g, max_iter=1000, tol=1e-12)
    for actual, expected in [(actual_a, expected_a), (actual_h, expected_h)]:
        norm = math.sqrt(sum(v*v for v in expected.values()))
        assert max(abs(actual[k]-expected[k]/norm) for k in g) < 1e-6
        assert actual[4] == 0
    a.unpersist(); h.unpersist()


def test_wikitext():
    page = ET.fromstring('<page><title>test_page</title><ns>0</ns><id>1</id><revision><text>[[other_page#A|B]] [[Category:Science]] [[other_page]]</text></revision></page>')
    record = page_records(page)
    assert record['targets'] == ['Other page']
    assert 'Science' in record['topic_text']
    assert title_key('hello_world#x') == 'Hello world'


def test_xml_resume_and_redirects(tmp_path, spark):
    from src.graphs import wikipedia
    from src.data import ROOT
    xml = '<mediawiki>' + ''.join([
        '<page><title>A</title><ns>0</ns><id>1</id><revision><text>[[R]] [[Unknown]] [[A]]</text></revision></page>',
        '<page><title>B</title><ns>0</ns><id>2</id><revision><text/></revision></page>',
        '<page><title>R</title><ns>0</ns><id>3</id><redirect title="R2"/><revision><text/></revision></page>',
        '<page><title>R2</title><ns>0</ns><id>4</id><redirect title="B"/><revision><text/></revision></page>',
        '<page><title>C</title><ns>0</ns><id>5</id><redirect title="C"/><revision><text/></revision></page>'
    ])+'</mediawiki>'
    source = tmp_path/'test.bz2'; source.write_bytes(bz2.compress(xml.encode()))
    target = tmp_path/'pages'
    first = extract_wikipedia(source, target, batch_size=2)
    assert extract_wikipedia(source, target, batch_size=2) == first
    out = wikipedia(spark, target, name='_test_wiki')
    edges = spark.read.parquet(str(out/'edges')).collect()
    assert [(r.src, r.dst) for r in edges] == [('1', '2')]
    import json
    audit = json.loads((out/'audit.json').read_text())
    assert audit['resolved_redirects'] == 2
    assert audit['unresolved_redirects_cycles_or_missing'] == 1
