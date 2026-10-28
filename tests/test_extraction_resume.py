import bz2
from unittest.mock import patch
from src import data


def test_resume_skips_parsing_saved_pages(tmp_path):
    source = tmp_path/'wiki.bz2'
    xml = '<mediawiki>'+''.join(f'<page><title>P{i}</title><ns>0</ns><id>{i}</id><revision><text>[[P0]]</text></revision></page>' for i in range(5))+'</mediawiki>'
    source.write_bytes(bz2.compress(xml.encode()))
    output = tmp_path/'parts'
    first = data.extract_wikipedia(source, output, batch_size=2)
    (output/'complete.json').unlink()
    (output/'part-0000002.parquet').unlink()
    original = data.page_records
    with patch('src.data.page_records', wraps=original) as parser:
        resumed = data.extract_wikipedia(source, output, batch_size=2)
    assert parser.call_count == 1
    assert resumed['namespace0'] == first['namespace0'] == 5
    assert len(list(output.glob('*.parquet'))) == 3
