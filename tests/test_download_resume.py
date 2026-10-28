import json
from unittest.mock import patch
import pytest
from src.data import download


class Response:
    def __init__(self, status, headers, body=b''):
        self.status_code, self.headers, self.body = status, headers, body
    def raise_for_status(self): pass
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def iter_content(self, _): yield self.body


def setup_partial(tmp_path):
    path = tmp_path/'dump.bz2'
    path.with_suffix('.bz2.part').write_bytes(b'ab')
    identity = {'url': 'https://example.test/dump', 'etag': 'version1', 'last_modified': 'date', 'length': '4'}
    path.with_suffix('.bz2.part.json').write_text(json.dumps(identity))
    head = Response(200, {'ETag': 'version1', 'Last-Modified': 'date', 'Content-Length': '4'})
    return path, head, identity['url']


def test_resume_preserves_existing_bytes(tmp_path):
    path, head, url = setup_partial(tmp_path)
    with patch('src.data.requests.head', return_value=head), patch('src.data.requests.get', return_value=Response(206, {'Content-Range': 'bytes 2-3/4'}, b'cd')) as get:
        download(url, path)
        assert get.call_args.kwargs['headers']['Range'] == 'bytes=2-'
    assert path.read_bytes() == b'abcd'
    assert json.loads(path.with_suffix('.bz2.json').read_text())['bytes'] == 4


def test_refused_range_does_not_truncate(tmp_path):
    path, head, url = setup_partial(tmp_path)
    with patch('src.data.requests.head', return_value=head), patch('src.data.requests.get', return_value=Response(200, {}, b'abcd')):
        with pytest.raises(RuntimeError, match='reanudación'):
            download(url, path)
    assert path.with_suffix('.bz2.part').read_bytes() == b'ab'
