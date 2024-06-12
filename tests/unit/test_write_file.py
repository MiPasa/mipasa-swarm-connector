"""
   Copyright 2020-2024 MiPasa

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""
import pandas as pd
import pytest
import json
import urllib.parse
from io import BytesIO
from mipasa_swarm_connector import SwarmConnection, SwarmTypeError
from .util import ImportErrorMock


def dump_request(lst, content):
    def handler(request, context):
        lst.append((request, context))
        return content
    return handler


def mock_upload(requests_mock, file_name, content=None, status_code=201):
    hash_index = 0
    rs = []

    def handler(request, context):
        nonlocal hash_index

        rs.append((request, context))
        context.status_code = status_code
        if content is None:
            hash_index += 1
            return json.dumps({'reference': 'testhash%d' % hash_index})
        return content

    gateway_url = 'http://not-real-test-gateway-url'

    requests_mock.post(
        '%s/bzz?file_name=%s' % (gateway_url, urllib.parse.quote(file_name)),
        content=handler
    )

    return rs, gateway_url


def test_file_upload(requests_mock):
    rs, url = mock_upload(
        requests_mock,
        'file.bin',
        content=b'{"reference": "testhash"}'
    )

    assert 'testhash' == SwarmConnection(url).write_file(b'test')
    assert len(rs) == 1
    assert rs[0][0].body == b'test'
    assert rs[0][0].headers['swarm-postage-batch-id'] == '0000000000000000000000000000000000000000000000000000000000000000'
    assert rs[0][0].headers['Content-Type'] == 'application/octet-stream'

    rs, url = mock_upload(
        requests_mock,
        'test_file.bin',
        content=b'{"reference": "testhash2"}'
    )

    assert 'testhash2' == SwarmConnection(url).write_file(b'test2', file_name='test_file.bin')
    assert len(rs) == 1
    assert rs[0][0].body == b'test2'
    assert rs[0][0].headers['swarm-postage-batch-id'] == '0000000000000000000000000000000000000000000000000000000000000000'
    assert rs[0][0].headers['Content-Type'] == 'application/octet-stream'

    rs, url = mock_upload(
        requests_mock,
        'test_file.csv',
        content=b'{"reference": "testhash3"}'
    )

    assert 'testhash3' == SwarmConnection(url).write_file(b'test3', file_name='test_file.csv', mime_type='text/csv', batch_id='2')
    assert len(rs) == 1
    assert rs[0][0].body == b'test3'
    assert rs[0][0].headers['swarm-postage-batch-id'] == '2'
    assert rs[0][0].headers['Content-Type'] == 'text/csv'


def test_file_upload_as_csv(requests_mock):
    rs, url = mock_upload(
        requests_mock,
        'file.csv',
        content=b'{"reference": "testhash"}'
    )

    df = pd.DataFrame(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=['a', 'b', 'c']
    )

    assert 'testhash' == SwarmConnection(url).write_file(df)
    assert len(rs) == 1
    assert rs[0][0].body == b'a,b,c\n1,2,3\n4,5,6\n7,8,9\n'
    assert rs[0][0].headers['Content-Type'] == 'text/csv'

    rs.clear()

    df = pd.DataFrame(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=['d', 'e', 'f']
    )

    assert 'testhash' == SwarmConnection(url).write_file(df, as_type='csv')
    assert len(rs) == 1
    assert rs[0][0].body == b'd,e,f\n1,2,3\n4,5,6\n7,8,9\n'
    assert rs[0][0].headers['Content-Type'] == 'text/csv'

    with pytest.raises(SwarmTypeError) as e_info:
        SwarmConnection(url).write_file('test2', as_type='csv')

    assert e_info.value.args[0] == "CSV upload requested, but content is not of type 'DataFrame'."
    assert e_info.value.expected_type == 'DataFrame'
    assert e_info.value.actual_type == 'str'


def test_file_upload_as_parquet(requests_mock):
    rs, url = mock_upload(
        requests_mock,
        'file.parquet',
        content=b'{"reference": "testhash"}'
    )

    # Test that both with PyArrow and FastParquet the result is correctly written.
    for blocked_import in ['pyarrow', 'fastparquet']:
        with ImportErrorMock([blocked_import]):
            rs.clear()

            df = pd.DataFrame(
                [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
                columns=['a', 'b', 'c']
            )

            assert 'testhash' == SwarmConnection(url).write_file(df, as_type='parquet')
            assert len(rs) == 1
            assert rs[0][0].headers['Content-Type'] == 'application/vnd.apache.parquet'

            parsed_df = pd.read_parquet(BytesIO(rs[0][0].body))

            assert df.equals(parsed_df)

    with pytest.raises(SwarmTypeError) as e_info:
        SwarmConnection(url).write_file(b'test2', as_type='parquet')

    assert e_info.value.args[0] == "Parquet upload requested, but content is not of type 'DataFrame'."
    assert e_info.value.expected_type == 'DataFrame'
    assert e_info.value.actual_type == 'bytes'


def test_file_upload_as_json(requests_mock):
    rs, url = mock_upload(
        requests_mock,
        'file.json',
        content=b'{"reference": "testhash"}'
    )

    test_object = {'a': 'b', 'c': 42}

    assert 'testhash' == SwarmConnection(url).write_file(test_object)
    assert len(rs) == 1
    assert json.loads(rs[0][0].body) == test_object
    assert rs[0][0].headers['Content-Type'] == 'application/json'

    rs.clear()

    assert 'testhash' == SwarmConnection(url).write_file(test_object, as_type='json')
    assert len(rs) == 1
    assert json.loads(rs[0][0].body) == test_object
    assert rs[0][0].headers['Content-Type'] == 'application/json'


def test_user_facing_import_error(requests_mock):
    import pandas as pd

    _, url = mock_upload(
        requests_mock,
        'file.csv',
        content=b'{"reference": "testhash"}'
    )

    mock_upload(
        requests_mock,
        'file.parquet',
        content=b'{"reference": "testhash"}'
    )

    with ImportErrorMock('pandas'):
        with pytest.raises(ImportError) as e_info:
            SwarmConnection(url).write_file('test', as_type='csv')

    assert e_info.value.args[0] == 'Pandas is not installed, but required for read_csv and read_parquet functions.'

    with ImportErrorMock('pyarrow'):
        SwarmConnection(url).write_file(pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            columns=['g', 'h', 'i']
        ), as_type='parquet')

    with ImportErrorMock('fastparquet'):
        SwarmConnection(url).write_file(pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            columns=['g', 'h', 'i']
        ), as_type='parquet')

    with ImportErrorMock(['pyarrow', 'fastparquet']):
        with pytest.raises(ImportError) as e_info:
            SwarmConnection(url).write_file(pd.DataFrame(
                [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
                columns=['g', 'h', 'i']
            ), as_type='parquet')

        assert e_info.value.args[0] == 'Neither PyArrow or FastParquet are installed, but required for read_parquet function.'
