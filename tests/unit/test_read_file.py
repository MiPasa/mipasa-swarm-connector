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

import pytest
import json
import re
import os
from mipasa_swarm_connector import SwarmConnection, SwarmAPIError, SwarmTypeError
from .util import ImportErrorMock


def mock_bzz_link(requests_mock, files=None):
    if files is None:
        files = dict()

    gateway_url = 'http://not-real-test-gateway-url'
    os.environ['BEE_GATEWAY_URL'] = gateway_url

    path_regex = re.compile('^%s/bzz/.*$' % re.escape(gateway_url))

    def handler(request, context):
        if request.path.startswith('/bzz/'):
            name = request.path.split('/')[2]
            if name in files:
                v = files[name]
                if not isinstance(v, bytes):
                    context.status_code = v.get('status_code', 200)
                    context.headers = v.get('headers', context.headers)
                    return v.get('content', b'')
                context.status_code = 200
                return files[name]

        context.status_code = 404
        return b'{"error": "Not found"}'

    requests_mock.get(path_regex, content=handler)


def test_file_download(requests_mock):
    mock_bzz_link(requests_mock, {'test': b'test'})

    assert b'test' == SwarmConnection().read_file('test')

    with pytest.raises(SwarmAPIError) as e_info:
        SwarmConnection().read_file('test2')

    assert e_info.value.swarm_hash == 'test2'
    assert e_info.value.status_code == 404
    assert e_info.value.args[0] == "Hash 'test2' not found or could not be retrieved from Swarm (code 404)"


def test_file_reinterpret_as_json(requests_mock):
    mock_bzz_link(requests_mock, {
        'test': {
            'headers': {
                'Content-Type': 'application/json'
            },
            'content': json.dumps({'key': 'value', 'a': 2}).encode('utf-8')
        },
        'test2': {
            'headers': {
                'Content-Disposition': 'attachment; filename="file.json"'
            },
            'content': json.dumps({'a': 42}).encode('utf-8')
        },
        'test3': json.dumps({'a': 16}).encode('utf-8'),
        'test4': b'not-json'
    })

    assert {'key': 'value', 'a': 2} == SwarmConnection().read_file('test', as_type='json', verify_type=True)
    assert {'a': 42} == SwarmConnection().read_file('test2', as_type='json', verify_type=True)
    assert {'a': 16} == SwarmConnection().read_file('test3', as_type='json', verify_type=False)

    with pytest.raises(SwarmTypeError) as e_info:
        SwarmConnection().read_file('test3', as_type='json', verify_type=True)

    assert e_info.value.expected_type == 'json'
    assert e_info.value.actual_type == 'bytes'
    assert e_info.value.args[0] == "Hash 'test3' is not of type 'json'"

    with pytest.raises(json.JSONDecodeError):
        SwarmConnection().read_file('test4', as_type='json', verify_type=False)

    assert b'not-json' == SwarmConnection().read_file('test4')


def test_file_reinterpret_as_csv(requests_mock):
    import pandas as pd

    mock_bzz_link(requests_mock, {
        'test': {
            'headers': {
                'Content-Type': 'text/csv'
            },
            'content': b'a,b,c\n1,2,3\n4,5,6\n7,8,9'
        },
        'test2': {
            'headers': {
                'Content-Disposition': 'attachment; filename="file.csv"'
            },
            'content': b'd,e,f\n1,2,3\n4,5,6\n7,8,9'
        },
        'test3': b'g,h,i\n1,2,3\n4,5,6\n7,8,9',
        'test4': b'',
    })

    assert pd.DataFrame(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=['a', 'b', 'c']
    ).equals(SwarmConnection().read_file('test', as_type='csv', verify_type=True))

    assert pd.DataFrame(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=['d', 'e', 'f']
    ).equals(SwarmConnection().read_file('test2', as_type='csv', verify_type=True))

    assert pd.DataFrame(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=['g', 'h', 'i']
    ).equals(SwarmConnection().read_file('test3', as_type='csv', verify_type=False))

    with pytest.raises(SwarmTypeError) as e_info:
        SwarmConnection().read_file('test3', as_type='csv', verify_type=True)

    assert e_info.value.expected_type == 'csv'
    assert e_info.value.actual_type == 'bytes'
    assert e_info.value.args[0] == "Hash 'test3' is not of type 'csv'"

    with pytest.raises(pd.errors.EmptyDataError):
        SwarmConnection().read_file('test4', as_type='csv', verify_type=False)

    assert b'' == SwarmConnection().read_file('test4')


def test_file_reinterpret_as_csv(requests_mock):
    import pandas as pd
    import pyarrow

    mock_bzz_link(requests_mock, {
        'test': {
            'headers': {
                'Content-Type': 'application/parquet'
            },
            'content': pd.DataFrame(
                [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
                columns=['a', 'b', 'c']
            ).to_parquet()
        },
        'test2': {
            'headers': {
                'Content-Disposition': 'attachment; filename="file.parquet"'
            },
            'content': pd.DataFrame(
                [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
                columns=['d', 'e', 'f']
            ).to_parquet()
        },
        'test3': pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            columns=['g', 'h', 'i']
        ).to_parquet(),
        'test4': b'invalid-parquet',
    })

    assert pd.DataFrame(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=['a', 'b', 'c']
    ).equals(SwarmConnection().read_file('test', as_type='parquet', verify_type=True))

    assert pd.DataFrame(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=['d', 'e', 'f']
    ).equals(SwarmConnection().read_file('test2', as_type='parquet', verify_type=True))

    assert pd.DataFrame(
        [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
        columns=['g', 'h', 'i']
    ).equals(SwarmConnection().read_file('test3', as_type='parquet', verify_type=False))

    with pytest.raises(SwarmTypeError) as e_info:
        SwarmConnection().read_file('test3', as_type='parquet', verify_type=True)

    assert e_info.value.expected_type == 'parquet'
    assert e_info.value.actual_type == 'bytes'
    assert e_info.value.args[0] == "Hash 'test3' is not of type 'parquet'"

    with pytest.raises(pyarrow.lib.ArrowInvalid):
        SwarmConnection().read_file('test4', as_type='parquet', verify_type=False)

    assert b'invalid-parquet' == SwarmConnection().read_file('test4')


def test_user_facing_import_error(requests_mock):
    import pandas as pd

    mock_bzz_link(requests_mock, {
        'test': b'g,h,i\n1,2,3\n4,5,6\n7,8,9',
        'test2': pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            columns=['g', 'h', 'i']
        ).to_parquet(),
    })

    with ImportErrorMock('pandas'):
        with pytest.raises(ImportError) as e_info:
            SwarmConnection().read_file('test', as_type='csv')

    assert e_info.value.args[0] == 'Pandas is not installed, but required for read_csv and read_parquet functions.'

    with ImportErrorMock('pyarrow'):
        assert pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            columns=['g', 'h', 'i']
        ).equals(SwarmConnection().read_file('test2', as_type='parquet'))

    with ImportErrorMock('fastparquet'):
        assert pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            columns=['g', 'h', 'i']
        ).equals(SwarmConnection().read_file('test2', as_type='parquet'))

    with ImportErrorMock(['pyarrow', 'fastparquet']):
        with pytest.raises(ImportError) as e_info:
            SwarmConnection().read_file('test2', as_type='parquet')

        assert e_info.value.args[0] == 'Neither PyArrow or FastParquet are installed, but required for read_parquet function.'
