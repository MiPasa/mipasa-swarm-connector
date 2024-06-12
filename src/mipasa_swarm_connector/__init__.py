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

import os
import cgi
from io import BytesIO
import json
import urllib.parse
import requests


class SwarmError(Exception):
    def __init__(self, msg, swarm_hash=None):
        super().__init__(msg)
        self.swarm_hash = swarm_hash


class SwarmClientError(SwarmError):
    def __init__(self, msg):
        super().__init__(msg)


class SwarmAPIError(SwarmError):
    def __init__(self, msg, swarm_hash=None, status_code=None):
        super().__init__(msg, swarm_hash=swarm_hash)
        self.status_code = status_code


class SwarmTypeError(SwarmError, TypeError):
    def __init__(self, msg, swarm_hash=None, expected_type=None, actual_type=None):
        super().__init__(msg, swarm_hash=swarm_hash)
        self.expected_type = expected_type
        self.actual_type = actual_type


class SwarmConnection:
    def __init__(self, gateway_url=None, session=None):
        if not gateway_url:
            gateway_url = os.getenv('BEE_GATEWAY_URL')
        if not gateway_url:
            raise SwarmClientError("""Swarm gateway URL is not specified.
Please either specify the URL explicitly in SwarmConnection() constructor,
 or set the environment variable BEE_GATEWAY_URL to the desired address.""")
        self.gateway_url = gateway_url
        self.session = session

    def __repr__(self):
        return "<SwarmConnection>"

    def _session(self):
        if self.session is not None:
            return self.session
        return requests

    @staticmethod
    def _detect_type(r):
        if "Content-Type" in r.headers:
            t, _ = cgi.parse_header(r.headers["Content-Type"])
            if t == "text/plain":
                return "text"
            elif t == "text/csv":
                return "csv"
            elif t == "application/json":
                return "json"
            elif t == "application/vnd.apache.parquet" or t == "application/parquet":
                return "parquet"

        if "Content-Disposition" in r.headers:
            _, params = cgi.parse_header(r.headers["Content-Disposition"])
            if "filename" in params:
                _, ext = os.path.splitext(params["filename"])
                ext = ext.lower()
                if ext == ".txt":
                    return "text"
                elif ext == ".csv":
                    return "csv"
                elif ext == ".json":
                    return "json"
                elif ext == ".parquet":
                    return "parquet"

        return "bytes"

    def _read_file_internal(self, swarm_hash):
        r = self._session().get(
            '%s/bzz/%s' % (self.gateway_url, urllib.parse.quote(swarm_hash))
        )

        if r.status_code != 200:
            raise SwarmAPIError(
                'Hash %s not found or could not be retrieved from Swarm (code %d)'
                % (repr(swarm_hash), r.status_code),
                swarm_hash=swarm_hash,
                status_code=r.status_code,
            )

        data_type = self._detect_type(r)
        return r.content, data_type

    @staticmethod
    def _load_optional_pandas():
        try:
            import pandas as pd
            return pd
        except ImportError as e:
            raise ImportError('Pandas is not installed, but required for read_csv and read_parquet functions.') from e

    @staticmethod
    def _check_optional_parquet():
        try:
            import pyarrow
        except ImportError:
            try:
                import fastparquet
            except ImportError as e:
                raise ImportError('Neither PyArrow or FastParquet are installed, but required for read_parquet function.') from e

    def _reinterpret_file(self, content, data_type):
        if data_type == "csv":
            pd = self._load_optional_pandas()
            f = BytesIO(content)
            return pd.read_csv(f)
        elif data_type == "parquet":
            pd = self._load_optional_pandas()
            self._check_optional_parquet()
            f = BytesIO(content)
            return pd.read_parquet(f)
        elif data_type == "json":
            f = BytesIO(content)
            return json.load(f)
        else:
            return content

    def read_file(self, swarm_hash, as_type=None, verify_type=False):
        allowed_types = [None, 'text', 'bytes', 'csv', 'parquet', 'json']
        if as_type not in allowed_types:
            raise ValueError('Unsupported type %s (expected one of %s)' % (repr(as_type), ', '.join(map(repr, allowed_types))))

        content, data_type = self._read_file_internal(swarm_hash)

        if as_type == "bytes":
            return content

        if verify_type and as_type is not None and data_type != as_type:
            raise SwarmTypeError(
                'Hash %s is not of type %s' % (repr(swarm_hash), repr(as_type)),
                swarm_hash=swarm_hash,
                expected_type=as_type,
                actual_type=data_type
            )

        if as_type is None:
            as_type = data_type

        return self._reinterpret_file(content, as_type)

    def read_csv(self, swarm_hash):
        return self.read_file(swarm_hash, as_type="csv")

    def read_parquet(self, swarm_hash):
        return self.read_file(swarm_hash, as_type="parquet")

    def read_json(self, swarm_hash):
        return self.read_file(swarm_hash, as_type="json")

    @staticmethod
    def _detect_upload_type(content):
        if isinstance(content, str):
            return 'text'
        if isinstance(content, bytes):
            return 'bytes'
        try:
            import pandas as pd
            if isinstance(content, pd.DataFrame):
                return 'csv'
        except ImportError:
            pass
        return 'json'

    def _write_file_internal(self, content, file_name, mime_type, batch_id):
        r = self._session().post(
            '%s/bzz?file_name=%s' % (self.gateway_url, urllib.parse.quote(file_name)),
            content,
            headers={
                'swarm-postage-batch-id': batch_id or '0000000000000000000000000000000000000000000000000000000000000000',
                'Content-Type': mime_type
            }
        )

        if r.status_code != 200 and r.status_code != 201:
            raise SwarmAPIError(
                'File could not be uploaded to Swarm (code %d)' % r.status_code,
                status_code=r.status_code,
            )

        try:
            response = r.json()
        except (json.JSONDecodeError, TypeError) as e:
            raise SwarmAPIError(
                'File could not be uploaded to Swarm'
            ) from e

        if 'reference' not in response:
            raise SwarmAPIError('File could not be uploaded to Swarm (reference not found in API response)')

        return response['reference']

    def write_file(self, content, file_name=None, as_type=None, mime_type=None, batch_id=None):
        if as_type == "text" and not isinstance(content, str):
            raise SwarmTypeError(
                "Text upload requested, but content is not of type 'str'.",
                expected_type='str',
                actual_type=type(content).__name__
            )
        if as_type == "bytes" and not isinstance(content, bytes) and not isinstance(content, bytearray):
            raise SwarmTypeError(
                "Byte upload requested, but content is not of type 'bytes'.",
                expected_type='bytes',
                actual_type=type(content).__name__
            )
        if as_type is None:
            as_type = self._detect_upload_type(content)
        if as_type == 'bytes':
            write_content = content
            write_file_name = file_name or 'file.bin'
            write_mime_type = mime_type or 'application/octet-stream'
        elif as_type == 'csv':
            pd = self._load_optional_pandas()
            if not isinstance(content, pd.DataFrame):
                raise SwarmTypeError(
                    "CSV upload requested, but content is not of type 'DataFrame'.",
                    expected_type='DataFrame',
                    actual_type=type(content).__name__
                )
            write_content = content.to_csv(index=False).encode('utf-8')
            write_file_name = file_name or 'file.csv'
            write_mime_type = mime_type or 'text/csv'
        elif as_type == 'parquet':
            pd = self._load_optional_pandas()
            self._check_optional_parquet()
            if not isinstance(content, pd.DataFrame):
                raise SwarmTypeError(
                    "Parquet upload requested, but content is not of type 'DataFrame'.",
                    expected_type='DataFrame',
                    actual_type=type(content).__name__
                )
            bio = BytesIO()
            bio_close = bio.close
            bio.close = lambda: None
            try:
                content.to_parquet(bio)
            finally:
                bio.close = bio_close
            bio.seek(0)
            write_content = bio.read()
            write_file_name = file_name or 'file.parquet'
            write_mime_type = mime_type or 'application/vnd.apache.parquet'
        elif as_type == 'json':
            try:
                write_content = json.dumps(content).encode('utf-8')
            except TypeError as e:
                raise SwarmTypeError("JSON upload requested, but content is not JSON serializable.") from e
            write_file_name = file_name or 'file.json'
            write_mime_type = mime_type or 'application/json'
        else:
            raise SwarmTypeError("Unsupported upload type '%s'" % as_type)
        return self._write_file_internal(write_content, write_file_name, write_mime_type, batch_id)
