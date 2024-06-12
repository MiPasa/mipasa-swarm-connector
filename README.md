# MiPasa Swarm Connector

![Tests Status](https://github.com/MiPasa/mipasa-swarm-connector/actions/workflows/tests.yaml/badge.svg) ![Integration Tests Status](https://github.com/MiPasa/mipasa-swarm-connector/actions/workflows/integration.yaml/badge.svg)

This Python library implements connecting to Swarm (BZZ) distributed storage network.

Learn more about MiPasa: https://www.mipasa.com/

Learn more about Swarm: https://www.ethswarm.org/

## Optional dependencies

- `mipasa_swarm_connector[pandas]` is required if you wish to read files as CSV or Parquet.
- `mipasa_swarm_connector[parquet]` is required if you wish to read files as Parquet. More specific versions of this dependency exist:
  - `mipasa_swarm_connector[parquet-pyarrow]`
  - `mipasa_swarm_connector[parquet-fastparquet]`

## Specifying the Swarm node address

In order to work with Swarm, you will need to specify a Bee node address.

For production environments, we advise doing so via setting the environment variable `BEE_GATEWAY_URL` to the HTTP endpoint of the chosen node.

For development, you can also specify the node address directly:

```python
from mipasa_swarm_connector import SwarmConnection

content = SwarmConnection('http://localhost:1633').read_file('36f0830b0ece6273a50cc0ff58c4597883e8e41ea9c8ddabb0d87d6b0ca95a1a')
print(repr(content))
```

## Reading files

### Simplest usage

```python
from mipasa_swarm_connector import SwarmConnection, SwarmAPIError

try:
    content = SwarmConnection().read_file('<your_swarm_file_hash_here>')
    print(repr(content))
except SwarmAPIError as e:
    print('Failed to fetch, status: %d' % e.status_code)
```

This code will read the contents of the specified file on Swarm.

By default, the exact file type will be autodetected:

- If the file has a known file type (which is `application/json`, `text/csv`, `application/vnd.apache.parquet` and `application/parquet`) it will be reintepreted as a JSON object or a Pandas DataFrame.
- Otherwise, the file will be downloaded as bytes.

Any use of `read_file` can raise `SwarmAPIError`, which, generally, means that a file is not found (status code `404`).

### Read a file as CSV

```python
from mipasa_swarm_connector import SwarmConnection, SwarmTypeError

try:
  dataframe = SwarmConnection().read_file('<your_swarm_file_hash_here>', as_type='csv', verify_type=True)
  print(repr(dataframe))
except SwarmTypeError as e:
  print('Expected type %s, got type %s' % (e.expected_type, e.actual_type))
```

This code will read the contents of the specified file on Swarm as a Pandas DataFrame, expecting it to be in CSV format.

`verify_type=True` can be passed (but not required) in order to check whether file's MIME type or filename matches CSV.

### Read a file as Parquet

```python
from mipasa_swarm_connector import SwarmConnection

dataframe = SwarmConnection().read_file('<your_swarm_file_hash_here>', as_type='parquet')
print(repr(dataframe))
```

When type-checking for Parquet, two MIME types are recognized:

- `application/vnd.apache.parquet`
- `application/parquet`

### Read a file as JSON

```python
from mipasa_swarm_connector import SwarmConnection

dataframe = SwarmConnection().read_file('<your_swarm_file_hash_here>', as_type='json')
print(repr(dataframe))
```



## Writing files

```python
from mipasa_swarm_connector import SwarmConnection, SwarmAPIError

try:
    swarm_hash = SwarmConnection().write_file(b'binary-content')
except SwarmAPIError as e:
    print('Failed to upload, status: %d' % e.status_code)
```

This code will attempt uploading the specified bytes to Swarm.

By default, the exact type of the uploaded file will be autodetected:

- If `bytes` or `bytearray` is passed, then the uploaded file type will be `application/octet-stream`.
- If a string is passed, it will be encoded as UTF-8 and the file type will be `text/plain`.
- If a Pandas DataFrame is passed, then the uploaded file type will be `text/csv` and the DataFrame will be formatted as CSV.
- If any other object is passed, JSON encoding will be attempted. If it succeeds, then the uploaded file type will be `application/json`.

Additionally, if your content is a Pandas DataFrame, you may specify `as_type='parquet'` in order to automatically format this file as Parquet (the file type will be `application/vnd.apache.parquet`).
