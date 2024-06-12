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

from .util import SwarmNode
from mipasa_swarm_connector import SwarmConnection
import pandas as pd


def test_csv_from_real_swarm_node():
    with SwarmNode() as node:
        conn = SwarmConnection(gateway_url='http://%s' % node.address)
        batch_id = node.make_batch_id()
        hash_of_file = conn.write_file(b'a,b,c\n1,2,3\n4,5,6\n7,8,9', file_name='test.csv', mime_type='text/csv', batch_id=batch_id)
        assert pd.DataFrame(
            [[1, 2, 3], [4, 5, 6], [7, 8, 9]],
            columns=['a', 'b', 'c']
        ).equals(conn.read_file(hash_of_file))
