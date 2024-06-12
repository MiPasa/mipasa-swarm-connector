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

import tempfile
import os
import subprocess
import requests
import time
import urllib.parse


class SwarmNode:
    def __init__(self):
        self.node_dir = tempfile.TemporaryDirectory()
        self.process = None
        self.address = '127.0.0.1:1633'

    def __enter__(self):
        self._install_node()
        self._start_node()
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self._stop_node()

    def make_batch_id(self):
        batch_data = requests.post('http://%s/stamps/1/17' % self.address).json()
        batch_id = batch_data['batchID']
        return batch_id

    def _stop_node(self):
        if self.process is not None:
            self.process.kill()

    def _start_node(self):
        self.process = subprocess.Popen(['%s/bee' % self.node_dir.name, 'dev', '--api-addr=%s' % self.address, '--debug-api-enable=false'])
        wait_seconds = 60
        for i in range(wait_seconds):
            try:
                rsp = requests.get('http://%s' % self.address).text
                if rsp == 'Ethereum Swarm Bee\n':
                    return
                print(repr(rsp))
                time.sleep(1)
            except:
                time.sleep(1)
        raise RuntimeError('Spawned Swarm node does not respond with standard response on "/" for %.2f seconds' % wait_seconds)

    def _install_node(self):
        cur_dir = os.getcwd()
        try:
            os.chdir(self.node_dir.name)
            result = os.system("bash -c 'curl -s https://raw.githubusercontent.com/ethersphere/bee/master/install.sh | BEE_INSTALL_DIR=. USE_SUDO=false PATH=$PATH:. bash'")
            if result != 0:
                raise RuntimeError('Failed to install Bee node for integration tests')
        finally:
            os.chdir(cur_dir)
