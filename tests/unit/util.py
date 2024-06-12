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

import builtins


class ImportErrorMock:
    def __init__(self, stop_modules):
        self.stop_modules = [stop_modules] if isinstance(stop_modules, str) else stop_modules
        self.original_import = None

    def __enter__(self):
        self.original_import = builtins.__import__
        builtins.__import__ = self._handle_import

    def __exit__(self, exc_type, exc_value, exc_traceback):
        builtins.__import__ = self.original_import

    def _handle_import(self, module, *args, **kwargs):
        should_be_stopped = [x for x in self.stop_modules if x == module or module.startswith(x+'.')]
        if should_be_stopped:
            raise ImportError('Mocked module failure: %s' % ', '.join(map(repr, should_be_stopped)))
        return self.original_import(module, *args, **kwargs)
