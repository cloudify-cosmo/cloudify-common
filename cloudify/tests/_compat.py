########
# Copyright (c) 2019 Cloudify Platform Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

"""Python 2 + 3 compatibility utils

Those are only related to tests, so that things that aren't used outside
of tests, don't need to be imported in the main compat module."""

from cloudify._compat import PY2

if PY2:
    from SocketServer import TCPServer
    from SimpleHTTPServer import SimpleHTTPRequestHandler
else:
    from socketserver import TCPServer
    from http.server import SimpleHTTPRequestHandler

__all__ = ['PY2', 'TCPServer', 'SimpleHTTPRequestHandler']
