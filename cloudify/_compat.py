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

"""Python 2 + 3 compatibility utils"""
# flake8: noqa

import sys
PY2 = sys.version_info[0] == 2


if PY2:
    from abc import ABCMeta
    import httplib
    import xmlrpclib
    import Queue as queue
    from urllib import quote as urlquote, pathname2url, urlencode, unquote
    from urllib2 import urlopen
    from ConfigParser import SafeConfigParser
    from urlparse import urlparse, urljoin, parse_qs
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO
    exec("""
def reraise(exception_type, value, traceback):
    raise exception_type, value, traceback
""")
    text_type = unicode
    exec("""
def exec_(code, globs):
    exec code in globs
""")

    class ABC:
        __metaclass__ = ABCMeta

    from uuid import uuid4 as stdlib_uuid4
    def uuid4():
        """Generate a random UUID.

        The python2 impl. just falls back to the stdlib one. Only py3 gets
        the performance boost.
        """
        return str(stdlib_uuid4())
else:
    from abc import ABC
    import builtins
    import os
    import queue
    import xmlrpc.client as xmlrpclib
    import http.client as httplib
    from io import StringIO
    from configparser import SafeConfigParser
    from urllib.parse import (
        quote as urlquote, urlparse, urljoin, parse_qs, urlencode, unquote
    )
    from urllib.request import pathname2url, urlopen

    def reraise(exception_type, value, traceback):
        raise value.with_traceback(traceback)

    text_type = str
    exec_ = getattr(builtins, 'exec')

    def uuid4():
        """Generate a random UUID, and return a string representation of it.

        This is pretty much a copy of the stdlib uuid4. We inline it here,
        because we'd like to avoid importing the stdlib uuid module on the
        operation dispatch critical path, because importing the stdlib
        uuid module runs some subprocesses (for detecting the uuid1-uuid3 MAC
        address), and that causes more memory pressure than we'd like.
        """
        uuid_bytes = os.urandom(16)
        uuid_as_int = int.from_bytes(uuid_bytes, byteorder='big')
        uuid_as_int &= ~(0xc000 << 48)
        uuid_as_int |= 0x8000 << 48
        uuid_as_int &= ~(0xf000 << 64)
        uuid_as_int |= 4 << 76
        uuid_as_hex = '%032x' % uuid_as_int
        return '%s-%s-%s-%s-%s' % (
            uuid_as_hex[:8],
            uuid_as_hex[8:12],
            uuid_as_hex[12:16],
            uuid_as_hex[16:20],
            uuid_as_hex[20:]
        )

try:
    from packaging.version import parse as parse_version
except ImportError:
    from distutils.version import LooseVersion as parse_version


__all__ = [
    'PY2', 'queue', 'StringIO', 'reraise', 'text_type', 'urlquote',
    'urlparse', 'exec_', 'urljoin', 'urlopen', 'pathname2url', 'parse_qs'
    'urlencode', 'unquote', 'httplib', 'SafeConfigParser', 'xmlrpclib',
    'parse_version', 'ABC', 'uuid4'
]
