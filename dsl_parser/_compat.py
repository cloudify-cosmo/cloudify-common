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

import sys
PY2 = sys.version_info[0] == 2

try:
    from abc import ABC
except ImportError:
    from abc import ABCMeta

    class ABC(object):
        __metaclass__ = ABCMeta


def reraise(exception_type, value, traceback):
    raise value.with_traceback(traceback)


if PY2:
    # py2's syntax doesn't exist in later pythons, so we can only
    # _create_ the function on py2
    exec("""
def reraise(exception_type, value, traceback):
    raise exception_type, value, traceback
""")


if PY2:
    from urllib import quote as urlquote, pathname2url
    from urllib2 import Request, urlopen, URLError
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO
    from urlparse import urljoin, urlparse
else:
    from urllib.parse import quote as urlquote, urljoin, urlparse
    from urllib.request import pathname2url, Request, urlopen
    from urllib.error import URLError
    from io import StringIO

if PY2:
    string_types = (str, unicode)
    text_type = unicode
else:
    string_types = (str, )
    text_type = str


__all__ = [
    'PY2', 'ABC', 'urlquote', 'reraise', 'pathname2url', 'Request', 'urlopen',
    'URLError', 'StringIO', 'urljoin', 'urlparse', 'string_types', 'text_type'
]
