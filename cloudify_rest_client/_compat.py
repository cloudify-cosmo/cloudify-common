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
PY36PLUS = sys.version_info >= (3, 6)


if PY2:
    from urllib import quote as urlquote, pathname2url
    from urlparse import urlparse
else:
    from urllib.parse import quote as urlquote, urlparse
    from urllib.request import pathname2url


__all__ = ['PY2', 'PY36PLUS', 'urlquote', 'pathname2url', 'urlparse']
