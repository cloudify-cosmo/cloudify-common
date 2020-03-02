########
# Copyright (c) 2020 Cloudify Platform Ltd. All rights reserved
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
    from urlparse import urlparse
    text_type = (str, unicode)

    class ABC(object):
        __metaclass__ = ABCMeta

    exec("""
def reraise(exception_type, value, traceback):
    raise exception_type, value, traceback
""")

else:
    from abc import ABC
    from urllib.parse import urlparse
    text_type = str

    def reraise(exception_type, value, traceback):
        raise value.with_traceback(traceback)


__all__ = ['PY2', 'text_type', 'urlparse', 'ABC', 'reraise']
