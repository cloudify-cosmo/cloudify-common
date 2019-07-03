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


import sys
PY2 = sys.version_info[0] == 2


def reraise(exception_type, value, traceback):
    raise value.with_traceback(traceback)


if PY2:
    # py2's syntax doesn't exist in later pythons, so we can only
    # _create_ the function on py2
    exec("""
def reraise(exception_type, value, traceback):
    raise exception_type, value, traceback
""")
