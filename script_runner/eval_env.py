########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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


__doc__ = """Used to obtain a clean global environment for eval"""

def setup_env_and_globals(ctx, script_path):
    copied_globals = globals().copy()
    del copied_globals['setup_env_and_globals']
    del copied_globals['clean_env']
    from os import path
    copied_globals['__file__'] = script_path
    copied_globals['__name__'] = '__main__'
    copied_globals['__package__'] = None

    import sys
    import imp
    ctx_holder = imp.new_module('ctx_holder')
    ctx_holder.ctx = ctx
    sys.modules['ctx_holder'] = ctx_holder

    return copied_globals

def clean_env():
    import sys
    del sys.modules['ctx_holder']
