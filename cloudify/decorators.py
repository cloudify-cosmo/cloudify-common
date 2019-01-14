########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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


def operation(func=None, resumable=False, **kwargs):
    """This decorator is not required for operations to work.

    Use this for readability, and to add additional markers for the function
    (eg. is it resumable).
    """
    if func:
        func.resumable = resumable
        return func
    else:
        return lambda fn: operation(fn, resumable=resumable, **kwargs)


def workflow(func=None, system_wide=False, resumable=False, **kwargs):
    """This decorator should only be used to decorate system wide
       workflows. It is not required for regular workflows.
    """
    if func:
        func.workflow_system_wide = system_wide
        func.resumable = resumable
        return func
    else:
        return lambda fn: workflow(fn, system_wide=system_wide,
                                   resumable=resumable, **kwargs)


task = operation
