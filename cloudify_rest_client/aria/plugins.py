########
# Copyright (c) 2017 GigaSpaces Technologies Ltd. All rights reserved
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
from functools import partial

from ..plugins import PluginsClient
from . import wrapper, exceptions


class PluginClient(PluginsClient):

    def __init__(self, api, *args, **kwargs):
        super(PluginClient, self).__init__(*args, **kwargs)
        self.api = api
        self._wrapper_cls = partial(wrapper.wrap, cls_name='Plugin')
        self._uri_prefix = 'aria-plugins'

    def delete(self, *args, **kwargs):
        raise exceptions.OperationNotSupported(
            'Delete operation for plugins is currently unsupported'
        )
