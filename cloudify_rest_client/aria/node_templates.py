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

from ..nodes import NodesClient
from . import wrapper


class NodeTemplatesClient(NodesClient):

    def __init__(self, *args, **kwargs):
        super(NodeTemplatesClient, self).__init__(*args, **kwargs)
        self._wrapper_cls = partial(wrapper.wrap, cls_name='NodeTemplate')
        self._uri_prefix = 'aria-node-templates'

    def _create_filters(
            self,
            sort=None,
            is_descending=False,
            service_id=None,
            node_template_id=None,
            **kwargs
    ):
        params = {}
        if node_template_id:
            params['node_template_id'] = node_template_id
        if service_id:
            params['service_id'] = service_id

        params.update(kwargs)

        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        return params

    def get(
            self,
            node_template_id,
            _include=None,
            evaluate_functions=False,
            **kwargs
    ):
        assert node_template_id
        result = self.api.get(
            '/{self._uri_prefix}/{id}'.format(self=self, id=node_template_id),
            node_template_id=node_template_id,
            _include=_include,
            evaluate_functions=evaluate_functions)
        try:
            return result[0]
        except IndexError:
            return None
