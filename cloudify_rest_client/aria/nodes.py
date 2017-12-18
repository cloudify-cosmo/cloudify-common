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

from ..node_instances import NodeInstancesClient
from . import wrapper, exceptions


class NodesClient(NodeInstancesClient):

    def __init__(self, *args, **kwargs):
        super(NodesClient, self).__init__(*args, **kwargs)
        self._wrapper_cls = partial(wrapper.wrap, cls_name='Node')
        self._uri_prefix = 'aria-nodes'

    def update(self, *args, **kwargs):
        raise exceptions.OperationNotSupported(
            'Update operation for plugins is currently unsupported'
        )

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
