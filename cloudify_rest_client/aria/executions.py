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

from .. import executions
from . import wrapper, exceptions


class ExecutionsClient(executions.ExecutionsClient):

    def __init__(self, *args, **kwargs):
        super(ExecutionsClient, self).__init__(*args, **kwargs)
        self._wrapper_cls = partial(wrapper.wrap, cls_name='Execution')
        self._uri_prefix = 'aria-executions'

    def update(self, *args, **kwargs):
        raise exceptions.OperationNotSupported(
            'Delete operation for plugins is currently unsupported'
        )

    def _create_filters(
            self,
            service_id=None,
            sort=None,
            is_descending=False,
            **kwargs
    ):
        params = {}
        if service_id:
            params['service_id'] = service_id
        params.update(kwargs)
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort
        return params

    def start(
            self,
            service_id,
            workflow_name,
            parameters=None,
            allow_custom_parameters=False,
            force=False
    ):
        assert service_id
        assert workflow_name
        data = {
            'service_id': service_id,
            'workflow_name': workflow_name,
            'parameters': parameters,
            'allow_custom_parameters': str(allow_custom_parameters).lower(),
            'force': str(force).lower()
        }
        response = self.api.post('/{self._uri_prefix}'.format(self=self),
                                 data=data,
                                 expected_status_code=201)
        return self._wrapper_cls(response)
