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

from ..responses import ListResponse
from . import wrapper


class ServiceClient(object):

    def __init__(self, api, *args, **kwargs):
        super(ServiceClient, self).__init__(*args, **kwargs)
        self.api = api
        self._uri_prefix = 'aria-services'

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        params = kwargs.copy()
        if sort:
            params['_sort'] = ('-' + sort) if is_descending else sort

        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                _include=_include,
                                params=params)

        return ListResponse(
            [wrapper.wrap(i, 'Service') for i in response['items']],
            response['metadata']
        )

    def get(self, service_id, _include=None):
        assert service_id
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=service_id)
        response = self.api.get(uri, _include=_include)
        return wrapper.wrap(response, 'Service')

    def create(
            self,
            service_template_id,
            service_name,
            inputs=None,
            private_resource=False
    ):
        assert service_template_id
        assert service_name
        params = {'private_resource': private_resource}
        data = {
            'service_template_id': service_template_id,
            'service_name': service_name
        }
        if inputs:
            data['inputs'] = inputs

        response = self.api.put('/{self._uri_prefix}'.format(self=self),
                                data,
                                params=params,
                                expected_status_code=201)
        return wrapper.wrap(response, 'Service')
