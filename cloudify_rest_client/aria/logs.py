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


class LogsClient(object):

    def __init__(self, api, *args, **kwargs):
        super(LogsClient, self).__init__(*args, **kwargs)
        self.api = api
        self._uri_prefix = 'aria-logs'

    def get(self, execution_id, _include=None):
        assert execution_id
        response = self.api.get(
            '/{self._uri_prefix}/{execution_id}'.format(
                self=self, execution_id=execution_id),
            _include=_include
        )
        return ListResponse(
            [wrapper.wrap(i, 'Log') for i in response['items']],
            response['metadata']
        )

    def list(self, _include=None, **kwargs):
        response = self.api.get(
            '/{self._uri_prefix}'.format(self=self),
            params=kwargs,
            _include=_include
        )
        return ListResponse(
            [wrapper.wrap(i, 'Log') for i in response['items']],
            response['metadata']
        )
