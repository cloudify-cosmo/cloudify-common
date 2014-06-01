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

__author__ = 'idanmo'


class Execution(dict):

    def __init__(self, execution):
        self.update(execution)

    @property
    def id(self):
        return self['id']


class ExecutionsClient(object):

    def __init__(self, api):
        self.api = api

    def list(self, deployment_id):
        assert deployment_id
        uri = '/deployments/{0}/executions'.format(deployment_id)
        response = self.api.get(uri)
        return [Execution(item) for item in response]

    def get(self, execution_id):
        assert execution_id
        uri = '/executions/{0}'.format(execution_id)
        response = self.api.get(uri)
        return Execution(response)

    def update(self, execution_id, status, error=None):
        uri = '/executions/{0}'.format(execution_id)
        params = {'status': status}
        if error:
            params['error'] = error
        response = self.api.patch(uri, data=params)
        return Execution(response)

    def cancel(self, execution_id):
        uri = '/executions/{0}'.format(execution_id)
        response = self.api.post(uri, data={'action':'cancel'})
        return Execution(response)
