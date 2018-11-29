########
# Copyright (c) 2018 Cloudify Platform Ltd. All rights reserved
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

from cloudify_rest_client.responses import ListResponse


class Operation(dict):
    def __init__(self, operation):
        self.update(operation)

    @property
    def id(self):
        return self.get('id')

    @property
    def state(self):
        return self.get('state')

    @property
    def created_at(self):
        return self.get('created_at')

    @property
    def dependencies(self):
        """IDs of operations that this operation depends on"""
        return self.get('dependencies')

    @property
    def type(self):
        return self.get('type')

    @property
    def parameters(self):
        return self.get('parameters')


class OperationsClient(object):
    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'operations'
        self._wrapper_cls = Operation

    def list(self, graph_id):
        params = {'graph_id': graph_id}
        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                params=params)
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata'])

    def create(self, operation_id, graph_id, name, type, parameters,
               dependencies):
        data = {
            'name': name,
            'graph_id': graph_id,
            'dependencies': dependencies,
            'type': type,
            'parameters': parameters
        }
        uri = '/operations/{0}'.format(operation_id)
        response = self.api.put(uri, data=data, expected_status_code=201)
        return Operation(response)

    def update(self, operation_id, state):
        uri = '/operations/{0}'.format(operation_id)
        response = self.api.patch(uri, data={'state': state})
        return Operation(response)

    def delete(self, operation_id):
        uri = '/operations/{0}'.format(operation_id)
        self.api.delete(uri)


class TasksGraph(dict):
    def __init__(self, tasks_graph):
        self.update(tasks_graph)

    @property
    def id(self):
        return self.get('id')

    @property
    def execution(self):
        return self.get('execution_id')

    @property
    def name(self):
        return self.get('name')


class TasksGraphClient(object):
    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'tasks_graphs'
        self._wrapper_cls = TasksGraph

    def list(self, execution_id, name):
        params = {'execution_id': execution_id, 'name': name}
        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                params=params)
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata'])

    def create(self, execution_id, name, operations=None):
        params = {
            'name': name,
            'execution_id': execution_id,
            'operations': operations
        }
        uri = '/{self._uri_prefix}/tasks_graphs'.format(self=self)
        response = self.api.post(uri, data=params, expected_status_code=201)
        return TasksGraph(response)

    def update(self, tasks_graph_id, state):
        uri = '/tasks_graphs/{0}'.format(tasks_graph_id)
        response = self.api.patch(uri, data={'state': state})
        return TasksGraph(response)
