# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
############

from cloudify_rest_client.agents import Agent
from cloudify_rest_client import CloudifyClient
from cloudify_rest_client.evaluate import EvaluatedFunctions
from cloudify_rest_client.executions import Execution
from cloudify_rest_client.operations import Operation
from cloudify_rest_client.node_instances import NodeInstance


node_instances = {}


def put_node_instance(node_instance_id,
                      node_id=None,
                      state='started',
                      runtime_properties=None,
                      relationships=None,
                      index=None):
    node_instances[node_instance_id] = NodeInstance({
        'id': node_instance_id,
        'node_id': node_id or node_instance_id,
        'state': state,
        'version': 0,
        'runtime_properties': runtime_properties,
        'relationships': relationships,
        'index': index or 0,
    })


class MockRestclient(CloudifyClient):

    def __init__(self, *args, **kwargs):
        pass

    @property
    def node_instances(self):
        return MockNodeInstancesClient()

    @property
    def nodes(self):
        return MockNodesClient()

    @property
    def executions(self):
        return MockExecutionsClient()

    @property
    def manager(self):
        return MockManagerClient()

    @property
    def agents(self):
        return MockAgentsClient()

    @property
    def operations(self):
        return MockOperationsClient()

    @property
    def evaluate(self):
        return MockEvaluateClient()


class MockEvaluateClient(object):
    def functions(self, deployment_id, context, payload):
        return EvaluatedFunctions({
            'payload': payload,
            'deployment_id': deployment_id
        })


class MockNodesClient(object):

    def list(self, deployment_id):
        return []


class MockNodeInstancesClient(object):

    def get(self, node_instance_id, evaluate_functions=False):
        if node_instance_id not in node_instances:
            raise RuntimeError(
                'No info for node with id {0}'.format(node_instance_id))
        return node_instances[node_instance_id]

    def list(self, deployment_id, node_id):
        return []


class MockExecutionsClient(object):

    def update(self, *args, **kwargs):
        return None

    def get(self, id):
        return Execution({
            'id': '111',
            'status': 'terminated'
        })


class MockManagerClient(object):

    def get_context(self):
        return {'context': {}}


class MockAgentsClient(object):

    def update(self, name, state):
        return Agent({
            'id': name,
            'name': name,
            'state': state
        })

    def get(self, name):
        return Agent({
            'id': name,
            'name': name,
            'state': 'started'
        })

    def create(self, name, node_instance_id, state, create_rabbitmq_user=True,
               **kwargs):
        return Agent({
            'id': name,
            'name': name,
            'node_instance_id': node_instance_id,
            'state': state,
            'create_rabbitmq_user': create_rabbitmq_user
        })


class MockOperationsClient(object):
    def get(self, operation_id):
        return Operation({'id': operation_id})

    def update(self, operation_id, state):
        pass
