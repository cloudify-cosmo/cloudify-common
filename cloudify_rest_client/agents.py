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

from cloudify.models_states import AgentState
from cloudify_rest_client.responses import ListResponse


class Agent(dict):
    def __init__(self, agent):
        super(Agent, self).__init__()
        self.update(agent)

    @property
    def id(self):
        """
        :return: The identifier of the agent's node instance.
        """
        return self.get('id')

    @property
    def host_id(self):
        """
        :return: The identifier of the host instance the agent is installed on.
        """
        return self.get('host_id')

    @property
    def ip(self):
        """
        :return: The IP address of the agent
        """
        return self.get('ip')

    @property
    def install_method(self):
        """
        :return: The install_method property of the agent.
        """
        return self.get('install_method')

    @property
    def system(self):
        """
        :return: The operating system the agent is installed on.
        """
        return self.get('system')

    @property
    def version(self):
        """
        :return: The version of the agent.
        """
        return self.get('version')

    @property
    def node(self):
        """
        :return: The identifier of the agent's node.
        """
        return self.get('node')

    @property
    def deployment(self):
        """
        :return: The identifier of the agent's deployment.
        """
        return self.get('deployment')

    @property
    def name(self):
        """
        :return: The name of the agent.
        """
        return self.get('name')

    @property
    def node_instance_id(self):
        """
        :return: The identifier of the agent's node instance.
        """
        return self.get('node_instance_id')

    @property
    def rabbitmq_exchange(self):
        """
        :return: The RabbitMQ exchange of the agent.
        """
        return self.get('rabbitmq_exchange')

    @property
    def rabbitmq_username(self):
        """
        :return: The RabbitMQ user of the agent.
        """
        return self.get('rabbitmq_username')

    @property
    def rabbitmq_password(self):
        """
        :return: The RabbitMQ password of the agent.
        """
        return self.get('rabbitmq_password')

    @property
    def state(self):
        """
        :return: The state of the agent.
        """
        return self.get('state')

    @property
    def created_at(self):
        """
        :return: The creation date of the agent.
        """
        return self.get('created_at')

    @property
    def updated_at(self):
        """
        :return: The modification date of the agent.
        """
        return self.get('updated_at')


class AgentsClient(object):
    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'agents'
        self._wrapper_cls = Agent

    def list(self, deployment_id=None, node_ids=None, node_instance_ids=None,
             install_methods=None):
        """List the agents installed from the manager.

        :param deployment_id: Deployment id to filter by
        :param node_ids: List of node ids to filter by
        :param node_instance_ids: List of node instance ids to filter by
        :return: A ListResponse containing the agents details
        """
        params = {}
        if deployment_id:
            params['deployment_id'] = deployment_id
        if node_ids:
            params['node_ids'] = node_ids
        if node_instance_ids:
            params['node_instance_ids'] = node_instance_ids
        if install_methods:
            params['install_methods'] = install_methods
        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                params=params)
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def get(self, name):
        """Get an agent from the manager.

         :param name: The name of the agent
         :return: The details of the agent
         """
        response = self.api.get('/{0}/{1}'.format(self._uri_prefix, name))
        return self._wrapper_cls(response)

    def create(self, name, node_instance_id, state=AgentState.CREATING,
               create_rabbitmq_user=True, **kwargs):
        """Create an agent in the DB.

         :param name: The name of the agent
         :param node_instance_id: The node_instance_id of the agent
         :param state: The state of the agent
         :param create_rabbitmq_user: Should create the rabbitmq user or not
         :return: The details of the agent
         """
        data = {'node_instance_id': node_instance_id,
                'state': state,
                'create_rabbitmq_user': create_rabbitmq_user}
        data.update(kwargs)
        response = self.api.put('/{0}/{1}'.format(self._uri_prefix, name),
                                data=data)
        return self._wrapper_cls(response)

    def update(self, name, state):
        """Update agent with the provided state.

        :param name: The name of the agent to update
        :param state: The updated state
        :return: The updated agent
        """
        data = {'state': state}
        response = self.api.patch('/{0}/{1}'.format(self._uri_prefix, name),
                                  data=data)
        return self._wrapper_cls(response)
