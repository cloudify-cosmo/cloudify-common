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
from cloudify_rest_client.utils import get_file_content


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

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """List the agents installed from the manager.
        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.Agent.fields
        :return: A ListResponse containing the agents' details
        """
        if sort:
            kwargs['_sort'] = '-' + sort if is_descending else sort

        kwargs.setdefault('state', [AgentState.STARTED])
        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                _include=_include,
                                params=kwargs)
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

    def replace_ca_certs(self,
                         bundle,
                         new_manager_ca_cert,
                         new_broker_ca_cert):
        """Replace the agents' CA certs

        :param bundle: If true, a CA bundle that consists the new CA
        certificate and the old one will be passed to the agents. Otherwise,
        only the new CA certificates will be passed.
        :param new_manager_ca_cert: The new Manager CA certificate
        :param new_broker_ca_cert: The new RabbitMQ CA certificate
        :return: A dictionary {'number_of_updated_agents':
        number of agents that their CA certs were updated}
        """
        manager_ca_cert_str = (get_file_content(new_manager_ca_cert)
                               if new_manager_ca_cert else None)

        broker_ca_cert_str = (get_file_content(new_broker_ca_cert)
                              if new_broker_ca_cert else None)

        data = {
            'bundle': bundle,
            'broker_ca_cert': broker_ca_cert_str,
            'manager_ca_cert': manager_ca_cert_str
        }

        response = self.api.patch('/' + self._uri_prefix, data=data)

        return response
