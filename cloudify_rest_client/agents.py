from cloudify.models_states import AgentState
from cloudify_rest_client import utils
from cloudify_rest_client.exceptions import CloudifyClientError
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

    def update(self, name, state=None, **kwargs):
        """Update agent with the provided settings.

        :param name: The name of the agent to update
        :param state: The updated state
        :param kwargs: Additional agent settings to update, e.g. version
        :return: The updated agent
        """
        data = {}
        # state is kept separate from kwargs, so that old-style (pre-7.0)
        # invocation with positional args like `.update(name, state)`
        # still works.
        if state:
            data['state'] = state
        data.update(kwargs)
        response = self.api.patch(
            '/{0}/{1}'.format(self._uri_prefix, name),
            data=data,
        )
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

    def dump(self, deployment_ids=None, agent_ids=None):
        """Generate agents' attributes for a snapshot.

        :param deployment_ids: A list of deployments' identifiers used to
         select agents to be dumped, should not be empty.
        :param agent_ids: A list of agents' identifiers, if not empty, used
         to select specific agents to be dumped.
        :returns: A generator of dictionaries, which describe agents'
         attributes.
        """
        if not deployment_ids:
            return
        for deployment_id in deployment_ids:
            entities = utils.get_all(
                    self.api.get,
                    f'/{self._uri_prefix}',
                    params={'_get_data': True,
                            'deployment_id': deployment_id},
                    _include=['id', 'node_instance_id', 'state', 'created_at',
                              'created_by', 'rabbitmq_password',
                              'rabbitmq_username', 'rabbitmq_exchange',
                              'version', 'system', 'install_method', 'ip',
                              'visibility'],
            )
            for entity in entities:
                if not agent_ids or entity['id'] in agent_ids:
                    yield {'__entity': entity, '__source_id': deployment_id}

    def restore(self, entities, logger):
        """Restore agents from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         agents to be restored.
        :param logger: A logger instance.
        """
        for entity in entities:
            entity['name'] = entity.pop('id')
            try:
                self.create(create_rabbitmq_user=True, **entity)
            except CloudifyClientError as exc:
                logger.error(f"Error restoring agent {entity['name']}: {exc}")
