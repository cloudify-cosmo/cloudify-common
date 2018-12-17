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
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
############


from cloudify import utils, broker_config
from cloudify.manager import get_rest_client
from cloudify.models_states import AgentState
from cloudify.rabbitmq_client import RabbitMQClient, USERNAME_PATTERN

from cloudify_rest_client.exceptions import CloudifyClientError


def create_agent_record(cloudify_agent,
                        state=AgentState.CREATING,
                        create_rabbitmq_user=True,
                        client=None):
    # Proxy agents are not being saved in the agents table
    if _is_proxied(cloudify_agent):
        return
    if create_rabbitmq_user:
        _initialize_rabbitmq_user(cloudify_agent)
    client = client or get_rest_client()
    client.agents.create(
        cloudify_agent['name'],
        cloudify_agent['node_instance_id'],
        state,
        create_rabbitmq_user,
        ip=cloudify_agent.get('ip'),
        install_method=cloudify_agent.get('install_method'),
        system=_get_agent_system(cloudify_agent),
        version=cloudify_agent.get('version'),
        rabbitmq_username=cloudify_agent.get('broker_user'),
        rabbitmq_password=cloudify_agent.get('broker_pass'),
        rabbitmq_exchange=cloudify_agent.get('queue')
    )


def update_agent_record(cloudify_agent, state):
    # Proxy agents are not being saved in the agents table
    if _is_proxied(cloudify_agent):
        return
    client = get_rest_client()
    client.agents.update(cloudify_agent['name'], state)


def _initialize_rabbitmq_user(cloudify_agent):
    client = get_rest_client()
    # Generate a rabbitmq user for the agent
    username = USERNAME_PATTERN.format(cloudify_agent['name'])
    password = utils.generate_user_password()

    try:
        # In case the agent already exists
        agent = client.agents.get(cloudify_agent['name'])
        cloudify_agent['broker_user'] = agent.rabbitmq_username
        cloudify_agent['broker_pass'] = agent.rabbitmq_password
    except CloudifyClientError as e:
        if e.status_code != 404:
            raise
        cloudify_agent['broker_user'] = username
        cloudify_agent['broker_pass'] = password


def delete_agent_rabbitmq_user(agent_name):
    # Delete the rabbitmq user of the agent
    rabbitmq_client = RabbitMQClient(broker_config.broker_management_hostname,
                                     broker_config.broker_username,
                                     broker_config.broker_password,
                                     verify=broker_config.broker_cert_path)
    username = USERNAME_PATTERN.format(agent_name)
    rabbitmq_client.delete_user(username)


def _get_agent_system(cloudify_agent):
    if cloudify_agent.get('windows'):
        system = 'windows'
    else:
        system = cloudify_agent.get('distro')
        if cloudify_agent.get('distro_codename'):
            system = '{0} {1}'.format(system,
                                      cloudify_agent.get('distro_codename'))
    return system


def _is_proxied(cloudify_agent):
    return cloudify_agent.get('proxy')
