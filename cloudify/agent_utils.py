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
from cloudify.state import ctx
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


def _get_rabbitmq_client():
    return RabbitMQClient([b.management_host for b in ctx.get_brokers()],
                          broker_config.broker_username,
                          broker_config.broker_password,
                          verify=broker_config.broker_cert_path,
                          logger=ctx.logger)


def get_agent_rabbitmq_user(cloudify_agent):
    # Proxy agents are not being installed
    if _is_proxied(cloudify_agent):
        return
    # Get the rabbitmq user of the agent
    username = USERNAME_PATTERN.format(cloudify_agent['name'])
    rabbitmq_client = _get_rabbitmq_client()
    for user in rabbitmq_client.get_users():
        if user['name'] == username:
            return user


def delete_agent_rabbitmq_user(cloudify_agent):
    # Proxy agents are not being installed
    if _is_proxied(cloudify_agent):
        return
    # Delete the rabbitmq user of the agent
    username = USERNAME_PATTERN.format(cloudify_agent['name'])
    rabbitmq_client = _get_rabbitmq_client()
    rabbitmq_client.delete_user(username)


def delete_agent_queues(cloudify_agent):
    if _is_proxied(cloudify_agent):
        return
    queues = [
        '{0}_service'.format(cloudify_agent['queue']),
        '{0}_operation'.format(cloudify_agent['queue'])
    ]
    vhost = cloudify_agent['tenant']['rabbitmq_vhost']
    rabbitmq_client = _get_rabbitmq_client()
    for queue in queues:
        rabbitmq_client.delete_queue(vhost, queue)


def delete_agent_exchange(cloudify_agent):
    if _is_proxied(cloudify_agent):
        return
    rabbitmq_client = _get_rabbitmq_client()
    vhost = cloudify_agent['tenant']['rabbitmq_vhost']
    name = cloudify_agent['name']
    rabbitmq_client.delete_exchange(vhost, name)


def _get_agent_system(cloudify_agent):
    if cloudify_agent.get('windows'):
        system = 'windows'
    elif cloudify_agent.get('architecture'):
        system = f"manylinux {cloudify_agent['architecture']}"
    else:
        system = cloudify_agent.get('distro')
        if cloudify_agent.get('distro_codename'):
            system = '{0} {1}'.format(system,
                                      cloudify_agent.get('distro_codename'))
    return system


def _is_proxied(cloudify_agent):
    return cloudify_agent.get('proxy')
