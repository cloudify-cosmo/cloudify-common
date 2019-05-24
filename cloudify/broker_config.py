########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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

# AMQP broker configuration for agents and manager
from __future__ import absolute_import

import json
import os
import ssl

from cloudify.constants import BROKER_PORT_SSL, BROKER_PORT_NO_SSL


def get_config_path():
    workdir_path = os.getenv('AGENT_WORK_DIR')
    if workdir_path is None:
        # We are not in an appropriately configured worker environment
        return None
    else:
        return os.path.join(workdir_path, 'broker_config.json')


def load_broker_config(path=None):
    """Load all the config variables from the config file.

    Users of this module expect the config to be exposed as global
    variables, so we set them. This allows to re-load them if needed.

    :param path: Path to the config file.
                 (default: $AGENT_WORK_DIR/broker_config.json)
    """
    global broker_cert_path, broker_username, broker_password, broker_vhost, \
        broker_hostname, broker_ssl_enabled, broker_port, \
        broker_management_hostname, broker_heartbeat, broker_ssl_options

    path = path or get_config_path()
    if not path or not os.path.isfile(path):
        return
    with open(path) as f:
        config = json.load(f)

    broker_cert_path = config.get('broker_cert_path') or broker_cert_path
    broker_username = config.get('broker_username') or broker_username
    broker_password = config.get('broker_password') or broker_password
    broker_hostname = config.get('broker_hostname') or broker_hostname
    broker_vhost = config.get('broker_vhost') or broker_vhost
    broker_ssl_enabled = config.get('broker_ssl_enabled') or broker_ssl_enabled
    broker_port = BROKER_PORT_SSL if broker_ssl_enabled else BROKER_PORT_NO_SSL
    broker_management_hostname = \
        config.get('broker_management_hostname') or broker_management_hostname

    broker_heartbeat = config.get('broker_heartbeat') or broker_heartbeat
    if broker_ssl_enabled:
        broker_ssl_options = {
            'ca_certs': broker_cert_path,
            'cert_reqs': ssl.CERT_REQUIRED,
        }
    else:
        broker_ssl_options = {}


# Provided as variables for retrieval by amqp_client and logger as required
broker_cert_path = ''
broker_username = 'guest'
broker_password = 'guest'
broker_hostname = 'localhost'
broker_vhost = '/'
broker_ssl_enabled = False
broker_port = BROKER_PORT_NO_SSL
broker_management_hostname = 'localhost'
broker_ssl_options = {}
broker_heartbeat = 30

# defaults are overridden from file at import time
load_broker_config()
