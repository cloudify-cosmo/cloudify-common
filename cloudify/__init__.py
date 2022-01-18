########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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

import json
import os

from cloudify.constants import BROKER_PORT_SSL, BROKER_PORT_NO_SSL


from cloudify.state import ctx  # noqa

_BROKER_CONFIG_DEFAULTS = {
    'broker_cert_path': '',
    'broker_username': 'guest',
    'broker_password': 'guest',
    'broker_hostname': 'localhost',
    'broker_vhost': '/',
    'broker_ssl_enabled': False,
    'broker_management_hostname': 'localhost',
    'broker_heartbeat': 30,
}


class _BrokerConfig(object):
    def __init__(self):
        for name, value in _BROKER_CONFIG_DEFAULTS.items():
            setattr(self, name, value)

    def get_config_path(self):
        workdir_path = os.getenv('AGENT_WORK_DIR')
        if workdir_path is None:
            # We are not in an appropriately configured worker environment
            return None
        else:
            return os.path.join(workdir_path, 'broker_config.json')

    def load_broker_config(self, path=None):
        """Load all the config variables from the config file.

        :param path: Path to the config file.
                     (default: $AGENT_WORK_DIR/broker_config.json)
        """
        path = path or self.get_config_path()
        if not path or not os.path.isfile(path):
            return
        with open(path) as f:
            config = json.load(f)

        for name in _BROKER_CONFIG_DEFAULTS:
            value = config.get(name)
            if value:
                setattr(self, name, value)

    @property
    def broker_port(self):
        return BROKER_PORT_SSL if self.broker_ssl_enabled \
            else BROKER_PORT_NO_SSL


broker_config = _BrokerConfig()
broker_config.load_broker_config()
