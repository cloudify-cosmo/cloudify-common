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

__author__ = 'idanmo'


import requests
import json


class NodesApi(object):

    def __init__(self, api_client):
        self.api_client = api_client

    def get_state_by_id(self, node_id, get_reachable_state=False,
                        get_runtime_state=True):

        resource_path = '/nodes/{0}'.format(node_id)

        query_params = {
            'reachable': str(get_reachable_state).lower(),
            'runtime': str(get_runtime_state).lower()
        }

        url = self.api_client.resource_url(resource_path)

        response = requests.get(url,
                                params=query_params)

        self.api_client.raise_if_not(200, response, url)

        return response.json()

    def update_node_state(self, node_id, updated_properties):

        resource_path = '/nodes/{0}'.format(node_id)
        url = self.api_client.resource_url(resource_path)
        response = requests.patch(url,
                                  headers={'Content-Type': 'application/json'},
                                  data=json.dumps(updated_properties))

        self.api_client.raise_if_not(200, response, url)

        return response.json()
