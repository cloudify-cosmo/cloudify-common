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


class NodesApi(object):

    def __init__(self, client):
        self.client = client

    def list(self, deployment_id=None):
        # TODO: list nodes for provided deployment id
        query_params = {}
        post_data = None

        response = self.client.callAPI('/nodes', 'GET', query_params, post_data)

        if not response:
            return None

        response_object = self.client.deserialize(response, 'array[Node]')
        return response_object

    def get_by_id(self, node_id):
        query_params = {}
        post_data = None

        resource_path = '/nodes/{0}'.format(self.client.toPathValue(node_id))
        response = self.client.callAPI(resource_path, 'GET', query_params, post_data)

        if not response:
            return None

        response_object = self.client.deserialize(response, 'Node')
        return response_object

    def get_reachable_state_by_id(self, node_id):
        query_params = {}
        post_data = None

        resource_path = '/nodes/{0}?reachable'.format(self.client.toPathValue(node_id))
        response = self.client.callAPI(resource_path, 'GET', query_params, post_data)

        if not response:
            return None

        response_object = self.client.deserialize(response, 'Node')
        return response_object
