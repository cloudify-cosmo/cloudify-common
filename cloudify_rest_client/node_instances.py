########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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


class NodeInstance(dict):
    """
    Cloudify node instance.
    """

    def __init__(self, node_instance):
        self.update(node_instance)

    @property
    def id(self):
        """
        :return: The identifier of the node instance.
        """
        return self['id']

    @property
    def deployment_id(self):
        """
        :return: The deployment id the node instance belongs to.
        """
        return self['deploymentId']

    @property
    def runtime_properties(self):
        """
        :return: The runtime properties of the node instnace.
        """
        return self['runtimeProperties']

    @property
    def state(self):
        """
        :return: The current state of the node instance.
        """
        return self['state']

    @property
    def version(self):
        """
        :return: The current version of the node instance
         (used for optimistic locking on update)
        """
        return self['version']


class NodeInstancesClient(object):

    def __init__(self, api):
        self.api = api

    @staticmethod
    def _get_node_instance_uri(node_instance_id):
        return '/node-instances/{0}'.format(node_instance_id)

    def create(self, node_instance_id, deployment_id, runtime_properties=None):
        """
        Creates a node instance in Cloudify's storage.

        :param node_instance_id: The node instance's identifier.
        :param deployment_id: The deployment id the node instance belongs to.
        :param runtime_properties: Initial runtime properties for the created
         node instance.
        :return: The created node instance.
        """
        assert node_instance_id
        assert deployment_id
        data = {
            'id': node_instance_id,
            'deploymentId': deployment_id,
            'runtimeProperties': runtime_properties or {}
        }
        uri = self._get_node_instance_uri(node_instance_id)
        response = self.api.put(uri, data=data, expected_status_code=201)
        return NodeInstance(response)

    def get(self, node_instance_id):
        """
        Returns the node instance for the provided node instance id.

        :param node_instance_id: The identifier of the node instance to get.
        :return: The retrieved node instance.
        """
        assert node_instance_id
        uri = self._get_node_instance_uri(node_instance_id)
        response = self.api.get(uri)
        return NodeInstance(response)

    def update(self,
               node_instance_id,
               state=None,
               runtime_properties=None,
               version=0):
        """
        Update node instance with the provided state & runtime_properties.

        :param node_instance_id: The identifier of the node instance to update.
        :param state: The updated state.
        :param runtime_properties: The updated runtime properties.
        :param version: Current version value of this node instance in
         Cloudify's storage (used for optimistic locking).
        :return: The updated node instance.
        """
        assert node_instance_id
        uri = self._get_node_instance_uri(node_instance_id)
        data = {'version': version}
        if runtime_properties:
            data['runtimeProperties'] = runtime_properties
        if state:
            data['state'] = state
        response = self.api.patch(uri, data=data)
        return NodeInstance(response)
