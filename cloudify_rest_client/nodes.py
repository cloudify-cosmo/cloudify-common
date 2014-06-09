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


class Node(dict):
    """
    Cloudify node.
    """

    def __init__(self, node_instance):
        self.update(node_instance)

    @property
    def id(self):
        """
        :return: The identifier of the node.
        """
        return self['id']

    @property
    def deployment_id(self):
        """
        :return: The deployment id the node belongs to.
        """
        return self['deployment_id']

    @property
    def properties(self):
        """
        :return: The static properties of the node.
        """
        return self['properties']

    @property
    def operations(self):
        """
        :return: The node operations mapped to plugins.
        :rtype: dict
        """
        return self['operations']

    @property
    def relationships(self):
        """
        :return: The node relationships with other nodes.
        :rtype: list
        """
        return self['relationships']

    @property
    def blueprint_id(self):
        """
        :return: The id of the blueprint this node belongs to.
        :rtype: str
        """
        return self['blueprint_id']

    @property
    def plugins(self):
        """
        :return: The plugins this node has operations mapped to.
        :rtype: dict
        """
        return self['plugins']

    @property
    def number_of_instances(self):
        """
        :return: The number of instances this node has.
        :rtype: int
        """
        return int(self['number_of_instances'])

    @property
    def host_id(self):
        """
        :return: The id of the node instance which hosts this node.
        :rtype: str
        """
        return self['host_id']

    @property
    def type_hierarchy(self):
        """
        :return: The type hierarchy of this node.
        :rtype: list
        """
        return self['type_hierarchy']

    @property
    def type(self):
        """
        :return: The type of this node.
        :rtype: str
        """
        return self['type']


class NodesClient(object):

    def __init__(self, api):
        self.api = api

    def list(self, deployment_id=None):
        """
        Returns a list of nodes which belong to the deployment identified
         by the provided deployment id.
        :param deployment_id: The deployment's id to list nodes for.
        :return: Nodes.
        :rtype: list
        """
        params = {'deployment_id': deployment_id} if deployment_id else None
        response = self.api.get('/nodes', params=params)
        return [Node(item) for item in response]
