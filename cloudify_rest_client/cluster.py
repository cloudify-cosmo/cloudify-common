########
# Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
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


from cloudify_rest_client.responses import ListResponse


class ClusterNode(dict):
    @property
    def master(self):
        return self.get('master', False)

    @property
    def online(self):
        return self.get('online', False)

    @property
    def host_ip(self):
        return self.get('host_ip')


class ClusterState(dict):
    @property
    def initialized(self):
        return self.get('initialized', False)

    @property
    def encryption_key(self):
        return self.get('encryption_key')

    @property
    def logs(self):
        return self.get('logs', [])

    @property
    def error(self):
        return self.get('error')


class ClusterClient(object):
    def __init__(self, api):
        self.api = api
        self.nodes = ClusterNodesClient(api)

    def status(self, _include=None, **kwargs):
        """
        Retrieve the current cluster status.

        :param _include: List of fields to include in the response.
        :param kwargs: optional filter fields
        :return: cluster status
        """
        response = self.api.get('/cluster',
                                _include=_include,
                                params=kwargs)
        return ClusterState(response)

    def start(self, config):
        """
        Create a HA cluster with the current manager as the master node.

        :param config: the cluster config
        :return: current state of the cluster
        :rtype: ClusterState
        """
        response = self.api.put('/cluster', data={
            'config': config
        })
        return ClusterState(response)

    def join(self, config):
        """
        Join the HA cluster on the current manager.

        :param config: the node config
        :return: current state of the cluster
        :rtype: ClusterState
        """
        node_id = config['node_name']
        response = self.api.put('/cluster/nodes/{0}'.format(node_id), data={
            'config': config
        })
        return ClusterState(response)

    def update(self, config):
        """
        Update the HA cluster configuration.

        :param config: new cluster configuration
        :return: current cluster status
        """
        response = self.api.patch('/cluster', data={
            'config': config
        })
        return ClusterState(response)


class ClusterNodesClient(object):
    def __init__(self, api):
        self.api = api

    def list(self):
        """
        List the nodes in the HA cluster.

        :return: details of all the nodes in the cluster
        :rtype: list
        """
        response = self.api.get('/cluster/nodes')
        return ListResponse([ClusterNode(item) for item in response], {})

    def details(self, node_id):
        """
        Get the details of a node in the cluster - configuration and state

        :param node_id: Node's id to get
        :return: details of the node
        """
        response = self.api.get('/cluster/nodes/{0}'.format(node_id))
        return ClusterNode(response)

    def delete(self, node_id):
        """
        Remove a node from the HA cluster.

        :param node_id: id of the node to be deleted from the cluster
        :return: details of the removed node

        Use this when a node is permanently down and needs to be deleted
        from the cluster.
        """
        response = self.api.delete('/cluster/nodes/{0}'.format(node_id))
        return ClusterNode(response)
