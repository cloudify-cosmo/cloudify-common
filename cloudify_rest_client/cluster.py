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

    @property
    def name(self):
        return self.get('name')

    @property
    def initialized(self):
        return self.get('initialized', False)

    @property
    def credentials(self):
        return self.get('credentials', None)

    @property
    def required(self):
        return self.get('required', None)


class ClusterState(dict):
    @property
    def initialized(self):
        return self.get('initialized', False)

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

    def start(self, host_ip, node_name, options=None):
        """
        Create a HA cluster with the current manager as the master node.

        :param host_ip: the externally-visible IP of the current node
        :param node_name: the name of this node used internally in the cluster
        :param options: additional configuration inputs
        :return: current state of the cluster
        :rtype: ClusterState
        """
        response = self.api.put('/cluster', data={
            'host_ip': host_ip,
            'node_name': node_name,
            'options': options
        })
        return ClusterState(response)

    def join(self, host_ip, node_name, credentials, join_addrs,
             required=None, options=None):
        """
        Join the HA cluster on the current manager.

        To generate credentials that are required for joining a cluster,
        first send the `client.cluster.nodes.add` request to the
        cluster leader.

        :param host_ip: the externally-visible IP of the current node
        :param node_name: the name of this node used internally in the cluster
        :param credentials: credentials used for joining the cluster
        :param join_addrs: IPs of the nodes in the cluster to join
        :param options: additional configuration inputs
        :return: current state of the cluster
        :rtype: ClusterState
        """
        response = self.api.put('/cluster', data={
            'host_ip': host_ip,
            'node_name': node_name,
            'credentials': credentials,
            'join_addrs': join_addrs,
            'options': options,
            'required': required
        })
        return ClusterState(response)

    def update(self, **kwargs):
        """
        Update the HA cluster configuration.

        :param kwargs: new cluster configuration
        :return: current cluster status
        """
        response = self.api.patch('/cluster', data=kwargs)
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
        return ListResponse([ClusterNode(item) for item in response['items']],
                            response.get('metadata', {}))

    def add(self, host_ip, node_name):
        """
        Add a node to the cluster.

        This allows the cluster leader to prepare the internal data store
        for the new node, run validations, and generate credentials for it.

        Use the credentials returned as part of the ClusterNode structure
        when joining the cluster.

        :param host_ip: the externally-visible IP of the current node
        :param node_name: the name of this node used internally in the cluster
        :return: representation of the node that will join the cluster
        :rtype: ClusterNode
        """
        response = self.api.put('/cluster/nodes/{0}'.format(node_name), data={
            'host_ip': host_ip,
            'node_name': node_name
        })
        return ClusterNode(response)

    def details(self, node_id):
        """
        Get the details of a node in the cluster - configuration and state

        :param node_id: Node's id to get
        :return: details of the node
        """
        response = self.api.get('/cluster/nodes/{0}'.format(node_id))
        return ClusterNode(response)

    def update(self, node_id, options):
        """
        Update a cluster nodes's settings

        :param node_id: ID of the node to be updated
        :param options: a dict of new node options
        :return: details of the node
        """
        response = self.api.patch('/cluster/nodes/{0}'.format(node_id),
                                  data=options)
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
