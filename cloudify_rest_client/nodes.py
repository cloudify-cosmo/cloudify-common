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
import warnings

from cloudify_rest_client.responses import ListResponse


class Node(dict):
    """
    Cloudify node.
    """

    @property
    def id(self):
        """
        :return: The identifier of the node.
        """
        return self.get('id')

    @property
    def deployment_id(self):
        """
        :return: The deployment id the node belongs to.
        """
        return self.get('deployment_id')

    @property
    def created_by(self):
        """
        :return: The name of the node creator.
        """
        return self.get('created_by')

    @property
    def properties(self):
        """
        :return: The static properties of the node.
        """
        return self.get('properties')

    @property
    def operations(self):
        """
        :return: The node operations mapped to plugins.
        :rtype: dict
        """
        return self.get('operations')

    @property
    def relationships(self):
        """
        :return: The node relationships with other nodes.
        :rtype: list
        """
        return self.get('relationships')

    @property
    def blueprint_id(self):
        """
        :return: The id of the blueprint this node belongs to.
        :rtype: str
        """
        return self.get('blueprint_id')

    @property
    def plugins(self):
        """
        :return: The plugins this node has operations mapped to.
        :rtype: dict
        """
        return self.get('plugins')

    @property
    def number_of_instances(self):
        """
        :return: The number of instances this node has.
        :rtype: int
        """

        return int(self.get(
            'number_of_instances')) if 'number_of_instances' in self else None

    @property
    def planned_number_of_instances(self):
        """
        :return: The planned number of instances this node has.
        :rtype: int
        """

        return int(self.get(
            'planned_number_of_instances')) if 'planned_number_of_instances' \
                                               in self else None

    @property
    def deploy_number_of_instances(self):
        """
        :return: The number of instances this set for this node when the
                 deployment was created.
        :rtype: int
        """

        return int(self.get(
            'deploy_number_of_instances')) if 'deploy_number_of_instances' \
                                              in self else None

    @property
    def unavailable_instances(self):
        """Amount of instances that failed their status check."""
        return self.get('unavailable_instances')

    @property
    def drifted_instances(self):
        """Amount of instances that have configuration drift."""
        return self.get('drifted_instances')

    @property
    def host_id(self):
        """
        :return: The id of the node instance which hosts this node.
        :rtype: str
        """
        return self.get('host_id')

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


class NodeTypes(dict):

    def __init__(self, node_type):
        super(NodeTypes, self).__init__()
        self.update(node_type)

    @property
    def id(self):
        """ID of the node."""
        return self['id']

    @property
    def deployment_id(self):
        """ID of the deployment the node belong to."""
        return self['deployment_id']

    @property
    def type(self):
        """Node's type."""
        return self['type']


class NodesClient(object):

    def __init__(self, api):
        self.api = api
        self._wrapper_cls = Node
        self._uri_prefix = 'nodes'
        self.types = NodeTypesClient(api)

    def _create_filters(
            self,
            deployment_id=None,
            node_id=None,
            sort=None,
            is_descending=False,
            evaluate_functions=False,
            **kwargs
    ):
        params = {'_evaluate_functions': evaluate_functions}
        if deployment_id:
            params['deployment_id'] = deployment_id
        if node_id:
            warnings.warn("'node_id' filtering capability is deprecated, use"
                          " 'id' instead", DeprecationWarning)
            params['id'] = node_id
        params.update(kwargs)
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        return params

    def list(self, _include=None, filter_rules=None, constraints=None,
             **kwargs):
        """
        Returns a list of nodes which belong to the deployment identified
        by the provided deployment id.

        :param deployment_id: The deployment's id to list nodes for.
        :param node_id: If provided, returns only the requested node. This
                        parameter is deprecated, use 'id' instead.
        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param constraints: A list of DSL constraints for node_template data
               type to filter the nodes by.
        :param kwargs: Optional filter fields. for a list of available fields
               see the REST service's models.DeploymentNode.fields
        :param evaluate_functions: Evaluate intrinsic functions
        :return: Nodes.
        :rtype: list
        """
        if constraints and filter_rules:
            raise ValueError(
                'provide either filter_rules or DSL constraints, not both')

        params = self._create_filters(**kwargs)
        if filter_rules is not None:
            if _include:
                params['_include'] = ','.join(_include)
            response = self.api.post(
                '/searches/{self._uri_prefix}'.format(self=self),
                params=params,
                data={'filter_rules': filter_rules}
            )
        elif constraints is not None:
            if _include:
                params['_include'] = ','.join(_include)
            response = self.api.post(
                '/searches/{self._uri_prefix}'.format(self=self),
                params=params,
                data={'constraints': constraints}
            )
        else:
            response = self.api.get(
                '/{self._uri_prefix}'.format(self=self),
                params=params,
                _include=_include
            )
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def get(self, deployment_id, node_id, _include=None,
            evaluate_functions=False):
        """
        Returns the node which belongs to the deployment identified
        by the provided deployment id .

        :param deployment_id: The deployment's id of the node.
        :param node_id: The node id.
        :param _include: List of fields to include in response.
        :param evaluate_functions: Evaluate intrinsic functions
        :return: Nodes.
        :rtype: Node
        """
        assert deployment_id
        assert node_id
        result = self.list(deployment_id=deployment_id,
                           id=node_id,
                           _include=_include,
                           evaluate_functions=evaluate_functions)
        if not result:
            return None
        else:
            return result[0]

    def create_many(self, deployment_id, nodes):
        """Create multiple nodes.

        :param deployment_id: the new nodes belong to this deployment
        :param nodes: list of dicts representing the nodes to be created.
            Each node dict must contain at least the keys: id, type.
        :return: None
        """
        self.api.post(
            '/{self._uri_prefix}'.format(self=self),
            data={
                'deployment_id': deployment_id,
                'nodes': nodes
            },
            expected_status_code=(201, 204),
        )

    def update(self, deployment_id, node_id, **kwargs):
        """Update a node with new attributes.

        This is only useful from the deployment-update workflow: updating
        node attributes will do nothing else by itself (it won't re-install
        the existing instances, or re-establish relationships, etc.)

        :param deployment_id: The deployment the node belongs to
        :param node_id: The node id within the given deployment
        :param kwargs: The new node attributes
        """
        self.api.patch(
            '/{self._uri_prefix}/{deployment_id}/{node_id}'
            .format(self=self, deployment_id=deployment_id, node_id=node_id),
            data=kwargs,
            expected_status_code=204,
        )

    def delete(self, deployment_id, node_id):
        """Delete a node

        :param deployment_id: The deployment the node belongs to
        :param node_id: The node id within the given deployment
        """
        self.api.delete(
            '/{self._uri_prefix}/{deployment_id}/{node_id}'
            .format(self=self, deployment_id=deployment_id, node_id=node_id),
            expected_status_code=204,
        )


class NodeTypesClient(object):

    def __init__(self, api):
        self.api = api

    def list(self, node_type=None, constraints=None, **kwargs):
        """
        Returns a list of node's types matching constraints.

        :param deployment_id: An identifier of a deployment which nodes
               are going to be searched.  If omitted, 'deployment_id' key
               should be present in the `constraints` dict, otherwise the
               request will fail.
        :param node_type: If provided, returns only the requested type.
        :param constraints: A list of DSL constraints for node_type
               data type to filter the types by.
        :param kwargs: Optional filter fields. for a list of available fields
               see the REST service's models.Node.fields
        :return: NodeTypes list.
        """
        params = kwargs
        if node_type:
            params['_search'] = node_type

        if constraints is None:
            constraints = dict()

        response = self.api.post('/searches/node-types', params=params,
                                 data={'constraints': constraints})
        return ListResponse(
            items=[NodeTypes(item) for item in response['items']],
            metadata=response['metadata']
        )
