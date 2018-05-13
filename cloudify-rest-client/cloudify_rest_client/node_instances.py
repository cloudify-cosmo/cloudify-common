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


class NodeInstance(dict):
    """
    Cloudify node instance.
    """

    @property
    def id(self):
        """
        :return: The identifier of the node instance.
        """
        return self.get('id')

    @property
    def node_id(self):
        """
        :return: The identifier of the node whom this is in instance of.
        """
        return self.get('node_id')

    @property
    def relationships(self):
        """
        :return: The node instance relationships.
        """
        return self.get('relationships')

    @property
    def host_id(self):
        """
        :return: The node instance host_id.
        """
        return self.get('host_id')

    @property
    def deployment_id(self):
        """
        :return: The deployment id the node instance belongs to.
        """
        return self.get('deployment_id')

    @property
    def created_by(self):
        """
        :return: The name of the node instance creator.
        """
        return self.get('created_by')

    @property
    def runtime_properties(self):
        """
        :return: The runtime properties of the node instance.
        """
        return self.get('runtime_properties')

    @property
    def state(self):
        """
        :return: The current state of the node instance.
        """
        return self.get('state')

    @property
    def version(self):
        """
        :return: The current version of the node instance
         (used for optimistic locking on update)
        """
        return self.get('version')

    @property
    def scaling_groups(self):
        """
        :return: Scaling group instances this node instance belongs to.
        """
        return self.get('scaling_groups', [])


class NodeInstancesClient(object):

    def __init__(self, api):
        self.api = api
        self._wrapper_cls = NodeInstance
        self._uri_prefix = 'node-instances'

    def get(self, node_instance_id, _include=None, evaluate_functions=False):
        """
        Returns the node instance for the provided node instance id.

        :param node_instance_id: The identifier of the node instance to get.
        :param _include: List of fields to include in response.
        :param evaluate_functions: Evaluate intrinsic functions
        :return: The retrieved node instance.
        """
        assert node_instance_id
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=node_instance_id)
        params = {'_evaluate_functions': evaluate_functions}
        response = self.api.get(uri, params=params, _include=_include)
        return self._wrapper_cls(response)

    def update(self,
               node_instance_id,
               state=None,
               runtime_properties=None,
               version=1):
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
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=node_instance_id)
        data = {'version': version}
        if runtime_properties is not None:
            data['runtime_properties'] = runtime_properties
        if state is not None:
            data['state'] = state
        response = self.api.patch(uri, data=data)
        return NodeInstance(response)

    def _create_filters(
            self,
            sort=None,
            is_descending=False,
            deployment_id=None,
            node_id=None,
            node_name=None,
            **kwargs
    ):
        params = {}
        if node_name:
            warnings.warn("'node_name' filtering capability is deprecated, use"
                          " 'node_id' instead", DeprecationWarning)
            params['node_id'] = node_name
        elif node_id:
            params['node_id'] = node_id
        if deployment_id:
            params['deployment_id'] = deployment_id

        params.update(kwargs)

        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        return params

    def list(self, _include=None, **kwargs):
        """
        Returns a list of node instances which belong to the deployment
        identified by the provided deployment id.

        :param deployment_id: Optional deployment id to list node instances
                              for.
        :param node_name: Optional node name to only fetch node instances with
                          this name. The node_name positional argument will be
                          deprecated as of the next rest-client version.
                          Use node_id instead.
        :param node_id: Equivalent to node_name.
        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. for a list of available fields
               see the REST service's models.DeploymentNodeInstance.fields
        :return: Node instances.
        :rtype: list
        """

        params = self._create_filters(**kwargs)
        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                params=params,
                                _include=_include)

        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )
