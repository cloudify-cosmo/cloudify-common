# Copyright (c) 2017-2019 Cloudify Platform Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from cloudify.deployment_dependencies import (create_deployment_dependency,
                                              DEPENDENCY_CREATOR)


from cloudify_rest_client.responses import ListResponse


class InterDeploymentDependency(dict):

    def __init__(self, dependency):
        self.update(dependency)

    @property
    def id(self):
        return self['id']

    @property
    def dependency_creator(self):
        return self[DEPENDENCY_CREATOR]

    @property
    def source_deployment_id(self):
        return self['source_deployment_id']

    @property
    def target_deployment_id(self):
        return self['target_deployment_id']

    @property
    def created_at(self):
        return self['created_at']


class InterDeploymentDependencyClient(object):
    """
    Cloudify's plugins update management client.
    """

    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'deployments/inter-deployment-dependencies'
        self._wrapper_cls = InterDeploymentDependency

    def _wrap_list(self, response):
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def create(self, dependency_creator, source_deployment, target_deployment):
        """Creates an inter-deployment dependency.

        :param dependency_creator: a string representing the entity that
         is responsible for this dependency (e.g. an intrinsic function
         blueprint path, 'node_instances.some_node_instance', etc.).
        :param source_deployment: source deployment that depends on the target
         deployment.
        :param target_deployment: the deployment that the source deployment
         depends on.
        :return: an InterDeploymentDependency object.
        """
        data = create_deployment_dependency(dependency_creator,
                                            source_deployment,
                                            target_deployment)
        response = self.api.put(
            '/{self._uri_prefix}'.format(self=self), data=data)
        return self._wrapper_cls(response)

    def delete(self,
               dependency_creator,
               source_deployment,
               target_deployment):
        """Deletes an inter-deployment dependency.

        :param dependency_creator: a string representing the entity that
         is responsible for this dependency (e.g. an intrinsic function
         blueprint path, 'node_instances.some_node_instance', etc.).
        :param source_deployment: source deployment that depends on the target
         deployment.
        :param target_deployment: the deployment that the source deployment
         depends on.
        :return: an InterDeploymentDependency object.
        """
        data = create_deployment_dependency(dependency_creator,
                                            source_deployment,
                                            target_deployment)
        response = self.api.delete(
            '/{self._uri_prefix}'.format(self=self), data=data)
        return self._wrapper_cls(response)

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of available  inter-deployment dependencies.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.InterDeploymentDependencies.fields
        :return: InterDeploymentDependencies list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                _include=_include,
                                params=params)
        return self._wrap_list(response)

    def restore(self, deployment_id, runtime_only_evaluation=False):
        """
        Updating the inter deployment dependencies table from the specified
        deployment during an upgrade

       """
        data = {
            'deployment_id': deployment_id,
            'runtime_only_evaluation': runtime_only_evaluation
        }
        self.api.post('/{self._uri_prefix}/restore'.format(self=self),
                      data=data)
