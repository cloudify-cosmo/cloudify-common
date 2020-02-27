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

from cloudify_rest_client.responses import ListResponse
from cloudify_rest_client.constants import VisibilityState


class Deployment(dict):
    """
    Cloudify deployment.
    """

    def __init__(self, deployment):
        super(Deployment, self).__init__()
        self.update(deployment)
        if 'workflows' in self and self['workflows']:
            # might be None, for example in response for delete deployment
            self['workflows'] = [Workflow(item) for item in self['workflows']]

    @property
    def id(self):
        """
        :return: The identifier of the deployment.
        """
        return self.get('id')

    @property
    def blueprint_id(self):
        """
        :return: The identifier of the blueprint this deployment belongs to.
        """
        return self.get('blueprint_id')

    @property
    def created_by(self):
        """
        :return: The name of the deployment creator.
        """
        return self.get('created_by')

    @property
    def workflows(self):
        """
        :return: The workflows of this deployment.
        """
        return self.get('workflows')

    @property
    def inputs(self):
        """
        :return: The inputs provided on deployment creation.
        """
        return self.get('inputs')

    @property
    def outputs(self):
        """
        :return: The outputs definition of this deployment.
        """
        return self.get('outputs')

    @property
    def capabilities(self):
        """
        :return: The capabilities definition of this deployment.
        """
        return self.get('capabilities')

    @property
    def description(self):
        """
        :return: The description of this deployment.
        """
        return self.get('description')

    @property
    def site_name(self):
        """
        :return: The site of this deployment.
        """
        return self.get('site_name')

    @property
    def visibility(self):
        """
        :return: The visibility of this deployment.
        """
        return self.get('visibility')

    @property
    def runtime_only_evaluation(self):
        return self.get('runtime_only_evaluation')

    @property
    def creation_execution_id(self):
        return self.get('creation_execution_id')


class Workflow(dict):

    def __init__(self, workflow):
        super(Workflow, self).__init__()
        self.update(workflow)

    @property
    def id(self):
        """
        :return: The workflow's id
        """
        return self['name']

    @property
    def name(self):
        """
        :return: The workflow's name
        """
        return self['name']

    @property
    def parameters(self):
        """
        :return: The workflow's parameters
        """
        return self['parameters']


class DeploymentOutputs(dict):

    def __init__(self, outputs):
        super(DeploymentOutputs, self).__init__()
        self.update(outputs)

    @property
    def deployment_id(self):
        """Deployment Id the outputs belong to."""
        return self['deployment_id']

    @property
    def outputs(self):
        """Deployment outputs as dict."""
        return self['outputs']


class DeploymentCapabilities(dict):

    def __init__(self, capabilities):
        super(DeploymentCapabilities, self).__init__()
        self.update(capabilities)

    @property
    def deployment_id(self):
        """Id of the deployment the capabilities belong to."""
        return self['deployment_id']

    @property
    def capabilities(self):
        """Deployment capabilities as dict."""
        return self['capabilities']


class DeploymentOutputsClient(object):

    def __init__(self, api):
        self.api = api

    def get(self, deployment_id):
        """Gets the outputs for the provided deployment's Id.

        :param deployment_id: Deployment Id to get outputs for.
        :return: Outputs as dict.
        """
        assert deployment_id
        uri = '/deployments/{0}/outputs'.format(deployment_id)
        response = self.api.get(uri)
        return DeploymentOutputs(response)


class DeploymentCapabilitiesClient(object):

    def __init__(self, api):
        self.api = api

    def get(self, deployment_id):
        """Gets the capabilities for the provided deployment's Id.

        :param deployment_id: Deployment Id to get capabilities for.
        :return: Capabilities as dict.
        """
        assert deployment_id
        uri = '/deployments/{0}/capabilities'.format(deployment_id)
        response = self.api.get(uri)
        return DeploymentCapabilities(response)


class DeploymentsClient(object):

    def __init__(self, api):
        self.api = api
        self.outputs = DeploymentOutputsClient(api)
        self.capabilities = DeploymentCapabilitiesClient(api)

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of all deployments.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. for a list of available fields
               see the REST service's models.Deployment.fields
        :return: Deployments list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/deployments',
                                _include=_include,
                                params=params)

        return ListResponse([Deployment(item) for item in response['items']],
                            response['metadata'])

    def get(self, deployment_id, _include=None):
        """
        Returns a deployment by its id.

        :param deployment_id: Id of the deployment to get.
        :param _include: List of fields to include in response.
        :return: Deployment.
        """
        assert deployment_id
        uri = '/deployments/{0}'.format(deployment_id)
        response = self.api.get(uri, _include=_include)
        return Deployment(response)

    def create(self,
               blueprint_id,
               deployment_id,
               inputs=None,
               visibility=VisibilityState.TENANT,
               skip_plugins_validation=False,
               site_name=None,
               runtime_only_evaluation=False):
        """
        Creates a new deployment for the provided blueprint id and
        deployment id.

        :param blueprint_id: Blueprint id to create a deployment of.
        :param deployment_id: Deployment id of the new created deployment.
        :param inputs: Inputs dict for the deployment.
        :param visibility: The visibility of the deployment, can be 'private'
                           or 'tenant'.
        :param skip_plugins_validation: Determines whether to validate if the
            required deployment plugins exist on the manager. If validation
            is skipped, plugins containing source URL will be installed
            from source.
        :param site_name: The name of the site for the deployment.
        :param runtime_only_evaluation: If set, all intrinsic functions
            will only be evaluated at runtime, and no functions will be
            evaluated at parse time.
        :return: The created deployment.
        """
        assert blueprint_id
        assert deployment_id
        data = {'blueprint_id': blueprint_id, 'visibility': visibility}
        if inputs:
            data['inputs'] = inputs
        if site_name:
            data['site_name'] = site_name
        data['skip_plugins_validation'] = skip_plugins_validation
        data['runtime_only_evaluation'] = runtime_only_evaluation
        uri = '/deployments/{0}'.format(deployment_id)
        response = self.api.put(uri, data, expected_status_code=201)
        return Deployment(response)

    def delete(self, deployment_id,
               ignore_live_nodes=False,
               delete_db_mode=False,
               with_logs=False):
        """
        Deletes the deployment whose id matches the provided deployment id.
        By default, deployment with live nodes deletion is not allowed and
        this behavior can be changed using the ignore_live_nodes argument.

        :param deployment_id: The deployment's to be deleted id.
        :param ignore_live_nodes: Determines whether to ignore live nodes.
        :param delete_db_mode: when set to true the deployment is deleted from
               the DB. This option is used by the `delete_dep_env` workflow to
               make sure the dep is deleted AFTER the workflow finished
               successfully.
        :param with_logs: when set to true, the management workers' logs for
               the deployment are deleted as well.
        :return: The deleted deployment.
        """
        assert deployment_id

        ignore_live_nodes = 'true' if ignore_live_nodes else 'false'
        delete_db_mode = 'true' if delete_db_mode else 'false'
        params = {'ignore_live_nodes': ignore_live_nodes,
                  'delete_db_mode': delete_db_mode,
                  'delete_logs': 'true' if with_logs else 'false'}

        response = self.api.delete('/deployments/{0}'.format(deployment_id),
                                   params=params)
        return Deployment(response)

    def set_visibility(self, deployment_id, visibility):
        """
        Updates the deployment's visibility

        :param deployment_id: Deployment's id to update.
        :param visibility: The visibility to update, should be 'tenant'.
        :return: The deployment.
        """
        data = {'visibility': visibility}
        return self.api.patch(
            '/deployments/{0}/set-visibility'.format(deployment_id),
            data=data
        )

    def set_site(self, deployment_id, site_name=None, detach_site=False):
        """
        Updates the deployment's site

        :param deployment_id: Deployment's id to update.
        :param site_name: The site to update
        :param detach_site: True for detaching the current site, making the
                            deployment siteless
        :return: The deployment.
        """
        data = {'detach_site': detach_site}
        if site_name:
            data['site_name'] = site_name
        return self.api.post(
            '/deployments/{0}/set-site'.format(deployment_id),
            data=data
        )
