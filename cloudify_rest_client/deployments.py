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
from cloudify_rest_client.constants import VisibilityState

from .labels import Label


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
        if self.get('labels'):
            self['labels'] = [Label(item) for item in self['labels']]

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
    def labels(self):
        """
        :return: The labels of this deployment.
        """
        return self.get('labels')

    @property
    def deployment_groups(self):
        """
        :return: IDs of deployment groups that this deployment belongs to
        """
        return self.get('deployment_groups')

    @property
    def latest_execution_status(self):
        """
        :return: The deployment latest execution status
        """
        return self.get('latest_execution_status')

    @property
    def installation_status(self):
        """
        :return: The deployment installation status
        """
        return self.get('installation_status')

    @property
    def deployment_status(self):
        """
        :return: The overall deployment status
        """
        return self.get('deployment_status')

    @property
    def sub_services_status(self):
        """
        :return: The aggregated sub services status
        """
        return self.get('sub_services_status')

    @property
    def sub_environments_status(self):
        """
        :return: The aggregated sub environments status
        """
        return self.get('sub_environments_status')

    @property
    def sub_services_count(self):
        """
        :return: The aggregated sub services count
        """
        return self.get('sub_services_count')

    @property
    def sub_environments_count(self):
        """
        :return: The aggregated sub environments count
        """
        return self.get('sub_environments_count')

    @property
    def environment_type(self):
        """
        :return: The environment type
        """
        return self.get('environment_type')

    @property
    def latest_execution_total_operations(self):
        """
        :return: The total operations for latest execution of deployment
        """
        return self.get('latest_execution_total_operations')

    @property
    def latest_execution_finished_operations(self):
        """
        :return: The finished operations for latest execution of deployment
        """
        return self.get('latest_execution_finished_operations')

    @property
    def tenant_name(self):
        return self.get('tenant_name')

    def is_environment(self):
        """
        :return: True if deployment is an environment
        """
        for label in self.labels:
            if label['key'].lower() == 'csys-obj-type':
                return label['value'].lower() == 'environment'
        return False

    @property
    def display_name(self):
        """
        :return: The deployment's display name
        """
        return self.get('display_name')

    @property
    def policy_types(self):
        return self.get('policy_types')

    @property
    def policy_triggers(self):
        return self.get('policy_triggers')

    @property
    def groups(self):
        return self.get('groups')

    @property
    def scaling_groups(self):
        return self.get('scaling_groups')


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


class DeploymentGroup(dict):
    def __init__(self, group):
        super(DeploymentGroup, self).__init__()
        self.update(group)

    @property
    def deployment_ids(self):
        """IDs of deployments belonging to this group"""
        return self['deployment_ids']

    @property
    def default_inputs(self):
        """Default inputs for new deployments created in this group"""
        return self['default_inputs']

    @property
    def default_blueprint_id(self):
        """Default blueprint for new deployments created in this group"""
        return self['default_blueprint_id']

    @property
    def labels(self):
        """Labels of this deployment group"""
        return self.get('labels')

    @property
    def description(self):
        """Description of this deployment group"""
        return self.get('description')


class DeploymentGroupsClient(object):
    def __init__(self, api):
        self.api = api

    def list(self):
        """List all deployment groups."""
        response = self.api.get('/deployment-groups')
        return ListResponse(
            [DeploymentGroup(item) for item in response['items']],
            response['metadata'])

    def get(self, group_id):
        """Get the specified deployment group."""
        response = self.api.get('/deployment-groups/{0}'.format(group_id))
        return DeploymentGroup(response)

    def put(self, group_id, visibility=VisibilityState.TENANT,
            description=None, blueprint_id=None, default_inputs=None,
            labels=None, filter_id=None, deployment_ids=None,
            new_deployments=None, deployments_from_group=None):
        """Create or update the specified deployment group.

        Setting group deployments using this method (via either filter_id
        or deployment_ids) will set, NOT ADD, ie. it will remove all other
        deployments from the group.

        :param visibility: visibility of the group
        :param description: description of the group
        :param blueprint_id: the default blueprint to use when extending
        :param default_inputs: the default inputs to use when extending
        :param deployment_ids: make the group contain these
            existing deployments
        :param labels: labels for this group; those will be automatically
            added to all deployments created by this group
        :param filter_id: set the group to contain the deployments matching
            this filter
        :param new_deployments: create new deployments using this
            specification, merged with the group's default_blueprint
            and default_inputs
        :type new_deployments: a list of dicts, each can contain the
            keys "id", "inputs", "labels"
        :param deployments_from_group: add all deployments belonging to the
            group given by this id
        :return: the created deployment group
        """
        response = self.api.put(
            '/deployment-groups/{0}'.format(group_id),
            data={
                'visibility': visibility,
                'description': description,
                'blueprint_id': blueprint_id,
                'default_inputs': default_inputs,
                'labels': labels,
                'filter_id': filter_id,
                'deployment_ids': deployment_ids,
                'new_deployments': new_deployments,
                'deployments_from_group': deployments_from_group,
            }
        )
        return DeploymentGroup(response)

    def add_deployments(self, group_id, deployment_ids=None, count=None,
                        new_deployments=None, filter_id=None,
                        filter_rules=None, deployments_from_group=None,
                        batch_size=5000):
        """Add the specified deployments to the group

        :param group_id: add deployments to this group
        :param deployment_ids: add these pre-existing deployments
        :param count: create this many deployments using the group's
            default_inputs and default_blueprint, and add them to the
            group. Mutally exclusive with inputs.
        :param new_deployments: create new deployments using this
            specification, merged with the group's default_blueprint
            and default_inputs. Mutually exclusive with count.
        :type new_deployments: a list of dicts, each can contain the
            keys "id", "inputs", "labels"
        :param filter_id: add deployments matching this filter
        :param filter_rules: add deployments matching these filter rules
        :param deployments_from_group: add all deployments belonging to the
            group given by this id
        :param batch_size: when creating new deployments, create this many
            at a time (do multiple HTTP calls if needed)
        :return: the updated deployment group
        """
        if new_deployments is not None and count is not None:
            raise ValueError('provide either count or new_deployments, '
                             'not both')
        if count:
            new_deployments = [{}] * count

        if new_deployments and len(new_deployments) > batch_size:
            batches = [[]]
            for dep_spec in new_deployments:
                batches[-1].append(dep_spec)
                if batch_size and len(batches[-1]) >= batch_size:
                    batches.append([])
        else:
            batches = [new_deployments]

        for new_deployments_batch in batches:
            response = self.api.patch(
                '/deployment-groups/{0}'.format(group_id),
                data={
                    'add': {
                        'deployment_ids': deployment_ids,
                        'new_deployments': new_deployments_batch or None,
                        'filter_id': filter_id,
                        'filter_rules': filter_rules,
                        'deployments_from_group': deployments_from_group,
                    }
                }
            )
            # don't send these again
            deployment_ids = filter_id = deployments_from_group = None
        return DeploymentGroup(response)

    def remove_deployments(self, group_id, deployment_ids=None,
                           filter_id=None, filter_rules=None,
                           deployments_from_group=None):
        """Remove the specified deployments from the group

        :param group_id: remove deployments from this group
        :param deployment_ids: remove these deployment from the group
        :param filter_id: remove deployments matching this filter
        :param filter_rules: remove deployments matching these filter rules
        :param deployments_from_group: remove all deployments belonging to the
            group given by this id
        :return: the updated deployment group
        """
        response = self.api.patch(
            '/deployment-groups/{0}'.format(group_id),
            data={
                'remove': {
                    'deployment_ids': deployment_ids,
                    'filter_id': filter_id,
                    'filter_rules': filter_rules,
                    'deployments_from_group': deployments_from_group,
                }
            }
        )
        return DeploymentGroup(response)

    def delete(self, group_id, delete_deployments=False,
               force=False, with_logs=False):
        """Delete a deployment group. By default, keep the deployments.

        :param group_id: the group to remove
        :param delete_deployments: also delete all deployments belonging
            to this group
        :param force: same meaning as in deployments.delete
        :param with_logs: same meaning as in deployments.delete
        """
        self.api.delete(
            '/deployment-groups/{0}'.format(group_id),
            params={
                'delete_deployments': delete_deployments,
                'force': force,
                'delete_logs': with_logs,
            },
            expected_status_code=204
        )


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

    def list(self, _include=None, sort=None, is_descending=False,
             filter_id=None, filter_rules=None, **kwargs):
        """
        Returns a list of all deployments.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param filter_id: A filter ID to filter the deployments list by
        :param filter_rules: A list of filter rules to filter the
               deployments list by
        :param kwargs: Optional filter fields. for a list of available fields
               see the REST service's models.Deployment.fields
        :return: Deployments list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort
        if _include:
            params['_include'] = ','.join(_include)
        if filter_id:
            params['_filter_id'] = filter_id

        if filter_rules:
            response = self.api.post('/searches/deployments', params=params,
                                     data={'filter_rules': filter_rules})
        else:
            response = self.api.get('/deployments', params=params)

        return ListResponse([Deployment(item) for item in response['items']],
                            response['metadata'])

    def get(self,
            deployment_id,
            _include=None,
            all_sub_deployments=True
            ):
        """
        Returns a deployment by its id.

        :param deployment_id: Id of the deployment to get.
        :param _include: List of fields to include in response.
        :param all_sub_deployments: The values for sub_services_count and
        sub_environments_count will represent recursive numbers. Otherwise
        if its False, then only the direct services and environments
        will be considered
        :return: Deployment.
        """
        assert deployment_id
        uri = '/deployments/{0}'.format(deployment_id)
        response = self.api.get(
            uri,
            _include=_include,
            params={'all_sub_deployments': all_sub_deployments}
        )
        return Deployment(response)

    def create(self,
               blueprint_id,
               deployment_id,
               inputs=None,
               visibility=VisibilityState.TENANT,
               skip_plugins_validation=False,
               site_name=None,
               runtime_only_evaluation=False,
               labels=None,
               display_name=None):
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
        :param labels: The deployment's labels. A list of 1-entry
            dictionaries: [{<key1>: <value1>}, {<key2>: <value2>}, ...]'
        :param display_name: The deployment's display name.
        :return: The created deployment.
        """
        assert blueprint_id
        assert deployment_id
        data = {'blueprint_id': blueprint_id, 'visibility': visibility}
        if inputs:
            data['inputs'] = inputs
        if site_name:
            data['site_name'] = site_name
        if labels:
            data['labels'] = labels
        if display_name:
            data['display_name'] = display_name
        data['skip_plugins_validation'] = skip_plugins_validation
        data['runtime_only_evaluation'] = runtime_only_evaluation
        uri = '/deployments/{0}'.format(deployment_id)
        response = self.api.put(uri, data, expected_status_code=201)
        return Deployment(response)

    def delete(self, deployment_id,
               force=False,
               delete_db_mode=False,
               with_logs=False):
        """
        Deletes the deployment whose id matches the provided deployment id.
        By default, deletion of a deployment with live nodes or installations
        which depend on it is not allowed. This behavior can be changed
        using the force argument.

        :param deployment_id: The deployment's to be deleted id.
        :param force: Delete deployment even if there are existing live nodes
               for it, or existing installations which depend on it.
        :param delete_db_mode: deprecated and does nothing
        :param with_logs: when set to true, the management workers' logs for
               the deployment are deleted as well.
        :return: The deleted deployment.
        """
        assert deployment_id
        params = {'force': force, 'delete_logs': with_logs}
        if delete_db_mode:
            warnings.warn('delete_db_mode is deprecated and does nothing',
                          DeprecationWarning)

        self.api.delete(
            '/deployments/{0}'.format(deployment_id), params=params)

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

    def update_labels(self, deployment_id, labels):
        """
        Updates the deployment's labels.

        :param deployment_id: Deployment's id to update.
        :param labels: The new deployment's labels. A list of 1-entry
            dictionaries: [{<key1>: <value1>}, {<key2>: <value2>}, ...]'
        :return: The deployment
        """
        data = {'labels': labels}
        updated_dep = self.api.patch(
            '/deployments/{0}'.format(deployment_id), data=data)
        return Deployment(updated_dep)

    def set_attributes(self, deployment_id, **kwargs):
        """Set kwargs on the deployment.

        This is used internally for first populating the deployment
        with the attributes from the plan.
        For updating existing deployments, use the deployment update methods.
        """
        updated_dep = self.api.patch(
            '/deployments/{0}'.format(deployment_id), data=kwargs)
        return Deployment(updated_dep)
