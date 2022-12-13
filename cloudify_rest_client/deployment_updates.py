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


class DeploymentUpdate(dict):

    def __init__(self, update):
        self.update(update)

    @property
    def id(self):
        return self['id']

    @property
    def state(self):
        return self['state']

    @property
    def deployment_id(self):
        return self['deployment_id']

    @property
    def old_blueprint_id(self):
        return self['old_blueprint_id']

    @property
    def new_blueprint_id(self):
        return self['new_blueprint_id']

    @property
    def old_inputs(self):
        return self['old_inputs']

    @property
    def new_inputs(self):
        return self['new_inputs']

    @property
    def steps(self):
        return self['steps']

    @property
    def execution_id(self):
        return self['execution_id']

    @property
    def created_at(self):
        return self['created_at']

    @property
    def runtime_only_evaluation(self):
        return self['runtime_only_evaluation']

    @property
    def deployment_plan(self):
        return self['deployment_plan']


class DeploymentUpdatesClient(object):

    def __init__(self, api):
        self.api = api

    def create(self, update_id, deployment_id, **kwargs):
        """Create a deployment-update object.

        This is only useful from within the deployment-update workflow.
        Do not use this otherwise.
        """
        url = '/deployment-updates/{0}'.format(update_id)
        data = {
            'deployment_id': deployment_id,
        }
        data.update(kwargs)
        return self.api.put(url, data=data, wrapper=DeploymentUpdate)

    def set_attributes(self, update_id, **kwargs):
        """Update a deployment-update object with the given attributes.

        This is only useful from within the deployment-update workflow.
        Do not use this otherwise.
        """
        return self.api.patch(
            '/deployment-updates/{0}'.format(update_id),
            data=kwargs,
        )

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """List deployment updates

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. for a list of available fields
               see the REST service's models.DeploymentUpdate.fields
        """

        uri = '/deployment-updates'
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        return self.api.get(
            uri,
            params=params,
            _include=_include,
            wrapper=ListResponse.of(DeploymentUpdate),
        )

    def bulk_insert(self, updates):
        """Bulk insert deployment updates. For internal use only."""
        return self.api.post(
            '/deployment-updates',
            {'deployment_updates': updates},
            expected_status_code=[201, 204],
        )

    def get(self, update_id, _include=None):
        """Get deployment update

        :param update_id: The update id
        """
        uri = '/deployment-updates/{0}'.format(update_id)
        return self.api.get(uri, _include=_include, wrapper=DeploymentUpdate)

    def update_with_existing_blueprint(
        self,
        deployment_id,
        blueprint_id=None,
        inputs=None,
        skip_install=False,
        skip_uninstall=False,
        skip_reinstall=False,
        skip_drift_check=False,
        skip_heal=False,
        force_reinstall=False,
        workflow_id=None,
        force=False,
        ignore_failure=False,
        install_first=False,
        reinstall_list=None,
        preview=False,
        update_plugins=True,
        runtime_only_evaluation=None,
        auto_correct_types=None,
        reevaluate_active_statuses=None
    ):
        data = {
            'workflow_id': workflow_id,
            'skip_install': skip_install,
            'skip_uninstall': skip_uninstall,
            'skip_reinstall': skip_reinstall,
            'skip_drift_check': skip_drift_check,
            'skip_heal': skip_heal,
            'force_reinstall': force_reinstall,
            'ignore_failure': ignore_failure,
            'install_first': install_first,
            'preview': preview,
            'blueprint_id': blueprint_id,
            'update_plugins': update_plugins,
            'force': force,
        }
        if inputs:
            data['inputs'] = inputs
        if reinstall_list:
            data['reinstall_list'] = reinstall_list
        if runtime_only_evaluation is not None:
            data['runtime_only_evaluation'] = runtime_only_evaluation
        if auto_correct_types is not None:
            data['auto_correct_types'] = auto_correct_types
        if reevaluate_active_statuses is not None:
            data['reevaluate_active_statuses'] = reevaluate_active_statuses
        uri = '/deployment-updates/{0}/update/initiate'.format(deployment_id)
        return self.api.post(uri, data=data, wrapper=DeploymentUpdate)

    def finalize_commit(self, update_id):
        """Finalize the committing process

        :param update_id:
        :return:
        """
        assert update_id

        uri = '/deployment-updates/{0}/update/finalize'.format(update_id)
        return self.api.post(uri, wrapper=DeploymentUpdate)
