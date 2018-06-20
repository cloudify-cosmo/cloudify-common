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


class Execution(dict):
    """Cloudify workflow execution."""
    TERMINATED = 'terminated'
    FAILED = 'failed'
    CANCELLED = 'cancelled'
    PENDING = 'pending'
    STARTED = 'started'
    CANCELLING = 'cancelling'
    FORCE_CANCELLING = 'force_cancelling'
    KILL_CANCELLING = 'kill_cancelling'
    END_STATES = [TERMINATED, FAILED, CANCELLED]

    def __init__(self, execution):
        self.update(execution)
        # default to status for compatibility with pre-4.4 managers
        self.setdefault('status_display', self.status)

    @property
    def id(self):
        """
        :return: The execution's id.
        """
        return self.get('id')

    @property
    def deployment_id(self):
        """
        :return: The deployment's id this execution is related to.
        """
        return self.get('deployment_id')

    @property
    def status(self):
        """
        :return: The execution's status.
        """
        return self.get('status')

    @property
    def status_display(self):
        """
        :return: The human-readable form of the execution's status.
        """
        return self.get('status_display')

    @property
    def error(self):
        """
        :return: The execution error in a case of failure, otherwise None.
        """
        return self.get('error')

    @property
    def workflow_id(self):
        """
        :return: The id of the workflow this execution represents.
        """
        return self.get('workflow_id')

    @property
    def parameters(self):
        """
        :return: The execution's parameters
        """
        return self.get('parameters') or {}

    @property
    def is_system_workflow(self):
        """
        :return: True if the workflow executed is a system workflow, otherwise
         False
        """
        return self.get('is_system_workflow', False)

    @property
    def created_at(self):
        """
        :return: The execution creation time.
        """
        return self.get('created_at')

    @property
    def ended_at(self):
        """
        :return: The execution end time.
        """
        return self.get('ended_at')

    @property
    def created_by(self):
        """
        :return: The name of the execution creator.
        """
        return self.get('created_by')


class ExecutionsClient(object):

    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'executions'
        self._wrapper_cls = Execution

    def _create_filters(
            self,
            deployment_id=None,
            include_system_workflows=False,
            sort=None,
            is_descending=False,
            **kwargs
    ):
        params = {'_include_system_workflows': include_system_workflows}
        if deployment_id:
            params['deployment_id'] = deployment_id
        params.update(kwargs)
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort
        return params

    def list(self, _include=None, **kwargs):
        """Returns a list of executions.

        :param deployment_id: Optional deployment id to get executions for.
        :param include_system_workflows: Include executions of system
               workflows
        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.Execution.fields
        :return: Executions list.
        """
        params = self._create_filters(**kwargs)

        response = self.api.get(
            '/{self._uri_prefix}'.format(self=self),
            params=params,
            _include=_include)
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def get(self, execution_id, _include=None):
        """Get execution by its id.

        :param execution_id: Id of the execution to get.
        :param _include: List of fields to include in response.
        :return: Execution.
        """
        assert execution_id
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=execution_id)
        response = self.api.get(uri, _include=_include)
        return self._wrapper_cls(response)

    def update(self, execution_id, status, error=None):
        """Update execution with the provided status and optional error.

        :param execution_id: Id of the execution to update.
        :param status: Updated execution status.
        :param error: Updated execution error (optional).
        :return: Updated execution.
        """

        uri = '/executions/{0}'.format(execution_id)
        params = {'status': status}
        if error:
            params['error'] = error
        response = self.api.patch(uri, data=params)
        return Execution(response)

    def start(self, deployment_id, workflow_id, parameters=None,
              allow_custom_parameters=False, force=False, dry_run=False):
        """Starts a deployment's workflow execution whose id is provided.

        :param deployment_id: The deployment's id to execute a workflow for.
        :param workflow_id: The workflow to be executed id.
        :param parameters: Parameters for the workflow execution.
        :param allow_custom_parameters: Determines whether to allow\
         parameters which weren't defined in the workflow parameters schema\
         in the blueprint.
        :param force: Determines whether to force the execution of the\
         workflow in a case where there's an already running execution for\
         this deployment.
        :param dry_run: If set to true, no actual actions will be performed.\
        This is a dry run of the execution
        :raises: IllegalExecutionParametersError
        :return: The created execution.
        """
        assert deployment_id
        assert workflow_id
        data = {
            'deployment_id': deployment_id,
            'workflow_id': workflow_id,
            'parameters': parameters,
            'allow_custom_parameters': str(allow_custom_parameters).lower(),
            'force': str(force).lower(),
            'dry_run': str(dry_run).lower()
        }
        uri = '/executions'
        response = self.api.post(uri,
                                 data=data,
                                 expected_status_code=201)
        return Execution(response)

    def cancel(self, execution_id, force=False, kill=False):
        """Cancels the execution which matches the provided execution id.

        :param execution_id: Id of the execution to cancel.
        :param force: Boolean describing whether to send a 'cancel' or a 'force-cancel' action  # NOQA
        :return: Cancelled execution.
        """
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=execution_id)
        action = 'kill' if kill else 'force-cancel' if force else 'cancel'
        response = self.api.post(uri,
                                 data={'action': action},
                                 expected_status_code=200)
        return self._wrapper_cls(response)
