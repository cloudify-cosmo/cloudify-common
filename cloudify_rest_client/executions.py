########
# Copyright (c) 2014-2019 Cloudify Platform Ltd. All rights reserved
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
    QUEUED = 'queued'
    SCHEDULED = 'scheduled'
    END_STATES = [TERMINATED, FAILED, CANCELLED]

    def __init__(self, execution):
        self.update(execution)

        if self.status:
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
    def blueprint_id(self):
        """
        :return: The deployment's main blueprint id this execution is
                 related to.
        """
        return self.get('blueprint_id')

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
    def started_at(self):
        """
        :return: The execution start time.
        """
        return self.get('started_at')

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

    @property
    def scheduled_for(self):
        """
        :return: The time this execution is scheduled for (if any)
        """
        return self.get('scheduled_for')

    @property
    def is_dry_run(self):
        """
        :return: True if the execution was performed as a dry run
        """
        return self.get('is_dry_run', False)

    @property
    def total_operations(self):
        """
        :return: The total count of operations in this execution
        """
        return self.get('total_operations', False)

    @property
    def finished_operations(self):
        """
        :return: The count of finished operations in this execution
        """
        return self.get('finished_operations', False)


class ExecutionGroup(dict):
    def __init__(self, group):
        super(ExecutionGroup, self).__init__()
        self.update(group)

    @property
    def id(self):
        """The ID of this group"""
        return self['id']

    @property
    def execution_ids(self):
        """IDs of executions in this group"""
        return self.get('execution_ids')

    @property
    def status(self):
        """Status of this group, based on the status of each execution"""
        return self.get('status')

    @property
    def deployment_group_id(self):
        """Deployment group ID that this execution group was started from"""
        return self.get('deployment_group_id')

    @property
    def workflow_id(self):
        """The workflow that this execution group is running"""
        return self.get('workflow_id')

    @property
    def concurrency(self):
        """The group runs this many executions at a time"""
        return self.get('concurrency')


class ExecutionGroupsClient(object):
    def __init__(self, api):
        self.api = api

    def list(self, **kwargs):
        response = self.api.get('/execution-groups', params=kwargs)
        return ListResponse(
            [ExecutionGroup(item) for item in response['items']],
            response['metadata'])

    def get(self, execution_group_id):
        response = self.api.get(
            '/execution-groups/{0}'.format(execution_group_id))
        return ExecutionGroup(response)

    def start(self, deployment_group_id, workflow_id, force=False,
              default_parameters=None, parameters=None,
              concurrency=5):
        """Start an execution group from a deployment group.

        :param deployment_group_id: start an execution for every deployment
            belonging to this deployment group
        :param workflow_id: the workflow to run
        :param force: force concurrent execution
        :param default_parameters: default parameters for every execution
        :param parameters: a dict of {deployment_id: params_dict}, overrides
            the default parameters on a per-deployment basis
        :param concurrency: run this many executions at a time
        """
        response = self.api.post('/execution-groups', data={
            'force': force,
            'deployment_group_id': deployment_group_id,
            'workflow_id': workflow_id,
            'parameters': parameters,
            'default_parameters': default_parameters,
            'concurrency': concurrency
        })
        return ExecutionGroup(response)

    def cancel(self, execution_group_id, force=False, kill=False):
        """Cancel the executions in this group.

        This cancels every non-queued execution according to the params,
        see executions.cancel for their semantics.
        Queued executions are marked cancelled immediately.
        """
        action = 'kill' if kill else 'force-cancel' if force else 'cancel'
        response = self.api.post(
            '/execution-groups/{0}'.format(execution_group_id),
            data={'action': action})
        return ExecutionGroup(response)

    def resume(self, execution_group_id, force=False):
        """Resume the executions in this group."""
        action = 'force-resume' if force else 'resume'
        response = self.api.post(
            '/execution-groups/{0}'.format(execution_group_id),
            data={'action': action})
        return ExecutionGroup(response)

    def set_target_group(self, execution_group_id,
                         success_group=None, failed_group=None):
        """Set the success or failure target group for this execution-group

        Deployments that have executions in this execution-group which
        terminated successfully, will be added to the success group.
        Deployments that have executions in this execution-group which
        failed, will be added to the failure group.
        Cancelled executions have no effect.

        :param execution_group_id: ID of the execution group
        :param success_group: ID of the target success deployment group
        :param success_group: ID of the target failure deployment group
        :return: The updated ExecutionGroup
        """
        response = self.api.patch(
            '/execution-groups/{0}'.format(execution_group_id),
            data={
                'success_group_id': success_group,
                'failure_group_id': failed_group,
            }
        )
        return ExecutionGroup(response)


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

    def should_start(self, execution_id):
        """
        Check if an execution can currently start running (no system exeuctions
        / executions under the same deployment are currently running).

        :param execution_id: Id of the executions that needs to be checked.
        :return: Whether or not this execution can currently start
        """
        assert execution_id
        uri = '/{self._uri_prefix}/{id}/should-start'.format(
            self=self, id=execution_id)
        response = self.api.get(uri)
        return response

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
              allow_custom_parameters=False, force=False, dry_run=False,
              queue=False, schedule=None, wait_after_fail=600):
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
        :param queue: If set, blocked executions will be queued and
            automatically run when possible
        :param schedule: A string representing the date and time this
            workflow should be executed at. If not passed this workflow will be
            executed immediately.

        :raises: IllegalExecutionParametersError
        :return: The created execution.
        """
        assert deployment_id
        assert workflow_id
        if schedule:
            warnings.warn("The 'schedule' flag is deprecated. Please use "
                          "`cfy deployments schedule create instead`",
                          DeprecationWarning)
        data = {
            'deployment_id': deployment_id,
            'workflow_id': workflow_id,
            'parameters': parameters,
            'allow_custom_parameters': str(allow_custom_parameters).lower(),
            'force': str(force).lower(),
            'dry_run': str(dry_run).lower(),
            'queue': str(queue).lower(),
            'scheduled_time': schedule,
            'wait_after_fail': wait_after_fail
        }
        uri = '/executions'
        response = self.api.post(uri,
                                 data=data,
                                 expected_status_code=201)
        return Execution(response)

    def cancel(self, execution_id, force=False, kill=False):
        """Cancels an execution.

        :param execution_id: id of the execution to cancel
        :param force: force-cancel the execution: does not wait for the
            workflow function to return
        :param kill: kill the workflow process and the operation processes
        :return: Cancelled execution.
        """
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=execution_id)
        action = 'kill' if kill else 'force-cancel' if force else 'cancel'
        response = self.api.post(uri,
                                 data={'action': action},
                                 expected_status_code=200)
        return self._wrapper_cls(response)

    def resume(self, execution_id, force=False):
        """Resume an execution.

        :param execution_id: Id of the execution to resume.
        :param force: Whether to resume failed/cancelled executions by
                      retrying their failed tasks.
        :return: Resumed execution.
        """
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=execution_id)
        action = 'force-resume' if force else 'resume'
        response = self.api.post(uri,
                                 data={'action': action},
                                 expected_status_code=200)
        return self._wrapper_cls(response)

    def requeue(self, execution_id):
        """
        Requeue an execution (e.g. after snapshot restore).

        :param execution_id: Id of the execution to be requeued.
        :return: Requeued execution.
        """
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=execution_id)
        response = self.api.post(uri,
                                 data={'action': 'requeue'},
                                 expected_status_code=200)
        return self._wrapper_cls(response)

    def delete(self, to_datetime=None, keep_last=None, **kwargs):
        """Deletes finished executions from the DB.

        :param to_datetime: Until which timestamp to delete executions
        :param keep_last: How many most recent executions to keep from deletion
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.Execution.fields
        :return: List of deleted executions.

        Parameters `to_datetime` and `keep_last` are mutually-exclusive.
        """
        data = {}
        if to_datetime:
            data['to_datetime'] = to_datetime.isoformat()
        if keep_last:
            data['keep_last'] = keep_last
        response = self.api.delete('/{self._uri_prefix}'.format(self=self),
                                   data=data,
                                   params=kwargs,
                                   expected_status_code=200)
        return response['items'][0]['count']
