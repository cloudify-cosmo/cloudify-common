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

__author__ = 'idanmo'


class Execution(dict):
    """
    Cloudify workflow execution.
    """
    TERMINATED = 'terminated'
    FAILED = 'failed'
    CANCELLED = 'cancelled'
    PENDING = 'pending'
    STARTED = 'started'
    CANCELLING = 'cancelling'
    FORCE_CANCELLING = 'force_cancelling'
    END_STATES = [TERMINATED, FAILED, CANCELLED]

    def __init__(self, execution):
        self.update(execution)

    @property
    def id(self):
        """
        :return: The execution's id.
        """
        return self['id']

    @property
    def status(self):
        """
        :return: The execution's status.
        """
        return self['status']

    @property
    def error(self):
        """
        :return: The execution error in a case of failure, otherwise None.
        """
        return self['error']

    @property
    def workflow_id(self):
        """
        :return: The id of the workflow this execution represents.
        """
        return self['workflow_id']

    @property
    def parameters(self):
        """
        :return: the execution's parameters
        """
        return self['parameters']


class ExecutionsClient(object):

    def __init__(self, api):
        self.api = api

    def list(self, deployment_id):
        """
        Returns a list of executions for the provided deployment's id.

        :param deployment_id: Deployment id to get a list of executions for.
        :return: Executions list.
        """
        assert deployment_id
        uri = '/deployments/{0}/executions'.format(deployment_id)
        response = self.api.get(uri)
        return [Execution(item) for item in response]

    def get(self, execution_id):
        """
        Get execution by its id.

        :param execution_id: Id of the execution to get.
        :return: Execution.
        """
        assert execution_id
        uri = '/executions/{0}'.format(execution_id)
        response = self.api.get(uri)
        return Execution(response)

    def update(self, execution_id, status, error=None):
        """
        Update execution with the provided status and optional error.

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

    def cancel(self, execution_id, force=False):
        """
        Cancels the execution which matches the provided execution id.
        :param execution_id: Id of the execution to cancel.
        :param force: Boolean describing whether to send a 'cancel' or a 'force-cancel' action  # NOQA
        :return: Cancelled execution.
        """
        uri = '/executions/{0}'.format(execution_id)
        action = 'force-cancel' if force else 'cancel'
        response = self.api.post(uri,
                                 data={'action': action},
                                 expected_status_code=201)
        return Execution(response)
