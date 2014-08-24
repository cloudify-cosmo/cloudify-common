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


class CloudifyClientError(Exception):

    def __init__(self, message, server_traceback=None, status_code=-1):
        super(CloudifyClientError, self).__init__(message)
        self.status_code = status_code
        self.server_traceback = server_traceback

    def __str__(self):
        if self.status_code != -1:
            return '{}: {}'.format(self.status_code, self.message)
        return self.message


class CreateDeploymentInProgressError(CloudifyClientError):
    """
    Raised when there's attempt to execute a deployment workflow and
    deployment creation workflow execution is still running.
    In such a case, workflow execution should be retried after a reasonable
    time or after the execution of deployment workers installation
    has terminated.
    """

    ERROR_CODE = 'deployment_workers_not_yet_installed_error'

    def __init__(self, message, server_traceback=None, status_code=-1):
        super(CreateDeploymentInProgressError, self).__init__(message,
                                                              server_traceback,
                                                              status_code)


class IllegalExecutionParametersError(CloudifyClientError):
    """
    Raised when an attempt to execute a workflow with wrong/missing parameters
    has been made.
    """

    ERROR_CODE = 'illegal_execution_parameters_error'

    def __init__(self, message, server_traceback=None, status_code=-1):
        super(IllegalExecutionParametersError, self).__init__(
            message, server_traceback, status_code)


class NoSuchIncludeFieldError(CloudifyClientError):
    """
    Raised when an _include query parameter contains a field which does not
    exist for the queried data model.
    """

    ERROR_CODE = 'no_such_include_field_error'

    def __init__(self, message, server_traceback=None, status_code=-1):
        super(NoSuchIncludeFieldError, self).__init__(
            message, server_traceback, status_code)


class MissingRequiredDeploymentInputError(CloudifyClientError):
    """
    Raised when a required deployment input was not specified on deployment
    creation.
    """
    ERROR_CODE = 'missing_required_deployment_input_error'

    def __init__(self, message, server_traceback=None, status_code=-1):
        super(MissingRequiredDeploymentInputError, self).__init__(
            message, server_traceback, status_code)


class UnknownDeploymentInputError(CloudifyClientError):
    """
    Raised when an unexpected input was specified on deployment creation.
    """
    ERROR_CODE = 'unknown_deployment_input_error'

    def __init__(self, message, server_traceback=None, status_code=-1):
        super(UnknownDeploymentInputError, self).__init__(
            message, server_traceback, status_code)
