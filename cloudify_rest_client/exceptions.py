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

    def __init__(self, message, status_code=-1):
        self.message = message
        self.status_code = status_code

    def __str__(self):
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

    def __init__(self, message, status_code=-1):
        super(CreateDeploymentInProgressError, self).__init__(message,
                                                              status_code)


class MissingExecutionParametersError(CloudifyClientError):
    """

    """

    ERROR_CODE = 'missing_execution_parameters_error'

    def __init__(self, message, status_code=-1):
        super(MissingExecutionParametersError, self).__init__(
            message, status_code)
