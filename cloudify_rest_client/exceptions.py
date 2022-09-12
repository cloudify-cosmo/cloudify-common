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


class CloudifyClientError(Exception):

    def __init__(self, message, server_traceback=None,
                 status_code=-1, error_code=None, response=None):
        super(CloudifyClientError, self).__init__(message)
        self.status_code = status_code
        self.error_code = error_code
        self.server_traceback = server_traceback
        self.response = response
        self._message = message

    def __str__(self):
        message = self._message
        if self.server_traceback:
            _, _, last_line = self.server_traceback.strip().rpartition('\n')
            if last_line:
                message = '{0} ({1})'.format(self._message, last_line)
        if self.status_code != -1:
            return '{0}: {1}'.format(self.status_code, message)
        return message


class DeploymentEnvironmentCreationInProgressError(CloudifyClientError):
    """
    Raised when there's attempt to execute a deployment workflow and
    deployment environment creation workflow execution is still running.
    In such a case, workflow execution should be retried after a reasonable
    time or after the execution of deployment environment creation workflow
    has terminated.
    """
    ERROR_CODE = 'deployment_environment_creation_in_progress_error'


class DeploymentEnvironmentCreationPendingError(CloudifyClientError):
    """
    Raised when there's attempt to execute a deployment workflow and
    deployment environment creation workflow execution is pending.
    In such a case, workflow execution should be retried after a reasonable
    time or after the execution of deployment environment creation workflow
    has terminated.
    """
    ERROR_CODE = 'deployment_environment_creation_pending_error'


class IllegalExecutionParametersError(CloudifyClientError):
    """
    Raised when an attempt to execute a workflow with wrong/missing parameters
    has been made.
    """
    ERROR_CODE = 'illegal_execution_parameters_error'


class NoSuchIncludeFieldError(CloudifyClientError):
    """
    Raised when an _include query parameter contains a field which does not
    exist for the queried data model.
    """
    ERROR_CODE = 'no_such_include_field_error'


class MissingRequiredDeploymentInputError(CloudifyClientError):
    """
    Raised when a required deployment input was not specified on deployment
    creation.
    """
    ERROR_CODE = 'missing_required_deployment_input_error'


class UnknownDeploymentInputError(CloudifyClientError):
    """
    Raised when an unexpected input was specified on deployment creation.
    """
    ERROR_CODE = 'unknown_deployment_input_error'


class UnknownDeploymentSecretError(CloudifyClientError):
    """
    Raised when a required secret was not found on deployment creation.
    """
    ERROR_CODE = 'unknown_deployment_secret_error'


class UnsupportedDeploymentGetSecretError(CloudifyClientError):
    """
    Raised when an unsupported get_secret intrinsic function appears in
    the blueprint on deployment creation.
    """
    ERROR_CODE = 'unsupported_deployment_get_secret_error'


class FunctionsEvaluationError(CloudifyClientError):
    """
    Raised when function evaluation failed.
    """
    ERROR_CODE = 'functions_evaluation_error'


class UnknownModificationStageError(CloudifyClientError):
    """
    Raised when an unknown modification stage was provided.
    """
    ERROR_CODE = 'unknown_modification_stage_error'


class ExistingStartedDeploymentModificationError(CloudifyClientError):
    """
    Raised when a deployment modification start is attempted while another
    deployment modification is currently started
    """
    ERROR_CODE = 'existing_started_deployment_modification_error'


class DeploymentModificationAlreadyEndedError(CloudifyClientError):
    """
    Raised when a deployment modification finish/rollback is attempted on
    a deployment modification that has already been finished/rolledback
    """
    ERROR_CODE = 'deployment_modification_already_ended_error'


class UserUnauthorizedError(CloudifyClientError):
    """
    Raised when a call has been made to a secured resource with an
    unauthorized user (no credentials / bad credentials)
    """
    ERROR_CODE = 'unauthorized_error'


class ForbiddenError(CloudifyClientError):
    """
    Raised when a call has been made by a user that is not permitted to
    perform it
    """
    ERROR_CODE = 'forbidden_error'


class PluginInUseError(CloudifyClientError):
    """
    Raised if a central deployment agent plugin deletion is attempted and at
    least one deployment is currently using this plugin.
    """
    ERROR_CODE = 'plugin_in_use'


class BlueprintInUseError(CloudifyClientError):
    """
    Raised if an imported blueprint (is used in another active blueprint)
    deletion is attempted.
    """
    ERROR_CODE = 'blueprint_in_use'


class PluginInstallationError(CloudifyClientError):
    """
    Raised if a central deployment agent plugin installation fails.
    """
    ERROR_CODE = 'plugin_installation_error'


class PluginInstallationTimeout(CloudifyClientError):
    """
    Raised if a central deployment agent plugin installation times out.
    """
    ERROR_CODE = 'plugin_installation_timeout'


class MaintenanceModeActiveError(CloudifyClientError):
    """
    Raised when a call has been blocked due to maintenance mode being active.
    """
    ERROR_CODE = 'maintenance_mode_active'

    def __str__(self):
        return self._message


class MaintenanceModeActivatingError(CloudifyClientError):
    """
    Raised when a call has been blocked while maintenance mode is activating.
    """
    ERROR_CODE = 'entering_maintenance_mode'

    def __str__(self):
        return self._message


class NotModifiedError(CloudifyClientError):
    """
    Raised when a 304 not modified error was returned
    """
    ERROR_CODE = 'not_modified'

    def __str__(self):
        return self._message


class InvalidExecutionUpdateStatus(CloudifyClientError):
    """
    Raised when execution update failed do to invalid status update
    """
    ERROR_CODE = 'invalid_exception_status_update'


class NotClusterMaster(CloudifyClientError):
    """
    Raised when the request was served by a manager that is not the master
    node of a manager cluster.
    The client should query for the cluster status to learn the master's
    address, and retry the request.
    If the client stores the server address, it should update the storage
    with the new master node address.
    """
    ERROR_CODE = 'not_cluster_master'


class RemovedFromCluster(CloudifyClientError):
    """
    Raised when attempting to contact a manager that was removed from a
    cluster.
    The client should retry the request with another manager in the cluster.
    If the client stores the server address, it should remove this node's
    address from storage.
    """
    ERROR_CODE = 'removed_from_cluster'


class DeploymentPluginNotFound(CloudifyClientError):
    """
    Raised when a plugin is listed in the blueprint but is not
    installed on the manager.
    """
    ERROR_CODE = 'deployment_plugin_not_found'


class IncompatibleClusterArchitectureError(CloudifyClientError):
    """
    Raised when a cluster node with architecture X is trying to join a cluster
    with architecture Y

    E.G. - Master is all-in-one and slave has an external database
    """
    ERROR_CODE = 'incompatible_cluster_architecture'


class ExpiredCloudifyLicense(CloudifyClientError):
    """
    Raised when the Cloudify license on the Manager has expired
    """
    ERROR_CODE = 'expired_cloudify_license'


class MissingCloudifyLicense(CloudifyClientError):
    """
    Raised when there is no Cloudify license on the Manager
    """
    ERROR_CODE = 'missing_cloudify_license'


class InvalidFilterRule(CloudifyClientError):
    """
    Raised when one of the provided filter rules is invalid
    """
    ERROR_CODE = 'invalid_filter_rule'

    def __init__(self, message, server_traceback=None,
                 status_code=-1, error_code=None, response=None):
        super(InvalidFilterRule, self).__init__(
            message, server_traceback, status_code, error_code, response)
        self.err_filter_rule = response.json().get('err_filter_rule')
        self.err_reason = response.json().get('err_reason')


class DeploymentParentNotFound(CloudifyClientError):
    """
    Raised when  deployment reference parent that does not exist using
    labels in blueprint dsl
    """
    ERROR_CODE = 'deployment_parent_not_found_error'


class ForbiddenWhileCancelling(CloudifyClientError):
    ERROR_CODE = 'forbidden_while_cancelling'


ERROR_MAPPING = dict([
    (error.ERROR_CODE, error)
    for error in [
        DeploymentEnvironmentCreationInProgressError,
        DeploymentEnvironmentCreationPendingError,
        IllegalExecutionParametersError,
        NoSuchIncludeFieldError,
        MissingRequiredDeploymentInputError,
        UnknownDeploymentInputError,
        UnknownDeploymentSecretError,
        UnsupportedDeploymentGetSecretError,
        FunctionsEvaluationError,
        UnknownModificationStageError,
        ExistingStartedDeploymentModificationError,
        DeploymentModificationAlreadyEndedError,
        UserUnauthorizedError,
        ForbiddenError,
        MaintenanceModeActiveError,
        MaintenanceModeActivatingError,
        NotModifiedError,
        InvalidExecutionUpdateStatus,
        PluginInUseError,
        BlueprintInUseError,
        PluginInstallationError,
        PluginInstallationTimeout,
        NotClusterMaster,
        RemovedFromCluster,
        DeploymentPluginNotFound,
        IncompatibleClusterArchitectureError,
        MissingCloudifyLicense,
        ExpiredCloudifyLicense,
        InvalidFilterRule,
        DeploymentParentNotFound,
        ForbiddenWhileCancelling,
    ]])
