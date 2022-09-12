########
# Copyright (c) 2018 Cloudify Platform Ltd. All rights reserved
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
############


class DeploymentModificationState(object):
    STARTED = 'started'
    FINISHED = 'finished'
    ROLLEDBACK = 'rolledback'

    STATES = [STARTED, FINISHED, ROLLEDBACK]
    END_STATES = [FINISHED, ROLLEDBACK]


class SnapshotState(object):
    CREATED = 'created'
    FAILED = 'failed'
    CREATING = 'creating'
    UPLOADED = 'uploaded'

    STATES = [CREATED, FAILED, CREATING, UPLOADED]
    END_STATES = [CREATED, FAILED, UPLOADED]


class LogBundleState(object):
    CREATED = 'created'
    FAILED = 'failed'
    CREATING = 'creating'
    UPLOADED = 'uploaded'

    STATES = [CREATED, FAILED, CREATING, UPLOADED]
    END_STATES = [CREATED, FAILED, UPLOADED]


class ExecutionState(object):
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

    STATES = [TERMINATED, FAILED, CANCELLED, PENDING, STARTED,
              CANCELLING, FORCE_CANCELLING, KILL_CANCELLING, QUEUED, SCHEDULED]

    WAITING_STATES = [SCHEDULED, QUEUED]
    QUEUED_STATE = [QUEUED]
    END_STATES = [TERMINATED, FAILED, CANCELLED]

    # In progress execution states
    IN_PROGRESS_STATES = [
        PENDING,
        STARTED,
        CANCELLING,
        FORCE_CANCELLING,
        KILL_CANCELLING
    ]


# needs to be separate because python3 doesn't allow `if` in listcomps
# using names from class scope
ExecutionState.ACTIVE_STATES = [
    state for state in ExecutionState.STATES
    if state not in ExecutionState.END_STATES and
    state not in ExecutionState.WAITING_STATES
]


class DeploymentState(object):
    # Installation states
    ACTIVE = 'active'
    INACTIVE = 'inactive'

    # Latest execution states
    IN_PROGRESS = 'in_progress'
    COMPLETED = 'completed'
    FAILED = 'failed'
    CANCELLED = 'cancelled'

    # deployment states
    GOOD = 'good'
    REQUIRE_ATTENTION = 'requires_attention'

    EXECUTION_STATES_SUMMARY = {
        ExecutionState.TERMINATED: COMPLETED,
        ExecutionState.FAILED: FAILED,
        ExecutionState.CANCELLED: CANCELLED,
        ExecutionState.PENDING: IN_PROGRESS,
        ExecutionState.STARTED: IN_PROGRESS,
        ExecutionState.CANCELLING: IN_PROGRESS,
        ExecutionState.FORCE_CANCELLING: IN_PROGRESS,
        ExecutionState.KILL_CANCELLING: IN_PROGRESS,
        ExecutionState.QUEUED: IN_PROGRESS
    }


class VisibilityState(object):
    PRIVATE = 'private'
    TENANT = 'tenant'
    GLOBAL = 'global'

    STATES = [PRIVATE, TENANT, GLOBAL]


class AgentState(object):
    CREATING = 'creating'
    CREATED = 'created'
    CONFIGURING = 'configuring'
    CONFIGURED = 'configured'
    STARTING = 'starting'
    STARTED = 'started'
    STOPPING = 'stopping'
    STOPPED = 'stopped'
    DELETING = 'deleting'
    DELETED = 'deleted'
    RESTARTING = 'restarting'
    RESTARTED = 'restarted'
    RESTORED = 'restored'
    UPGRADING = 'upgrading'
    UPGRADED = 'upgraded'
    NONRESPONSIVE = 'nonresponsive'
    FAILED = 'failed'

    STATES = [CREATING, CREATED, CONFIGURING, CONFIGURED, STARTING, STARTED,
              STOPPING, STOPPED, DELETING, DELETED, RESTARTING, RESTARTED,
              UPGRADING, UPGRADED, FAILED, NONRESPONSIVE, RESTORED]


class PluginInstallationState(object):
    PENDING = 'pending-install'
    INSTALLING = 'installing'
    INSTALLED = 'installed'
    ERROR = 'error'
    PENDING_UNINSTALL = 'pending-uninstall'
    UNINSTALLING = 'uninstalling'
    UNINSTALLED = 'uninstalled'


class BlueprintUploadState(object):
    PENDING = 'pending'
    VALIDATING = 'validating'
    UPLOADING = 'uploading'
    EXTRACTING = 'extracting'
    PARSING = 'parsing'
    UPLOADED = 'uploaded'
    FAILED_UPLOADING = 'failed_uploading'
    FAILED_EXTRACTING = 'failed_extracting'
    FAILED_PARSING = 'failed_parsing'
    FAILED_EXTRACTING_TO_FILE_SERVER = 'failed_extracting_to_file_server'
    INVALID = 'invalid'

    FAILED_STATES = [FAILED_UPLOADING, FAILED_EXTRACTING, FAILED_PARSING,
                     FAILED_EXTRACTING_TO_FILE_SERVER, INVALID]
    END_STATES = FAILED_STATES + [UPLOADED]
    STATES = END_STATES + [PENDING, UPLOADING, EXTRACTING, PARSING]
