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

    SCHEDULED_STATE = [SCHEDULED]
    QUEUED_STATE = [QUEUED]
    END_STATES = [TERMINATED, FAILED, CANCELLED]
    ACTIVE_STATES = [state for state in STATES if state not in END_STATES and
                     state not in QUEUED_STATE not in SCHEDULED_STATE]


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
