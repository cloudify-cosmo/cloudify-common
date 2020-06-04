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
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

import time
import json
from calendar import timegm
from cloudify.utils import setup_logger

try:
    from pysnmp.hlapi import (SnmpEngine,
                              ContextData,
                              CommunityData,
                              ObjectIdentity,
                              NotificationType,
                              sendNotification,
                              UdpTransportTarget,
                              OctetString,
                              Counter64)
except ImportError:
    SNMP_AVAILABLE = False
else:
    SNMP_AVAILABLE = True


# The name of our mib
CLOUDIFY_MIB = 'CLOUDIFY-MIB'

# The notification types
WORKFLOW_QUEUED = '1.3.6.1.4.1.52312.1.0.1'
WORKFLOW_FAILED = '1.3.6.1.4.1.52312.1.0.5'
WORKFLOW_STARTED = '1.3.6.1.4.1.52312.1.0.2'
WORKFLOW_SUCCEEDED = '1.3.6.1.4.1.52312.1.0.3'
WORKFLOW_CANCELLED = '1.3.6.1.4.1.52312.1.0.4'

# The object types
ERROR = '1.3.6.1.4.1.52312.1.1.7'
TIMESTAMP = '1.3.6.1.4.1.52312.1.1.1'
TENANT_NAME = '1.3.6.1.4.1.52312.1.1.3'
EXECUTION_ID = '1.3.6.1.4.1.52312.1.1.5'
DEPLOYMENT_ID = '1.3.6.1.4.1.52312.1.1.2'
WORKFLOW_NAME = '1.3.6.1.4.1.52312.1.1.4'
WORKFLOW_PARAMETERS = '1.3.6.1.4.1.52312.1.1.6'

NOTIFY_TYPE = 'trap'

notification_types = {
    'workflow_started': WORKFLOW_STARTED,
    'workflow_succeeded': WORKFLOW_SUCCEEDED,
    'workflow_failed': WORKFLOW_FAILED,
    'workflow_cancelled': WORKFLOW_CANCELLED,
    'workflow_queued': WORKFLOW_QUEUED
}


def send_snmp_trap(event_context, **kwargs):
    if not SNMP_AVAILABLE:
        raise RuntimeError('pysnmp not available')
    notification_type = _create_notification_type(event_context)
    destination_address = kwargs['destination_address']
    destination_port = kwargs['destination_port']
    community_string = kwargs['community_string']

    error_indication, _, _, _ = next(
        sendNotification(
            SnmpEngine(),
            CommunityData(community_string, mpModel=1),
            UdpTransportTarget((destination_address, destination_port)),
            ContextData(),
            NOTIFY_TYPE,
            notification_type
        )
    )

    logger = setup_logger('cloudify.snmp.snmp_trap')
    if error_indication:
        logger.error(error_indication)

    logger.info('Sent SNMP trap of the event: {0} and the execution_id: {1}'
                .format(event_context['event_type'],
                        event_context['execution_id']))


def _create_notification_type(event_context):
    event_type = event_context['event_type']
    workflow_id = event_context['workflow_id']
    execution_id = event_context['execution_id']
    tenant_name = event_context['tenant_name']
    deployment_id = event_context['deployment_id'] or ''
    timestamp = _get_epoch_time(event_context)
    execution_parameters = _get_execution_parameters(event_context)

    notification_type = NotificationType(
        ObjectIdentity(notification_types[event_type]))

    notification_type.addVarBinds(
        (EXECUTION_ID, OctetString(execution_id)),
        (WORKFLOW_NAME, OctetString(workflow_id)),
        (TENANT_NAME, OctetString(tenant_name)),
        (DEPLOYMENT_ID, OctetString(deployment_id)),
        (TIMESTAMP, Counter64(timestamp)),
        (WORKFLOW_PARAMETERS, OctetString(execution_parameters))
    )

    if event_type == 'workflow_failed':
        error = _get_error(event_context)
        notification_type.addVarBinds((ERROR, OctetString(error)))
    return notification_type


def _get_epoch_time(event_context):
    timestamp = event_context['timestamp']
    utc_time = time.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    return timegm(utc_time)


def _get_execution_parameters(event_context):
    parameters = json.dumps(event_context.get('execution_parameters', {}))
    if len(parameters.encode('utf-8')) > 512:
        return {'parameters_too_long': 'The length of the workflow '
                                       'parameters json is too long (>512)'}

    return parameters


def _get_error(event_context):
    error = event_context['arguments']['error'].encode('utf-8')

    # If the length of the error message is too long, it will be truncated
    if len(error) > 512:
        # Get the error message instead of some of the stacktrace
        error = error.split('\n')[-2]
    return error
