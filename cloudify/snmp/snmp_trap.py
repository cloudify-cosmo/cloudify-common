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

import os
import time
import json
from calendar import timegm

from pysmi.reader import HttpReader
from pysmi.compiler import MibCompiler
from pysmi.parser.smi import SmiV2Parser
from pysmi.writer.pyfile import PyFileWriter
from pysmi.reader.localfile import FileReader
from pysmi.codegen.pysnmp import PySnmpCodeGen
from pysmi.searcher.pyfile import PyFileSearcher
from pysnmp.hlapi import (SnmpEngine,
                          ObjectType,
                          ContextData,
                          CommunityData,
                          ObjectIdentity,
                          NotificationType,
                          sendNotification,
                          UdpTransportTarget)

from cloudify.utils import setup_logger


# The name of our mib
CLOUDIFY_MIB = 'CLOUDIFY-MIB'

# The notification types
WORKFLOW_QUEUED = 'cloudifyWorkflowQueued'
WORKFLOW_FAILED = 'cloudifyWorkflowFailed'
WORKFLOW_STARTED = 'cloudifyWorkflowStarted'
WORKFLOW_SUCCEEDED = 'cloudifyWorkflowSucceeded'
WORKFLOW_CANCELLED = 'cloudifyWorkflowCancelled'

# The object types
ERROR = 'cloudifyErrorDetails'
TIMESTAMP = 'cloudifyTimeStamp'
TENANT_NAME = 'cloudifyTenantName'
EXECUTION_ID = 'cloudifyExecutionID'
DEPLOYMENT_ID = 'cloudifyDeploymentID'
WORKFLOW_NAME = 'cloudifyWorkflowName'
WORKFLOW_PARAMETERS = 'cloudifyWorkflowParameters'

NOTIFY_TYPE = 'trap'

notification_types = {
    'workflow_started': WORKFLOW_STARTED,
    'workflow_succeeded': WORKFLOW_SUCCEEDED,
    'workflow_failed': WORKFLOW_FAILED,
    'workflow_cancelled': WORKFLOW_CANCELLED,
    'workflow_queued': WORKFLOW_QUEUED
}


def send_snmp_trap(event_context, **kwargs):
    _compile_cloudify_mib()
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
        ObjectIdentity(CLOUDIFY_MIB, notification_types[event_type]))
    notification_type.addVarBinds(
        ObjectType(ObjectIdentity(CLOUDIFY_MIB, EXECUTION_ID), execution_id),
        ObjectType(ObjectIdentity(CLOUDIFY_MIB, WORKFLOW_NAME), workflow_id),
        ObjectType(ObjectIdentity(CLOUDIFY_MIB, TENANT_NAME), tenant_name),
        ObjectType(ObjectIdentity(CLOUDIFY_MIB, DEPLOYMENT_ID), deployment_id),
        ObjectType(ObjectIdentity(CLOUDIFY_MIB, TIMESTAMP), timestamp),
        ObjectType(ObjectIdentity(CLOUDIFY_MIB, WORKFLOW_PARAMETERS),
                   execution_parameters)
    )

    if event_type == 'workflow_failed':
        error = _get_error(event_context)
        notification_type.addVarBinds(ObjectType(ObjectIdentity(
            CLOUDIFY_MIB, ERROR), error))
    return notification_type


def _compile_cloudify_mib():
    mibs_dir = "pysnmp_mibs"
    if os.path.exists('{0}/{1}.py'.format(mibs_dir, CLOUDIFY_MIB)):
        return
    mib_compiler = MibCompiler(SmiV2Parser(),
                               PySnmpCodeGen(),
                               PyFileWriter(mibs_dir))
    cloudify_mib_dir = os.path.dirname(os.path.realpath(__file__))
    mib_compiler.addSources(FileReader(cloudify_mib_dir))
    mib_compiler.addSources(HttpReader('mibs.snmplabs.com', 80, '/asn1/@mib@'))
    mib_compiler.addSearchers(PyFileSearcher(mibs_dir))
    mib_compiler.compile(CLOUDIFY_MIB)


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
