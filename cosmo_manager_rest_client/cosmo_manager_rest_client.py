########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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

__author__ = 'ran'

import tempfile
import os
import sys
import time
import shutil
import tarfile
import json

from os.path import expanduser
from contextlib import contextmanager
from urllib2 import HTTPError, URLError
from httplib import HTTPException

from swagger.swagger import ApiClient
from swagger.BlueprintsApi import BlueprintsApi
from swagger.ExecutionsApi import ExecutionsApi
from swagger.DeploymentsApi import DeploymentsApi
from swagger.events_api import EventsApi
from swagger.nodes_api import NodesApi


class CosmoManagerRestClient(object):

    def __init__(self, server_ip, port=8100):
        server_url = 'http://{0}:{1}'.format(server_ip, port)
        api_client = ApiClient(apiServer=server_url, apiKey='')
        self._blueprints_api = BlueprintsApi(api_client)
        self._executions_api = ExecutionsApi(api_client)
        self._deployments_api = DeploymentsApi(api_client)
        self._nodes_api = NodesApi(api_client)
        self._events_api = EventsApi(api_client)

    def list_blueprints(self):
        with self._protected_call_to_server('listing blueprints'):
            return self._blueprints_api.list()

    def publish_blueprint(self, blueprint_path):
        tempdir = tempfile.mkdtemp()
        try:
            tar_path = self._tar_blueprint(blueprint_path, tempdir)
            application_file = os.path.basename(blueprint_path)

            with self._protected_call_to_server('publishing blueprint'):
                with open(tar_path, 'rb') as f:
                    blueprint_state = self._blueprints_api.upload(
                        f,
                        application_file_name=application_file)
                return blueprint_state
        finally:
            shutil.rmtree(tempdir)

    def delete_blueprint(self, blueprint_id):
        with self._protected_call_to_server('deleting blueprint'):
            return self._blueprints_api.delete(blueprint_id)

    def validate_blueprint(self, blueprint_id):
        with self._protected_call_to_server('validating blueprint'):
            return self._blueprints_api.validate(blueprint_id)

    def list_deployments(self):
        with self._protected_call_to_server('list deployments'):
            return self._deployments_api.list()

    def create_deployment(self, blueprint_id):
        with self._protected_call_to_server('creating new deployment'):
            body = {
                'blueprintId': blueprint_id
            }
            return self._deployments_api.createDeployment(body=body)

    @staticmethod
    def _create_events_query(execution_id, timestamp=None):
        execution_id_match = {"match": {"context.execution_id": execution_id}}
        if timestamp is not None:
            query = {
                "bool": {
                    "must": [
                        execution_id_match,
                        {"range": {"@timestamp": {"gt": timestamp}}}
                    ]
                }
            }
        else:
            query = execution_id_match
        return query

    def get_execution_events(self,
                             execution_id,
                             timestamp=None,
                             from_event=0,
                             batch_size=100):
        """
        Returns event for the provided execution id.

        Args:
            execution_id: Id of execution to get events for.
            timestamp: The timestamp to get events after
                (None for getting all available events).
            from_event: Index of first event to retrieve on pagination.
            batch_size: Maximum number of events to retrieve per call.

        Returns (3 values):
            Events list, total number of currently available events and
            last event timestamp.
        """
        body = {
            "from": from_event,
            "size": batch_size,
            "sort": [{"@timestamp": {"order": "asc"}}],
            "query": self._create_events_query(execution_id, timestamp)
        }
        result = self.get_events(body)
        events = map(lambda x: x['_source'], result.json()['hits']['hits'])
        total_events = result.json()['hits']['total']
        last_timestamp = events[-1]['@timestamp'] if len(events) > 0 else None
        return events, total_events, last_timestamp

    def execute_deployment(self, deployment_id, operation, events_handler=None,
                           timeout=900):
        end = time.time() + timeout

        with self._protected_call_to_server('executing deployment operation'):
            body = {
                'workflowId': operation
            }
            execution = self._deployments_api.execute(
                deployment_id=deployment_id,
                body=body)

            end_states = ('terminated', 'failed')
            timestamp = None
            from_event = 0

            def handle_events(execution_events):
                if len(execution_events) > 0:
                    events_handler(execution_events)
                return len(execution_events)

            while execution.status not in end_states:
                if end < time.time():
                    raise CosmoManagerRestCallError(
                        'execution of operation {0} for deployment {1} timed '
                        'out'.format(operation, deployment_id))
                time.sleep(3)

                execution = self._executions_api.getById(execution.id)

                if events_handler is not None:
                    events, total_events, timestamp = \
                        self.get_execution_events(execution.id,
                                                  timestamp=timestamp,
                                                  from_event=from_event)
                    handle_events(events)

            # get remaining events after execution is over
            while events_handler is not None:
                if end < time.time():
                    raise CosmoManagerRestCallError(
                        'execution of operation {0} for deployment {1} timed '
                        'out'.format(operation, deployment_id))
                time.sleep(1)
                events, total_events, timestamp = \
                    self.get_execution_events(execution.id,
                                              timestamp=timestamp,
                                              from_event=from_event)
                handle_events(events)
                if from_event >= total_events:
                    break

            if execution.status != 'terminated':
                raise CosmoManagerRestCallError('Execution of operation {0} '
                                                'for deployment {1} failed. '
                                                '[execution_id={2}, '
                                                'status_response={3}]'
                                                .format(
                                                    operation,
                                                    deployment_id,
                                                    execution.id,
                                                    execution.error))

    def get_events(self, query):
        """
        Returns events for the provided ElasticSearch query.

        Args:
            query: ElasticSearch query (dict), for example:
                {
                    "query": {
                        "range": {
                            "@timestamp": {
                                "gt": "2014-01-28T13:27:56.231Z",
                            }
                        }
                    }
                }

        Returns:
            A list of events in ElasticSearch's response format.
        """
        with self._protected_call_to_server('getting events'):
            return self._events_api.get_events(query)

    def list_workflows(self, deployment_id):
        """
        Returns a list of available workflows for the provided deployment id.

        Args:
            deployment_id: Deployment id to get available workflows for.

        Returns:
            Workflows list.
        """
        with self._protected_call_to_server('listing workflows'):
            return self._deployments_api.listWorkflows(deployment_id)

    def list_nodes(self):
        """
        List all nodes.
        """
        with self._protected_call_to_server('getting node'):
            return self._nodes_api.list()

    def list_deployment_nodes(self, deployment_id, get_reachable_state=False):
        """
        List nodes for the provided deployment_id
        """
        with self._protected_call_to_server('getting deployment nodes'):
            return self._deployments_api.listNodes(deployment_id,
                                                   get_reachable_state)

    def get_node_state(self, node_id, get_reachable_state=False,
                       get_runtime_state=True):
        """
        Gets node runtime/reachable state for the provided node_id.
        """
        with self._protected_call_to_server('getting node'):
            return self._nodes_api.get_state_by_id(node_id,
                                                   get_reachable_state,
                                                   get_runtime_state)

    def update_node_state(self, node_id, updated_properties):
        """Updates node runtime state for the provided node_id.
        Args:
            node_id: The node id.
            updated_properties: The node's updated runtime properties as dict
                where each key's value is a list of values [new, previous]
                in order to provide the storage implementation a way to
                perform the update with some kind of optimistic locking.
                For new keys list should contain a single item.
                For example: {
                    "new_key": ["value"],
                    "updated_key": ["new_value", "old_value"]
                }
        Returns:
            Updated node runtime properties.
            Example:
                { "id": "node...", "runtimeInfo" { ... } }
        """
        with self._protected_call_to_server('updating node runtime state'):
            return self._nodes_api.update_node_state(node_id,
                                                     updated_properties)

    @staticmethod
    def _tar_blueprint(blueprint_path, tempdir):
        blueprint_path = expanduser(blueprint_path)
        blueprint_name = os.path.basename(os.path.splitext(blueprint_path)[0])
        blueprint_directory = os.path.dirname(blueprint_path)
        if not blueprint_directory:
            #blueprint path only contains a file name from the local directory
            blueprint_directory = os.getcwd()
        tar_path = '{0}/{1}.tar.gz'.format(tempdir, blueprint_name)
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(blueprint_directory,
                    arcname=os.path.basename(blueprint_directory))
        return tar_path

    @staticmethod
    @contextmanager
    def _protected_call_to_server(action_name):
        try:
            yield
        except HTTPError, ex:
            server_message = None
            server_output = None
            if ex.fp:
                server_output = json.loads(ex.fp.read())
            else:
                try:
                    server_output = json.loads(ex.reason)
                except Exception:
                    pass
            if server_output and 'message' in server_output:
                server_message = server_output['message']
            trace = sys.exc_info()[2]
            raise CosmoManagerRestCallError(
                'Failed {0}: Error code - {1}; Message - "{2}"'
                .format(
                    action_name,
                    ex.code,
                    server_message if
                    server_message else ex.msg)), None, trace
        except URLError, ex:
            trace = sys.exc_info()[2]
            raise CosmoManagerRestCallError(
                'Failed {0}: Reason - {1}'
                .format(action_name, ex.reason)), None, trace
        except HTTPException, ex:
            trace = sys.exc_info()[2]
            raise CosmoManagerRestCallError(
                'Failed {0}: Error - {1}'
                .format(action_name, str(ex))), None, trace


class CosmoManagerRestCallError(Exception):
    pass
