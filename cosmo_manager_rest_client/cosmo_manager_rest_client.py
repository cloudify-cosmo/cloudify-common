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
from swagger.search_api import SearchApi
from swagger.nodes_api import NodesApi


class ExecutionEvents(object):

    def __init__(self, rest_client, execution_id, batch_size=100,
                 timeout=60, include_logs=False):
        self._rest_client = rest_client
        self._execution_id = execution_id
        self._batch_size = batch_size
        self._from_event = 0
        self._timeout = timeout
        self._include_logs = include_logs

    def fetch_and_process_events(self,
                                 get_remaining_events=False,
                                 events_handler=None):
        if events_handler is None:
            return
        events = self.fetch_events(get_remaining_events=get_remaining_events)
        events_handler(events)

    def fetch_events(self, get_remaining_events=False):
        if get_remaining_events:
            events = []
            timeout = time.time() + self._timeout
            while time.time() < timeout:
                result = self._fetch_events()
                if len(result) > 0:
                    events.extend(result)
                else:
                    return events
                time.sleep(1)
            raise CosmoManagerRestCallError('events/log fetching timed out')
        else:
            return self._fetch_events()

    def _fetch_events(self):
        events, total = self._rest_client.get_execution_events(
            self._execution_id,
            from_event=self._from_event,
            batch_size=self._batch_size,
            include_logs=self._include_logs)
        self._from_event += len(events)
        return events


class CosmoManagerRestClient(object):

    def __init__(self, server_ip, port=8100):
        server_url = 'http://{0}:{1}'.format(server_ip, port)
        api_client = ApiClient(apiServer=server_url, apiKey='')
        self._blueprints_api = BlueprintsApi(api_client)
        self._executions_api = ExecutionsApi(api_client)
        self._deployments_api = DeploymentsApi(api_client)
        self._nodes_api = NodesApi(api_client)
        self._events_api = EventsApi(api_client)
        self._search_api = SearchApi(api_client)

    def list_blueprints(self):
        with self._protected_call_to_server('listing blueprints'):
            return self._blueprints_api.list()

    def publish_blueprint(self, blueprint_path, blueprint_id=None):
        tempdir = tempfile.mkdtemp()
        try:
            tar_path = self._tar_blueprint(blueprint_path, tempdir)
            application_file = os.path.basename(blueprint_path)

            with self._protected_call_to_server('publishing blueprint'):
                with open(tar_path, 'rb') as f:
                    blueprint_state = self._blueprints_api.upload(
                        f,
                        application_file_name=application_file,
                        blueprint_id=blueprint_id)
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

    def list_deployment_executions(self, deployment_id,
                                   get_executions_statuses=False):
        with self._protected_call_to_server('list executions'):
            return self._deployments_api.listExecutions(
                deployment_id, get_executions_statuses)

    def create_deployment(self, blueprint_id, deployment_id):
        with self._protected_call_to_server('creating new deployment'):
            body = {
                'blueprintId': blueprint_id
            }
            return self._deployments_api.createDeployment(
                deployment_id=deployment_id,
                body=body)

    @staticmethod
    def _create_events_query(execution_id, include_logs):
        query = {
            "bool": {
                "must": [
                    {"match": {"context.execution_id": execution_id}},
                ]
            }
        }
        match_cloudify_event = {"match": {"type": "cloudify_event"}}
        if include_logs:
            match_cloudify_log = {"match": {"type": "cloudify_log"}}
            query['bool']['should'] = [
                match_cloudify_event, match_cloudify_log
            ]
        else:
            query['bool']['must'].append(match_cloudify_event)
        return query

    def get_all_execution_events(self, execution_id, include_logs=False):
        execution_events = ExecutionEvents(self,
                                           execution_id,
                                           include_logs=include_logs)
        return execution_events.fetch_events(get_remaining_events=True)

    def get_execution_events(self,
                             execution_id,
                             from_event=0,
                             batch_size=100,
                             include_logs=False):
        """
        Returns event for the provided execution id.

        Args:
            execution_id: Id of execution to get events for.
            from_event: Index of first event to retrieve on pagination.
            batch_size: Maximum number of events to retrieve per call.
            include_logs: Whether to also get logs.

        Returns (tuple):
            Events list and total number of currently available events.
        """
        body = {
            "from": from_event,
            "size": batch_size,
            "sort": [{"@timestamp": {"order": "asc"}}],
            "query": self._create_events_query(execution_id, include_logs)
        }
        result = self.get_events(body)
        events = map(lambda x: x['_source'], result['hits']['hits'])
        total_events = result['hits']['total']
        return events, total_events

    def execute_deployment(self, deployment_id, operation, events_handler=None,
                           timeout=900,
                           wait_for_execution=True):
        end = time.time() + timeout

        with self._protected_call_to_server('executing deployment operation'):
            body = {
                'workflowId': operation
            }
            execution = self._deployments_api.execute(
                deployment_id=deployment_id,
                body=body)

            if wait_for_execution:
                end_states = ('terminated', 'failed')
                execution_events = ExecutionEvents(self, execution.id)

                while execution.status not in end_states:
                    if end < time.time():
                        raise CosmoManagerRestCallTimeoutError(
                            execution.id,
                            'execution of operation {0} for deployment {1} '
                            'timed out'.format(operation, deployment_id))

                    time.sleep(3)

                    execution = self._executions_api.getById(execution.id)
                    execution_events.fetch_and_process_events(
                        events_handler=events_handler)

                # Process remaining events
                execution_events.fetch_and_process_events(
                    get_remaining_events=True,
                    events_handler=events_handler)

                error = None
                if execution.status != 'terminated':
                    error = execution.error
                return execution.id, error
            else:
                return execution.id, None

    def cancel_execution(self, execution_id):
        """
        Cancel an execution by its id.
        """
        with self._protected_call_to_server('cancel execution'):
            self._executions_api.cancel(execution_id)

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
            A json list of events in ElasticSearch's response format.
        """
        with self._protected_call_to_server('getting events'):
            return self._events_api.get_events(query).json()

    def run_search(self, query):
        """
        Returns results from the storage for the provided ElasticSearch query.

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
            json results from the storage in ElasticSearch's response format.
        """
        with self._protected_call_to_server('running search'):
            return self._search_api.run_search(query).json()

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

    def put_node_state(self, node_id, runtime_properties):
        """Puts node runtime state on the server
        Args:
            node_id: The node id.
            runtime_properties: The node's runtime properties as dict
        Returns:
            Inserted node.
            Example:
                { "id": "node...", "runtimeInfo" { ... } }
        """
        with self._protected_call_to_server('putting node runtime state'):
            return self._nodes_api.put_node_state(node_id,
                                                  runtime_properties)

    def update_node_state(self, node_id, updated_properties, state_version):
        """Updates node runtime state for the provided node_id.
        Args:
            node_id: The node id.
            updated_properties: The node's updated runtime properties as dict
            state_version: The node's state's version as int
        Returns:
            Updated node.
            Example:
                { "id": "node...", "runtimeInfo" { ... } }
        """
        with self._protected_call_to_server('updating node runtime state'):
            return self._nodes_api.update_node_state(node_id,
                                                     updated_properties,
                                                     state_version)

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


class CosmoManagerRestCallTimeoutError(Exception):

    def __init__(self, execution_id, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)
        self.execution_id = execution_id
