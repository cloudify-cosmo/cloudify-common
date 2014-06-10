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
from swagger.search_api import SearchApi
from swagger.nodes_api import NodesApi
from swagger.status_api import StatusApi
from swagger.provider_context_api import ProviderContextApi


class CosmoManagerRestClient(object):

    def __init__(self, server_ip, port=80):
        server_url = 'http://{0}:{1}'.format(server_ip, port)
        api_client = ApiClient(apiServer=server_url, apiKey='')
        self._blueprints_api = BlueprintsApi(api_client)
        self._executions_api = ExecutionsApi(api_client)
        self._deployments_api = DeploymentsApi(api_client)
        self._nodes_api = NodesApi(api_client)
        self._search_api = SearchApi(api_client)
        self._status_api = StatusApi(api_client)
        self._provider_context_api = ProviderContextApi(api_client)

    def status(self):
        with self._protected_call_to_server('status'):
            return self._status_api.status()

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

    def get_blueprint(self, blueprint_id):
        with self._protected_call_to_server('getting blueprint'):
            return self._blueprints_api.getById(blueprint_id)

    def get_blueprint_source(self, blueprint_id):
        with self._protected_call_to_server('getting blueprint source'):
            return self._blueprints_api.get_source(blueprint_id)

    def delete_blueprint(self, blueprint_id):
        with self._protected_call_to_server('deleting blueprint'):
            return self._blueprints_api.delete(blueprint_id)

    def validate_blueprint(self, blueprint_id):
        with self._protected_call_to_server('validating blueprint'):
            return self._blueprints_api.validate(blueprint_id)

    def download_blueprint(self, blueprint_id, output_file=None):
        """
        Downloads a previously uploaded blueprint from Cloudify's manager.

        :param blueprint_id: The Id of the blueprint to be downloaded.
        :param output_file: The file path of the downloaded blueprint file
         (optional)
        :return: The file path of the downloaded blueprint.
        """
        with self._protected_call_to_server('downloading blueprint'):
            return self._blueprints_api.download(blueprint_id, output_file)

    def list_deployments(self):
        with self._protected_call_to_server('list deployments'):
            return self._deployments_api.list()

    def get_deployment(self, deployment_id):
        with self._protected_call_to_server('get deployment'):
            return self._deployments_api.getById(deployment_id)

    def delete_deployment(self, deployment_id, ignore_live_nodes=False):
        with self._protected_call_to_server('deleting deployment'):
            return self._deployments_api.delete(deployment_id,
                                                ignore_live_nodes)

    def list_deployment_executions(self, deployment_id):
        with self._protected_call_to_server('list executions'):
            return self._deployments_api.listExecutions(deployment_id)

    def create_deployment(self, blueprint_id, deployment_id):
        with self._protected_call_to_server('creating new deployment'):
            body = {
                'blueprintId': blueprint_id
            }
            return self._deployments_api.createDeployment(
                deployment_id=deployment_id,
                body=body)

    def update_execution_status(self, execution_id, status, error=None):
        """
        Update an execution's status by its id.
        """
        with self._protected_call_to_server('update execution status'):
            return self._executions_api.update_execution_status(
                execution_id, status, error)

    def cancel_execution(self, execution_id):
        """
        Cancel an execution by its id.
        """
        with self._protected_call_to_server('cancel execution'):
            return self._executions_api.cancel(execution_id)

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

    def list_deployment_nodes(self, deployment_id, get_state=False):
        """
        List nodes for the provided deployment_id
        """
        with self._protected_call_to_server('getting deployment nodes'):
            return self._deployments_api.listNodes(deployment_id, get_state)

    def get_node_instance(self, node_id,
                          get_state_and_runtime_properties=True):
        """
        Gets node runtime/state for the provided node_id.
        """
        with self._protected_call_to_server('getting node instance'):
            return self._nodes_api.get_node_instance(
                node_id,
                get_state_and_runtime_properties)

    def update_node_instance(self, node_id, state_version,
                             runtime_properties=None, state=None):
        """Updates node instance for the provided node_id.
           Will not override existing values if None is passed (e.g.
           it's possible to change updated_properties without modifying state)
        Args:
            node_id: The node id.
            state_version: The node's state's version as int
            runtime_properties: The node's updated runtime properties as dict
            state: The node's updated state
        Returns:
            Updated node.
            Example:
                { "id": "node...", "runtimeInfo" { ... }, ... }
        """
        with self._protected_call_to_server('updating node instance'):
            return self._nodes_api.update_node_instance(
                node_id, state_version, runtime_properties, state)

    def post_provider_context(self, name, context):
        """
        Sets the provider context on at the manager server.
        Args:
            name: The name of the provider
            provider_context: The context to store
        """
        with self._protected_call_to_server('posting provider context'):
            return self._provider_context_api.post_context(name, context)

    def get_provider_context(self):
        """
        Gets the provider context from the manager server.
        """
        with self._protected_call_to_server('getting provider context'):
            return self._provider_context_api.get_context()

    @staticmethod
    def _tar_blueprint(blueprint_path, tempdir):
        blueprint_path = expanduser(blueprint_path)
        blueprint_name = os.path.basename(os.path.splitext(blueprint_path)[0])
        blueprint_directory = os.path.dirname(blueprint_path)
        if not blueprint_directory:
            # blueprint path only contains a file name from the local directory
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
            raise CosmoManagerRestCallHTTPError(
                ex.code,
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
        except (HTTPException, IOError), ex:
            trace = sys.exc_info()[2]
            raise CosmoManagerRestCallError(
                'Failed {0}: Error - {1}'
                .format(action_name, str(ex))), None, trace


class CosmoManagerRestCallError(Exception):
    pass


class CosmoManagerRestCallHTTPError(CosmoManagerRestCallError):

    def __init__(self, status_code, *args, **kwargs):
        super(CosmoManagerRestCallHTTPError, self).__init__(self,
                                                            *args,
                                                            **kwargs)
        self.status_code = status_code


class CosmoManagerRestCallTimeoutError(Exception):

    def __init__(self, execution_id, *args, **kwargs):
        Exception.__init__(self, *args, **kwargs)
        self.execution_id = execution_id
