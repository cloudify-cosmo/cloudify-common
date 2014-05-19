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

import requests
import json


class DeploymentsApi(object):

    def __init__(self, api_client):
        self.api_client = api_client

    def list(self):
        """Returns a list existing deployments.
        Args:
        Returns: list[Deployment]
        """

        resource_path = '/deployments'
        url = self.api_client.resource_url(resource_path)
        response = requests.get(url)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(),
                                           'list[Deployment]')

    def listWorkflows(self, deployment_id):
        """Returns a list of the deployments workflows.

        Args:
            deployment_id : str

        Returns: Workflows
        """

        resource_path = '/deployments/{0}/workflows'.format(deployment_id)
        url = self.api_client.resource_url(resource_path)
        response = requests.get(url)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(),
                                           'Workflows')

    def createDeployment(self, body, deployment_id):
        """Creates a new deployment
        Args:
            body, DeploymentRequest: Deployment blue print (required)
        Returns: Deployment
        """

        resource_path = '/deployments/{0}'.format(deployment_id)
        url = self.api_client.resource_url(resource_path)
        response = requests.put(url,
                                headers={'Content-type': 'application/json'},
                                data=json.dumps(body))

        self.api_client.raise_if_not(201, response, url)

        return self.api_client.deserialize(response.json(),
                                           'Deployment')

    def getById(self, deployment_id):
        """
        Args:
            deployment_id, :  (optional)
        Returns: Deployment
        """

        resource_path = '/deployments/{0}'.format(deployment_id)
        url = self.api_client.resource_url(resource_path)
        response = requests.get(url)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(),
                                           'Deployment')

    def delete(self, deployment_id, ignore_live_nodes=False):
        """Deletes a given deployment.

        Args:
            deployment_id: str
            ignore_live_nodes: bool

        Returns: Deployment
        """

        resource_path = '/deployments/{0}'.format(deployment_id)
        url = self.api_client.resource_url(resource_path)
        query_params = {'ignore_live_nodes': str(ignore_live_nodes).lower()}

        response = requests.delete(url, params=query_params)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(), 'Deployment')

    def execute(self, deployment_id, body, force=False):
        """Execute a workflow
        Args:
            deployment_id, :  (required)
            body, : Workflow execution request (required),
            force : Should workflow be executed even though there is a running
                    workflow under the same deployment (optional,
                                                        default: false)
        Returns: Execution
        """

        resource_path = '/deployments/{0}/executions'.format(deployment_id)

        query_params = {
            'force': str(force).lower()
        }

        url = self.api_client.resource_url(resource_path)
        response = requests.post(url,
                                 headers={'Content-type': 'application/json'},
                                 data=json.dumps(body),
                                 params=query_params)

        self.api_client.raise_if_not(201, response, url)

        return self.api_client.deserialize(response.json(),
                                           'Execution')

    def listExecutions(self, deployment_id, get_executions_statuses=False):
        """Returns deployment executions
        Args:
            deployment_id, :  (required)
            get_execution_status: (optional)
        Returns: Execution
        """

        resource_path = '/deployments/{0}/executions'.format(deployment_id)
        url = self.api_client.resource_url(resource_path)

        query_params = {
            'statuses': str(get_executions_statuses).lower()
        }

        response = requests.get(url, params=query_params)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(),
                                           'list[Execution]')

    def listNodes(self, deployment_id, get_state=False):
        """Returns a list of the deployments workflows.

        Args:
            deployment_id : str
            get_state: bool (default: False)

        Returns: DeploymentNodes
        """

        resource_path = '/deployments/{0}/nodes'.format(deployment_id)
        url = self.api_client.resource_url(resource_path)
        query_params = {
            'state': str(get_state).lower()
        }

        response = requests.get(url,
                                params=query_params)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(),
                                           'DeploymentNodes')
