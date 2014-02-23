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
        Returns: BlueprintState
        """

        resource_path = '/deployments/{0}'.format(deployment_id)
        url = self.api_client.resource_url(resource_path)
        response = requests.get(url)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(),
                                           'BlueprintState')

    def execute(self, deployment_id, body):
        """Execute a workflow
        Args:
            deployment_id, :  (required)
            body, : Workflow execution request (required)
        Returns: Execution
        """

        resource_path = '/deployments/{0}/executions'.format(deployment_id)
        url = self.api_client.resource_url(resource_path)
        response = requests.post(url,
                                 headers={'Content-type': 'application/json'},
                                 data=json.dumps(body))

        self.api_client.raise_if_not(201, response, url)

        return self.api_client.deserialize(response.json(),
                                           'Execution')

    def listExecutions(self, deployment_id):
        """Returns deployment executions
        Args:
            deployment_id, :  (required)
        Returns: Execution
        """

        resource_path = '/deployments/{0}/executions'.format(deployment_id)
        url = self.api_client.resource_url(resource_path)
        response = requests.get(url)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(),
                                           'list[Execution]')

    def eventsHeaders(self, id, responseHeadersBuffers):
        """Get headers for events associated with the deployment
        Args:
            id, str: ID of deployment that needs to be fetched (required)
            responseHeadersBuffers, dict: a buffer for the response headers
        Returns:
        """

        resource_path = '/deployments/{0}/events'.format(id)
        url = self.api_client.resource_url(resource_path)
        response = requests.head(url)

        self.api_client.raise_if_not(200, response, url)

        responseHeadersBuffers.update(response.headers)

    def readEvents(self, id, responseHeadersBuffers=None, from_param=0,
                   count_param=500):
        """Returns deployments events.
        Args:
            id, str: ID of deployment that needs to be fetched (required)
            from_param, int: Index of the first request event. (optional)
            count_param, int: Maximum number of events to read. (optional)
            responseHeadersBuffers, dict: a buffer for the response
                headers (optional)
        Returns: DeploymentEvents
        """

        resource_path = '/deployments/{0}/events'.format(id)
        url = self.api_client.resource_url(resource_path)

        query_params = {
            'from': str(from_param),
            'count': str(count_param)
        }

        response = requests.get(url,
                                params=query_params)

        self.api_client.raise_if_not(200, response, url)

        if responseHeadersBuffers is not None:
            responseHeadersBuffers.update(response.headers)

        response_json = response.json()

        events_json_str = map(lambda x: json.dumps(x),
                              response_json['events'])
        response_json['events'] = events_json_str

        return self.api_client.deserialize(response_json,
                                           'DeploymentEvents')

    def listNodes(self, deployment_id, get_reachable_state=False):
        """Returns a list of the deployments workflows.

        Args:
            deployment_id : str
            get_reachable_state: bool (default: False)

        Returns: DeploymentNodes
        """

        resource_path = '/deployments/{0}/nodes'.format(deployment_id)
        url = self.api_client.resource_url(resource_path)
        query_params = {
            'reachable': str(get_reachable_state).lower()
        }

        response = requests.get(url,
                                params=query_params)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(),
                                           'DeploymentNodes')
