#!/usr/bin/env python
"""
WordAPI.py
Copyright 2012 Wordnik, Inc.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

import requests
from urllib2 import HTTPError


class DeploymentsApi(object):

    def __init__(self, apiClient):
        self.apiClient = apiClient

    def list(self, **kwargs):
        """Returns a list existing deployments.
        Args:
        Returns: list[Deployment]
        """

        allParams = []

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' "
                                "to method list" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/deployments'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if response is None:
            return None

        responseObject = self.apiClient.deserialize(response,
                                                    'list[Deployment]')
        return responseObject

    def listWorkflows(self, deployment_id, **kwargs):
        """Returns a list of the deployments workflows.

        Args:
            deployment_id : str

        Returns: Workflows
        """

        resourcePath = '/deployments/{0}/workflows'.format(deployment_id)
        method = 'GET'
        response = self.apiClient.callAPI(resourcePath, method, {}, None)

        if response is None:
            return None

        responseObject = self.apiClient.deserialize(response, 'Workflows')
        return responseObject

    def createDeployment(self, body, **kwargs):
        """Creates a new deployment
        Args:
            body, DeploymentRequest: Deployment blue print (required)
        Returns: Deployment
        """

        allParams = ['body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' "
                                "to method createDeployment" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/deployments'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if response is None:
            return None

        responseObject = self.apiClient.deserialize(response, 'Deployment')
        return responseObject

    def getById(self, deployment_id, **kwargs):
        """
        Args:
            deployment_id, :  (optional)
        Returns: BlueprintState
        """

        allParams = ['deployment_id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s' "
                                "to method getById" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/deployments/{deployment_id}'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'GET'

        queryParams = {}
        headerParams = {}

        if ('deployment_id' in params):
            replacement = str(self.apiClient.toPathValue(
                params['deployment_id']))
            resourcePath = resourcePath.replace('{' + 'deployment_id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if response is None:
            return None

        responseObject = self.apiClient.deserialize(response,
                                                    'BlueprintState')
        return responseObject

    # def list(self, deployment_id, **kwargs):
    #     """Returns a list of executions related to the provided blueprint.
    #     Args:
    #         deployment_id, :  (optional)
    #     Returns: list[Execution]
    #     """
    #
    #     allParams = ['deployment_id']
    #
    #     params = locals()
    #     for (key, val) in params['kwargs'].iteritems():
    #         if key not in allParams:
    #             raise TypeError("Got an unexpected keyword argument '%s'"
    #                             " to method list" % key)
    #         params[key] = val
    #     del params['kwargs']
    #
    #     resourcePath = '/deployments/{deployment_id}/executions'
    #     resourcePath = resourcePath.replace('{format}', 'json')
    #     method = 'GET'
    #
    #     queryParams = {}
    #     headerParams = {}
    #
    #     if ('deployment_id' in params):
    #         replacement = str(self.apiClient.toPathValue(
    #             params['deployment_id']))
    #         resourcePath = resourcePath.replace('{' + 'deployment_id' + '}',
    #                                             replacement)
    #     postData = (params['body'] if 'body' in params else None)
    #
    #     response = self.apiClient.callAPI(resourcePath, method, queryParams,
    #                                       postData, headerParams)
    #
    #     if response is None:
    #         return None
    #
    #    responseObject = self.apiClient.deserialize(response,
    #                                                'list[Execution]')
    #     return responseObject

    def execute(self, deployment_id, body, **kwargs):
        """Execute a workflow
        Args:
            deployment_id, :  (required)
            body, : Workflow execution request (required)
        Returns: Execution
        """

        allParams = ['deployment_id', 'body']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s'"
                                " to method execute" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/deployments/{deployment_id}/executions'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'POST'

        queryParams = {}
        headerParams = {}

        if ('deployment_id' in params):
            replacement = str(self.apiClient.toPathValue(
                params['deployment_id']))
            resourcePath = resourcePath.replace('{' + 'deployment_id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(resourcePath, method, queryParams,
                                          postData, headerParams)

        if response is None:
            return None

        responseObject = self.apiClient.deserialize(response, 'Execution')
        return responseObject

    def eventsHeaders(self, id, responseHeadersBuffers, **kwargs):
        """Get headers for events associated with the deployment
        Args:
            id, str: ID of deployment that needs to be fetched (required)
            responseHeadersBuffers, dict: a buffer for the response headers
        Returns:
        """

        allParams = ['id']

        params = locals()
        for (key, val) in params['kwargs'].iteritems():
            if key not in allParams:
                raise TypeError("Got an unexpected keyword argument '%s'"
                                " to method eventsHeaders" % key)
            params[key] = val
        del params['kwargs']

        resourcePath = '/deployments/{id}/events'
        resourcePath = resourcePath.replace('{format}', 'json')
        method = 'HEAD'

        queryParams = {}
        headerParams = {}

        if ('id' in params):
            replacement = str(self.apiClient.toPathValue(params['id']))
            resourcePath = resourcePath.replace('{' + 'id' + '}',
                                                replacement)
        postData = (params['body'] if 'body' in params else None)

        response = self.apiClient.callAPI(
            resourcePath, method, queryParams, postData, headerParams,
            responseHeadersBuffers=responseHeadersBuffers)

    def listNodes(self, deployment_id, get_reachable_state=False):
        """Returns a list of the deployments workflows.

        Args:
            deployment_id : str
            get_reachable_state: bool (default: False)

        Returns: DeploymentNodes
        """

        resourcePath = '/deployments/{0}/nodes'.format(deployment_id)
        queryParams = {
            'reachable': str(get_reachable_state).lower()
        }

        url = '{0}{1}'.format(self.apiClient.apiServer, resourcePath)
        response = requests.get(url, params=queryParams)

        if response.status_code != 200:
            raise HTTPError(url, response.status_code,
                            response.content, response.headers, None)

        responseObject = self.apiClient.deserialize(response.json(),
                                                    'DeploymentNodes')
        return responseObject
