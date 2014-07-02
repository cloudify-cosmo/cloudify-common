########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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

__author__ = 'idanmo'

import requests
import json

from cloudify_rest_client.blueprints import BlueprintsClient
from cloudify_rest_client.deployments import DeploymentsClient
from cloudify_rest_client.executions import ExecutionsClient
from cloudify_rest_client.nodes import NodesClient
from cloudify_rest_client.node_instances import NodeInstancesClient
from cloudify_rest_client.events import EventsClient
from cloudify_rest_client.manager import ManagerClient
from cloudify_rest_client.search import SearchClient
from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify_rest_client.exceptions import CreateDeploymentInProgressError
from cloudify_rest_client.exceptions import IllegalExecutionParametersError
from cloudify_rest_client.exceptions import NoSuchIncludeFieldError


class HTTPClient(object):

    def __init__(self, host, port=80):
        self.port = port
        self.host = host
        self.url = 'http://{0}:{1}'.format(host, port)

    @staticmethod
    def _raise_client_error(response, url=None):
        try:
            result = response.json()
        except Exception:
            message = response.content
            if url:
                message = '{0} [{1}]'.format(message, url)
            error_msg = '{0}: {1}'.format(response.status_code, message)
            raise CloudifyClientError(error_msg, response.status_code)
        message = result['message']
        code = result['error_code']
        if code == CreateDeploymentInProgressError.ERROR_CODE:
            raise CreateDeploymentInProgressError(message,
                                                  response.status_code)
        if code == IllegalExecutionParametersError.ERROR_CODE:
            raise IllegalExecutionParametersError(message,
                                                  response.status_code)
        if code == NoSuchIncludeFieldError.ERROR_CODE:
            raise NoSuchIncludeFieldError(message, response.status_code)

        raise CloudifyClientError(message, response.status_code)

    def verify_response_status(self, response, expected_code=200):
        if response.status_code != expected_code:
            self._raise_client_error(response)

    def do_request(self,
                   requests_method,
                   uri,
                   data=None,
                   params=None,
                   expected_status_code=200):
        request_url = '{0}{1}'.format(self.url, uri)
        body = json.dumps(data) if data is not None else None

        response = requests_method(request_url,
                                   data=body,
                                   params=params,
                                   headers={
                                       'Content-type': 'application/json'
                                   })
        if response.status_code != expected_status_code:
            self._raise_client_error(response, request_url)
        return response.json()

    def get(self, uri, data=None, params=None,
            _include=None, expected_status_code=200):
        if _include:
            uri = '{}?_include={}'.format(uri, ','.join(_include))
        return self.do_request(requests.get,
                               uri,
                               data=data,
                               params=params,
                               expected_status_code=expected_status_code)

    def put(self, uri, data=None, params=None, expected_status_code=200):
        return self.do_request(requests.put,
                               uri,
                               data=data,
                               params=params,
                               expected_status_code=expected_status_code)

    def patch(self, uri, data=None, params=None, expected_status_code=200):
        return self.do_request(requests.patch,
                               uri,
                               data=data,
                               params=params,
                               expected_status_code=expected_status_code)

    def post(self, uri, data=None, params=None, expected_status_code=200):
        return self.do_request(requests.post,
                               uri,
                               data=data,
                               params=params,
                               expected_status_code=expected_status_code)

    def delete(self, uri, data=None, params=None, expected_status_code=200):
        return self.do_request(requests.delete,
                               uri,
                               data=data,
                               params=params,
                               expected_status_code=expected_status_code)


class CloudifyClient(object):
    """Cloudify's management client."""

    def __init__(self, host='localhost', port=80):
        """
        Creates a Cloudify client with the provided host and optional port.

        :param host: Host of Cloudify's management machine.
        :param port: Port of REST API service on management machine.
        :return: Cloudify client instance.
        """
        self._client = HTTPClient(host, port)
        self.blueprints = BlueprintsClient(self._client)
        self.deployments = DeploymentsClient(self._client)
        self.executions = ExecutionsClient(self._client)
        self.nodes = NodesClient(self._client)
        self.node_instances = NodeInstancesClient(self._client)
        self.manager = ManagerClient(self._client)
        self.events = EventsClient(self._client)
        self.search = SearchClient(self._client)
