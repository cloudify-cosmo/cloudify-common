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

import json
import requests
import logging
from itsdangerous import base64_encode

from cloudify_rest_client import exceptions
from cloudify_rest_client.blueprints import BlueprintsClient
from cloudify_rest_client.deployments import DeploymentsClient
from cloudify_rest_client.executions import ExecutionsClient
from cloudify_rest_client.nodes import NodesClient
from cloudify_rest_client.node_instances import NodeInstancesClient
from cloudify_rest_client.events import EventsClient
from cloudify_rest_client.manager import ManagerClient
from cloudify_rest_client.search import SearchClient
from cloudify_rest_client.evaluate import EvaluateClient
from cloudify_rest_client.deployment_modifications import (
    DeploymentModificationsClient)


class HTTPClient(object):

    def __init__(self, host, port=80, protocol='http', user=None, password=None):
        self.port = port
        self.host = host
        self.url = '{0}://{1}:{2}'.format(protocol, host, port)
        if user:
            credentials = '{0}:{1}'.format(user, password)
            self.encoded_credentials = base64_encode(credentials)
        self.logger = logging.getLogger('cloudify.rest_client.http')


    @staticmethod
    def _raise_client_error(response, url=None):
        try:
            result = response.json()
        except Exception:
            message = response.content
            if url:
                message = '{0} [{1}]'.format(message, url)
            error_msg = '{0}: {1}'.format(response.status_code, message)
            raise exceptions.CloudifyClientError(
                error_msg,
                status_code=response.status_code)
        message = result['message']
        code = result['error_code']
        server_traceback = result['server_traceback']
        error = exceptions.ERROR_MAPPING.get(code,
                                             exceptions.CloudifyClientError)
        raise error(message, server_traceback,
                    response.status_code, error_code=code)

    def verify_response_status(self, response, expected_code=200):
        if response.status_code != expected_code:
            self._raise_client_error(response)

    def _do_request(self, requests_method, request_url, body, params, headers,
                    expected_status_code):
        response = requests_method(request_url,
                                   data=body,
                                   params=params,
                                   headers=headers)
        if self.logger.isEnabledFor(logging.DEBUG):
            for hdr, hdr_content in response.request.headers.iteritems():
                self.logger.debug('request header:  %s: %s'
                                  % (hdr, hdr_content))
            self.logger.debug('reply:  "%s %s" %s'
                              % (response.status_code,
                                 response.reason, response.content))
            for hdr, hdr_content in response.headers.iteritems():
                self.logger.debug('response header:  %s: %s'
                                  % (hdr, hdr_content))

        if response.status_code != expected_status_code:
            self._raise_client_error(response, request_url)
        return response.json()

    def do_request(self,
                   requests_method,
                   uri,
                   data=None,
                   params=None,
                   headers=None,
                   expected_status_code=200):
        request_url = '{0}{1}'.format(self.url, uri)
        # build headers
        if headers is None:
            headers = {'Content-type': 'application/json'}

        if self.encoded_credentials:
            headers['Authorization'] = self.encoded_credentials

        body = json.dumps(data) if data is not None else None
        if self.logger.isEnabledFor(logging.DEBUG):
            print_content = body if body is not None else ''
            self.logger.debug('Sending request: "%s %s" %s'
                              % (requests_method.func_name.upper(),
                                 request_url, print_content))
        return self._do_request(requests_method, request_url, body,
                                params, headers, expected_status_code)

    def get(self, uri, data=None, params=None, _include=None, headers=None,
            expected_status_code=200):
        if _include:
            fields = ','.join(_include)
            if not params:
                params = {}
            params['_include'] = fields
        return self.do_request(requests.get,
                               uri,
                               data=data,
                               params=params,
                               headers=headers,
                               expected_status_code=expected_status_code)

    def put(self, uri, data=None, params=None, headers=None, expected_status_code=200):
        return self.do_request(requests.put,
                               uri,
                               data=data,
                               params=params,
                               headers=headers,
                               expected_status_code=expected_status_code)

    def patch(self, uri, data=None, params=None, headers=None, expected_status_code=200):
        return self.do_request(requests.patch,
                               uri,
                               data=data,
                               params=params,
                               headers=headers,
                               expected_status_code=expected_status_code)

    def post(self, uri, data=None, params=None, headers=None, expected_status_code=200):
        return self.do_request(requests.post,
                               uri,
                               data=data,
                               params=params,
                               headers=headers,
                               expected_status_code=expected_status_code)

    def delete(self, uri, data=None, params=None, headers=None, expected_status_code=200):
        return self.do_request(requests.delete,
                               uri,
                               data=data,
                               params=params,
                               headers=headers,
                               expected_status_code=expected_status_code)


class CloudifyClient(object):
    """Cloudify's management client."""

    def __init__(self, host='localhost', port=80, protocol='http', user=None, password=None):
        """
        Creates a Cloudify client with the provided host and optional port.

        :param host: Host of Cloudify's management machine.
        :param port: Port of REST API service on management machine.
        :return: Cloudify client instance.
        """
        self._client = HTTPClient(host, port, protocol, user, password)
        self.blueprints = BlueprintsClient(self._client)
        self.deployments = DeploymentsClient(self._client)
        self.executions = ExecutionsClient(self._client)
        self.nodes = NodesClient(self._client)
        self.node_instances = NodeInstancesClient(self._client)
        self.manager = ManagerClient(self._client)
        self.events = EventsClient(self._client)
        self.search = SearchClient(self._client)
        self.evaluate = EvaluateClient(self._client)
        self.deployment_modifications = DeploymentModificationsClient(
            self._client)
