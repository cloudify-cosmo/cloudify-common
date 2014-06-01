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

from cloudify_rest_client.blueprints import BlueprintsClient
from cloudify_rest_client.deployments import DeploymentsClient
from cloudify_rest_client.executions import ExecutionsClient
from cloudify_rest_client.node_instances import NodeInstancesClient
from cloudify_rest_client.exceptions import *


class HTTPClient(object):

    def __init__(self, host, port=80):
        self.port = port
        self.host = host
        self.url = 'http://{0}:{1}'.format(host, port)

    @staticmethod
    def _raise_client_error(response):
        try:
            message = response.json()['message']
        except Exception:
            message = response.content
        raise CloudifyClientError(message)

    def do_request(self,
                   requests_method,
                   uri,
                   data=None,
                   params=None,
                   expected_status_code=200):
        request_url = '{0}{1}'.format(self.url, uri)
        response = requests_method(request_url,
                                   data=data,
                                   params=params,
                                   headers={'Content-type': 'application/json'})
        if response.status_code != expected_status_code:
            self._raise_client_error(response)
        return response.json()

    def get(self, uri, data=None, params=None, expected_status_code=200):
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

    def __init__(self, host, port=80):
        self._client = HTTPClient(host, port)
        self.blueprints = BlueprintsClient(self._client)
        self.deployments = DeploymentsClient(self._client)
        self.executions = ExecutionsClient(self._client)
        self.node_instances = NodeInstancesClient(self._client)

