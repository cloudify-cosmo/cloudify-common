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

__author__ = 'ran'


import json
import requests


class ProviderContextApi(object):

    def __init__(self, client):
        self.api_client = client

    def get_context(self):
        resource_path = '/provider/context'
        url = self.api_client.resource_url(resource_path)

        response = requests.get(url)

        self.api_client.raise_if_not(200, response, url)

        return response.json()['context']

    def post_context(self, provider_context):
        resource_path = '/provider/context'

        url = self.api_client.resource_url(resource_path)

        response = requests.post(url,
                                 headers={'Content-Type': 'application/json'},
                                 data=json.dumps({
                                     'context': provider_context
                                 }))

        self.api_client.raise_if_not(201, response, url)

        return self.api_client.deserialize(response.json(),
                                           'ProviderContextPostStatus')
