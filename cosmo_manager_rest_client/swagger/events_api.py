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


class EventsApi(object):

    def __init__(self, client):
        self.server_url = client.apiServer

    def get_events(self, query):
        resource_url = '{0}/events'.format(self.server_url)
        headers = {'Content-type': 'application/json'}
        return requests.get(resource_url, headers=headers,
                            data=json.dumps(query))
