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


class ExecutionsApi(object):

    def __init__(self, api_client):
        self.api_client = api_client

    def getById(self, execution_id):
        """Returns the execution state by its id.
        Args:
            execution_id, str: ID of the execution that needs to
                be fetched (required)
        Returns: Execution
        """

        resource_path = '/executions/{0}'.format(execution_id)
        url = self.api_client.resource_url(resource_path)
        response = requests.get(url)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(),
                                           'Execution')
