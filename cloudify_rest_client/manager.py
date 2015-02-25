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


class ManagerClient(object):

    def __init__(self, api):
        self.api = api

    def get_status(self):
        """
        :return: Cloudify's management machine status.
        """
        response = self.api.get('/status')
        return response

    def get_version(self):
        """
        :return: Cloudify's management machine version information.
        """
        response = self.api.get('/version')
        return response

    def get_context(self, _include=None):
        """
        Gets the context which was stored on management machine bootstrap.
        The context contains Cloudify specific information and Cloud provider
        specific information.

        :param _include: List of fields to include in response.
        :return: Context stored in manager.
        """
        response = self.api.get('/provider/context', _include=_include)
        return response

    def create_context(self, name, context, force=False):
        """
        Creates context in Cloudify's management machine.
        This method is usually invoked right after management machine
        bootstrap with relevant Cloudify and cloud provider
        context information.

        :param name: Cloud provider name.
        :param context: Context as dict.
        :param force: true to override any existing context. if no context
        exists then it will simply be created as expected.
        :return: Create context result.
        """
        data = {'name': name, 'context': context}
        response = self.api.post('/provider/context?force={0}'.format(force),
                                 data,
                                 expected_status_code=201)
        return response
