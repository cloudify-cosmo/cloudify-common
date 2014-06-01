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


class Deployment(dict):

    def __init__(self, deployment):
        self.update(deployment)

    @property
    def id(self):
        return self['id']


class Workflows(dict):

    def __init__(self, workflows):
        self.update(workflows)
        self['workflows'] = [Workflow(item) for item in self['workflows']]

    @property
    def blueprint_id(self):
        return self['blueprintId']

    @property
    def deployment_id(self):
        return self['deploymentId']

    @property
    def workflows(self):
        return self['workflows']


class Workflow(dict):

    def __init__(self, workflow):
        self.update(workflow)

    @property
    def id(self):
        return self['name']


class DeploymentsClient(object):

    def __init__(self, api):
        self.api = api

    def list(self):
        response = self.api.get('/deployments')
        return [Deployment(item) for item in response]

    def list_workflows(self, deployment_id):
        assert deployment_id
        uri = '/deployments/{0}/workflows'.format(deployment_id)
        response = self.api.get(uri)
        return Workflows(response)