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


from cloudify_rest_client.executions import Execution


class Deployment(dict):
    """
    Cloudify deployment.
    """

    def __init__(self, deployment):
        self.update(deployment)
        if self['workflows']:
            # might be None, for example in response for delete deployment
            self['workflows'] = [Workflow(item) for item in self['workflows']]

    @property
    def id(self):
        """
        :return: The identifier of the deployment.
        """
        return self['id']

    @property
    def blueprint_id(self):
        """
        :return: The identifier of the blueprint this deployment belongs to.
        """
        return self['blueprint_id']

    @property
    def workflows(self):
        """
        :return: The workflows of this deployment
        """
        return self['workflows']


class Workflow(dict):

    def __init__(self, workflow):
        self.update(workflow)

    @property
    def id(self):
        """
        :return: The workflow's id
        """
        return self['name']

    @property
    def name(self):
        """
        :return: The workflow's name
        """
        return self['name']

    @property
    def parameters(self):
        """
        :return: The workflow's parameters
        """
        return self['parameters']


class DeploymentsClient(object):

    def __init__(self, api):
        self.api = api

    def list(self):
        """
        Returns a list of all deployments.

        :return: Deployments list.
        """
        response = self.api.get('/deployments')
        return [Deployment(item) for item in response]

    def get(self, deployment_id):
        """
        Returns a deployment by its id.

        :param deployment_id: Id of the deployment to get.
        :return: Deployment.
        """
        assert deployment_id
        response = self.api.get('/deployments/{0}'.format(deployment_id))
        return Deployment(response)

    def create(self, blueprint_id, deployment_id):
        """
        Creates a new deployment for the provided blueprint id and
        deployment id.

        :param blueprint_id: Blueprint id to create a deployment of.
        :param deployment_id: Deployment id of the new created deployment.
        :return: The created deployment.
        """
        assert blueprint_id
        assert deployment_id
        data = {
            'blueprint_id': blueprint_id
        }
        uri = '/deployments/{0}'.format(deployment_id)
        response = self.api.put(uri, data, expected_status_code=201)
        return Deployment(response)

    def delete(self, deployment_id, ignore_live_nodes=False):
        """
        Deletes the deployment whose id matches the provided deployment id.
        By default, deployment with live nodes deletion is not allowed and
        this behavior can be changed using the ignore_live_nodes argument.

        :param deployment_id: The deployment's to be deleted id.
        :param ignore_live_nodes: Determines whether to ignore live nodes.
        :return: The deleted deployment.
        """
        assert deployment_id
        params = {'ignore_live_nodes': 'true'} if ignore_live_nodes else None
        response = self.api.delete('/deployments/{0}'.format(deployment_id),
                                   params=params)
        return Deployment(response)

    def list_executions(self, deployment_id):
        """
        Returns a list of executions for the provided deployment's id.

        :param deployment_id: Deployment id to get a list of executions for.
        :return: List of executions.
        """
        assert deployment_id
        uri = '/deployments/{0}/executions'.format(deployment_id)
        response = self.api.get(uri)
        return [Execution(item) for item in response]

    def execute(self, deployment_id, workflow_id, parameters=None,
                allow_custom_parameters=False, force=False):
        """
        Executes a deployment's workflow whose id is provided.

        :param deployment_id: The deployment's id to execute a workflow for.
        :param workflow_id: The workflow to be executed id.
        :param parameters: Parameters for the workflow execution.
        :param allow_custom_parameters: Determines whether to allow
         parameters which weren't defined in the workflow parameters schema
         in the blueprint.
        :param force: Determines whether to force the execution of the workflow
         in a case where there's an already running execution for this
         deployment.
        :raises: IllegalExecutionParametersError
        :return: The created execution.
        """
        assert deployment_id
        assert workflow_id
        data = {
            'workflow_id': workflow_id,
            'parameters': parameters,
            'allow_custom_parameters': str(allow_custom_parameters).lower(),
            'force': str(force).lower()
        }
        uri = '/deployments/{0}/executions'.format(deployment_id)
        response = self.api.post(uri,
                                 data=data,
                                 expected_status_code=201)
        return Execution(response)
