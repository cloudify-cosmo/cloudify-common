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


class BlueprintsApi(object):

    def __init__(self, api_client):
        self.api_client = api_client

    def upload(self, tar_file_obj, application_file_name=None):
        """Upload a new blueprint to Cloudify
        Args:
            tar_file_obj, File object of the tar gzipped
                blueprint directory (required)
            application_file_name, : File name of yaml containing
                the main blueprint. (optional)
        Returns: BlueprintState
        """

        resource_path = '/blueprints'

        query_params = {}
        if application_file_name is not None:
            query_params['application_file_name'] = \
                self.api_client.toPathValue(application_file_name)

        def file_gen():
            buffer_size = 8192
            while True:
                read_bytes = tar_file_obj.read(buffer_size)
                yield read_bytes
                if len(read_bytes) < buffer_size:
                    return

        url = self.api_client.resource_url(resource_path)
        response = requests.post(url,
                                 params=query_params,
                                 data=file_gen())

        self.api_client.raise_if_not(201, response, url)

        return self.api_client.deserialize(response.json(),
                                           'BlueprintState')

    def list(self):
        """Returns a list a submitted blueprints.
        Args:
        Returns: list[BlueprintState]
        """

        resource_path = '/blueprints'
        url = self.api_client.resource_url(resource_path)
        response = requests.get(url)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(),
                                           'list[BlueprintState]')

    def getById(self, blueprint_id):
        """Returns a blueprint by its id.
        Args:
            blueprint_id, :  (optional)
        Returns: BlueprintState
        """

        resource_path = '/blueprints/{0}'.format(blueprint_id)
        url = self.api_client.resource_url(resource_path)
        response = requests.get(url)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response,
                                           'BlueprintState')

    def validate(self, blueprint_id):
        """Validates a given blueprint.

        Args:
            blueprint_id, :  (optional)

        Returns: BlueprintValidationStatus
        """

        resource_path = '/blueprints/{0}/validate'.format(blueprint_id)
        url = self.api_client.resource_url(resource_path)
        response = requests.get(url)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response,
                                           'BlueprintValidationStatus')

    def delete(self, blueprint_id):
        """Deletes a given blueprint.

        Args:
            blueprint_id: str

        Returns: BlueprintState
        """

        resource_path = '/blueprints/{0}'.format(blueprint_id)
        url = self.api_client.resource_url(resource_path)
        response = requests.delete(url)

        self.api_client.raise_if_not(201, response, url)

        return self.api_client.deserialize(response,
                                           'BlueprintState')
