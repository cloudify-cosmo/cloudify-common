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

import os

import requests


class BlueprintsApi(object):

    CONTENT_DISPOSITION_HEADER = 'content-disposition'

    def __init__(self, api_client):
        self.api_client = api_client

    def upload(self, tar_file_obj,
               application_file_name=None,
               blueprint_id=None):
        """Upload a new blueprint to Cloudify
        Args:
            tar_file_obj, File object of the tar gzipped
                blueprint directory (required)
            application_file_name, : File name of yaml containing
                the main blueprint. (optional)
            blueprint_id: Uploaded blueprint id (optional, plan name is used
                                                 if not provided)
        Returns: BlueprintState
        """

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

        if blueprint_id is not None:
            resource_path = '/blueprints/{0}'.format(blueprint_id)
            url = self.api_client.resource_url(resource_path)
            response = requests.put(url,
                                    params=query_params,
                                    data=file_gen())
        else:
            resource_path = '/blueprints'
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

        return self.api_client.deserialize(response.json(),
                                           'BlueprintState')

    def get_source(self, blueprint_id):
        resource_path = '/blueprints/{0}/source'.format(blueprint_id)
        url = self.api_client.resource_url(resource_path)
        response = requests.get(url)

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(),
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

        return self.api_client.deserialize(response.json(),
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

        self.api_client.raise_if_not(200, response, url)

        return self.api_client.deserialize(response.json(),
                                           'BlueprintState')

    def download(self, blueprint_id, output_file=None):
        """
        Downloads a previously uploaded blueprint from Cloudify's manager
        to a file and returns its file name.

        :param blueprint_id: Blueprint Id to download.
        :param output_file: An optional parameter for specifying the local
         target file name.
        :return: Downloaded blueprint file name.
        """
        resource_path = '/blueprints/{0}/archive'.format(blueprint_id)
        url = self.api_client.resource_url(resource_path)

        r = requests.get(url, stream=True)
        self.api_client.raise_if_not(requests.codes.ok, r, url)

        if not output_file:
            if self.CONTENT_DISPOSITION_HEADER not in r.headers:
                raise RuntimeError(
                    'Cannot determine attachment filename: {0} header not'
                    ' found in response headers'.format(
                        self.CONTENT_DISPOSITION_HEADER))
            output_file = r.headers[
                self.CONTENT_DISPOSITION_HEADER].split('filename=')[1]

        if os.path.exists(output_file):
            raise OSError("Output file '%s' already exists" % output_file)

        with open(output_file, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8096):
                if chunk:
                    f.write(chunk)
                    f.flush()

        return output_file
