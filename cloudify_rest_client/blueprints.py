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


import os
import tempfile
import shutil
import requests
import tarfile
import urllib

from os.path import expanduser


class Blueprint(dict):

    def __init__(self, blueprint):
        self.update(blueprint)

    @property
    def id(self):
        """
        :return: The identifier of the blueprint.
        """
        return self.get('id')

    @property
    def created_at(self):
        """
        :return: Timestamp of blueprint creation.
        """
        return self.get('created_at')

    @property
    def plan(self):
        """
        Gets the plan the blueprint represents: nodes, relationships etc...

        :return: The content of the blueprint.
        """
        return self.get('plan')


class BlueprintsClient(object):

    CONTENT_DISPOSITION_HEADER = 'content-disposition'

    def __init__(self, api):
        self.api = api

    @staticmethod
    def _tar_blueprint(blueprint_path, tempdir):
        blueprint_path = expanduser(blueprint_path)
        blueprint_name = os.path.basename(os.path.splitext(blueprint_path)[0])
        blueprint_directory = os.path.dirname(blueprint_path)
        if not blueprint_directory:
            # blueprint path only contains a file name from the local directory
            blueprint_directory = os.getcwd()
        tar_path = '{0}/{1}.tar.gz'.format(tempdir, blueprint_name)
        with tarfile.open(tar_path, "w:gz") as tar:
            tar.add(blueprint_directory,
                    arcname=os.path.basename(blueprint_directory))
        return tar_path

    def _upload(self, tar_file_obj,
                blueprint_id,
                application_file_name=None):
        query_params = {}
        if application_file_name is not None:
            query_params['application_file_name'] = \
                urllib.quote(application_file_name)

        def file_gen():
            buffer_size = 8192
            while True:
                read_bytes = tar_file_obj.read(buffer_size)
                yield read_bytes
                if len(read_bytes) < buffer_size:
                    return

        uri = '/blueprints/{0}'.format(blueprint_id)
        url = '{0}{1}'.format(self.api.url, uri)
        response = requests.put(url, params=query_params, data=file_gen())

        self.api.verify_response_status(response, 201)
        return response.json()

    def list(self, _include=None):
        """
        Returns a list of currently stored blueprints.

        :param _include: List of fields to include in response.
        :return: Blueprints list.
        """
        response = self.api.get('/blueprints', _include=_include)
        return [Blueprint(item) for item in response]

    def upload(self, blueprint_path, blueprint_id):
        """
        Uploads a blueprint to Cloudify's manager.

        :param blueprint_path: Main blueprint yaml file path.
        :param blueprint_id: Id of the uploaded blueprint.
        :return: Created blueprint.

        Blueprint path should point to the main yaml file of the blueprint
        to be uploaded. Its containing folder will be packed to an archive
        and get uploaded to the manager.
        Blueprint ID parameter is available for specifying the
        blueprint's unique Id.
        """
        tempdir = tempfile.mkdtemp()
        try:
            tar_path = self._tar_blueprint(blueprint_path, tempdir)
            application_file = os.path.basename(blueprint_path)

            with open(tar_path, 'rb') as f:
                blueprint = self._upload(
                    f,
                    blueprint_id=blueprint_id,
                    application_file_name=application_file)
            return Blueprint(blueprint)
        finally:
            shutil.rmtree(tempdir)

    def get(self, blueprint_id, _include=None):
        """
        Gets a blueprint by its id.

        :param blueprint_id: Blueprint's id to get.
        :param _include: List of fields to include in response.
        :return: The blueprint.
        """
        assert blueprint_id
        uri = '/blueprints/{0}'.format(blueprint_id)
        response = self.api.get(uri, _include=_include)
        return Blueprint(response)

    def delete(self, blueprint_id):
        """
        Deletes the blueprint whose id matches the provided blueprint id.

        :param blueprint_id: The id of the blueprint to be deleted.
        :return: Deleted blueprint.
        """
        assert blueprint_id
        response = self.api.delete('/blueprints/{0}'.format(blueprint_id))
        return Blueprint(response)

    def download(self, blueprint_id, output_file=None):
        """
        Downloads a previously uploaded blueprint from Cloudify's manager.

        :param blueprint_id: The Id of the blueprint to be downloaded.
        :param output_file: The file path of the downloaded blueprint file
         (optional)
        :return: The file path of the downloaded blueprint.
        """
        url = '{0}{1}'.format(self.api.url,
                              '/blueprints/{0}/archive'.format(blueprint_id))
        response = requests.get(url, stream=True)
        self.api.verify_response_status(response, 200)

        if not output_file:
            if self.CONTENT_DISPOSITION_HEADER not in response.headers:
                raise RuntimeError(
                    'Cannot determine attachment filename: {0} header not'
                    ' found in response headers'.format(
                        self.CONTENT_DISPOSITION_HEADER))
            output_file = response.headers[
                self.CONTENT_DISPOSITION_HEADER].split('filename=')[1]

        if os.path.exists(output_file):
            raise OSError("Output file '%s' already exists" % output_file)

        with open(output_file, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8096):
                if chunk:
                    f.write(chunk)
                    f.flush()

        return output_file
