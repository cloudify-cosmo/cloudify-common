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


class Blueprint(dict):

    def __init__(self, blueprint):
        self.update(blueprint)

    @property
    def id(self):
        return self['id']


class BlueprintsClient(object):

    CONTENT_DISPOSITION_HEADER = 'content-disposition'

    def __init__(self, api):
        self.api = api

    def list(self):
        response = self.api.get('/blueprints')
        return [Blueprint(item) for item in response]

    def upload_blueprint(self, blueprint_path, blueprint_id):
        tempdir = tempfile.mkdtemp()
        try:
            tar_path = self._tar_blueprint(blueprint_path, tempdir)
            application_file = os.path.basename(blueprint_path)

            with self._protected_call_to_server('publishing blueprint'):
                with open(tar_path, 'rb') as f:
                    blueprint_state = self._blueprints_api.upload(
                        f,
                        application_file_name=application_file,
                        blueprint_id=blueprint_id)
                return blueprint_state
        finally:
            shutil.rmtree(tempdir)

    def get(self, blueprint_id):
        assert blueprint_id
        response = self.api.get('/blueprints/{0}'.format(blueprint_id))
        return Blueprint(response)

    def get_source(self, blueprint_id):
        assert blueprint_id
        return self.api.get('/blueprints/{0}/source'.format(blueprint_id))

    def delete(self, blueprint_id):
        assert blueprint_id
        response = self.api.delete('/blueprints/{0}'.format(blueprint_id))
        return response

    def download(self, blueprint_id, output_file=None):
        """
        Downloads a previously uploaded blueprint from Cloudify's manager.

        :param blueprint_id: The Id of the blueprint to be downloaded.
        :param output_file: The file path of the downloaded blueprint file
         (optional)
        :return: The file path of the downloaded blueprint.
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


