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

import os
import tempfile
import shutil
import urllib
import urlparse
import contextlib

from cloudify_rest_client import utils
from cloudify_rest_client import bytes_stream_utils
from cloudify_rest_client.responses import ListResponse
from cloudify_rest_client.constants import VisibilityState


class Blueprint(dict):

    def __init__(self, blueprint):
        super(Blueprint, self).__init__()
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
    def created_by(self):
        """
        :return: The name of the blueprint creator.
        """
        return self.get('created_by')

    @property
    def main_file_name(self):
        """
        :return: Blueprint main file name.
        """
        return self.get('main_file_name')

    @property
    def plan(self):
        """
        Gets the plan the blueprint represents: nodes, relationships etc...

        :return: The content of the blueprint.
        """
        return self.get('plan')

    @property
    def description(self):
        """
        Gets the description of the blueprint

        :return: The description of the blueprint.
        """
        return self.get('description')


class BlueprintsClient(object):

    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'blueprints'
        self._wrapper_cls = Blueprint

    def _upload(self,
                archive_location,
                blueprint_id,
                application_file_name=None,
                visibility=VisibilityState.TENANT,
                progress_callback=None):
        query_params = {'visibility': visibility}
        if application_file_name is not None:
            query_params['application_file_name'] = \
                urllib.quote(application_file_name)

        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=blueprint_id)

        # For a Windows path (e.g. "C:\aaa\bbb.zip") scheme is the
        # drive letter and therefore the 2nd condition is present
        if urlparse.urlparse(archive_location).scheme and \
                not os.path.exists(archive_location):
            # archive location is URL
            query_params['blueprint_archive_url'] = archive_location
            data = None
        else:
            # archive location is a system path - upload it in chunks
            data = bytes_stream_utils.request_data_file_stream_gen(
                archive_location, progress_callback=progress_callback)

        return self.api.put(uri, params=query_params, data=data,
                            expected_status_code=201)

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of currently stored blueprints.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.BlueprintState.fields
        :return: Blueprints list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                _include=_include,
                                params=params)
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def publish_archive(self,
                        archive_location,
                        blueprint_id,
                        blueprint_filename=None,
                        visibility=VisibilityState.TENANT,
                        progress_callback=None):
        """Publishes a blueprint archive to the Cloudify manager.

        :param archive_location: Path or Url to the archive file.
        :param blueprint_id: Id of the uploaded blueprint.
        :param blueprint_filename: The archive's main blueprint yaml filename.
        :param visibility: The visibility of the blueprint, can be 'private',
                           'tenant' or 'global'.
        :param progress_callback: Progress bar callback method
        :return: Created blueprint.

        Archive file should contain a single directory in which there is a
        blueprint file named `blueprint_filename` (if `blueprint_filename`
        is None, this value will be passed to the REST service where a
        default value should be used).
        Blueprint ID parameter is available for specifying the
        blueprint's unique Id.
        """

        response = self._upload(
            archive_location,
            blueprint_id=blueprint_id,
            application_file_name=blueprint_filename,
            visibility=visibility,
            progress_callback=progress_callback)
        return self._wrapper_cls(response)

    @staticmethod
    def calc_size(blueprint_path):
        tempdir = tempfile.mkdtemp()
        try:
            tar_path = utils.tar_blueprint(blueprint_path, tempdir)
            size = os.path.getsize(tar_path)
        finally:
            shutil.rmtree(tempdir)
        return size

    def upload(self,
               path,
               entity_id,
               visibility=VisibilityState.TENANT,
               progress_callback=None):
        """
        Uploads a blueprint to Cloudify's manager.

        :param path: Main blueprint yaml file path.
        :param entity_id: Id of the uploaded blueprint.
        :param visibility: The visibility of the blueprint, can be 'private',
                           'tenant' or 'global'.
        :param progress_callback: Progress bar callback method
        :return: Created response.

        Blueprint path should point to the main yaml file of the response
        to be uploaded. Its containing folder will be packed to an archive
        and get uploaded to the manager.
        Blueprint ID parameter is available for specifying the
        response's unique Id.
        """
        tempdir = tempfile.mkdtemp()
        try:
            tar_path = utils.tar_blueprint(path, tempdir)
            application_file = os.path.basename(path)

            blueprint = self._upload(
                tar_path,
                blueprint_id=entity_id,
                application_file_name=application_file,
                visibility=visibility,
                progress_callback=progress_callback)
            return self._wrapper_cls(blueprint)
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
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=blueprint_id)
        response = self.api.get(uri, _include=_include)
        return self._wrapper_cls(response)

    def delete(self, blueprint_id):
        """
        Deletes the blueprint whose id matches the provided blueprint id.

        :param blueprint_id: The id of the blueprint to be deleted.
        :return: Deleted blueprint.
        """
        assert blueprint_id
        response = self.api.delete('/{self._uri_prefix}/{id}'.format(
            self=self, id=blueprint_id))
        return self._wrapper_cls(response)

    def download(self, blueprint_id, output_file=None, progress_callback=None):
        """
        Downloads a previously uploaded blueprint from Cloudify's manager.

        :param blueprint_id: The Id of the blueprint to be downloaded.
        :param progress_callback: Callback function for printing a progress bar
        :param output_file: The file path of the downloaded blueprint file
         (optional)
        :return: The file path of the downloaded blueprint.
        """
        uri = '/{self._uri_prefix}/{id}/archive'.format(self=self,
                                                        id=blueprint_id)

        with contextlib.closing(
                self.api.get(uri, stream=True)) as streamed_response:

            output_file = bytes_stream_utils.write_response_stream_to_file(
                streamed_response,
                output_file,
                progress_callback=progress_callback)

            return output_file

    def set_global(self, blueprint_id):
        """
        Updates the blueprint's visibility to global

        :param blueprint_id: Blueprint's id to update.
        :return: The blueprint.
        """
        data = {'visibility': VisibilityState.GLOBAL}
        return self.api.patch(
            '/{self._uri_prefix}/{id}/set-visibility'.format(
                self=self, id=blueprint_id),
            data=data
        )

    def set_visibility(self, blueprint_id, visibility):
        """
        Updates the blueprint's visibility

        :param blueprint_id: Blueprint's id to update.
        :param visibility: The visibility to update, should be 'tenant'
                           or 'global'.
        :return: The blueprint.
        """
        data = {'visibility': visibility}
        return self.api.patch(
            '/{self._uri_prefix}/{id}/set-visibility'.format(
                self=self, id=blueprint_id),
            data=data
        )
