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
import contextlib

from cloudify_rest_client import utils
from cloudify_rest_client import bytes_stream_utils
from cloudify_rest_client._compat import urlquote, urlparse
from cloudify_rest_client.constants import VisibilityState
from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify_rest_client.responses import ListResponse

from .labels import Label


class Blueprint(dict):

    def __init__(self, blueprint):
        super(Blueprint, self).__init__()
        self.update(blueprint)
        if self.get('labels'):
            self['labels'] = [Label(item) for item in self['labels']]

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

    @property
    def state(self):
        """
        Gets the upload state of the blueprint

        :return: The upload state of the blueprint.
        """
        return self.get('state')

    @property
    def labels(self):
        """
        :return: The labels of this blueprint.
        """
        return self.get('labels')

    @property
    def upload_execution(self):
        """
        :return: The upload_blueprint execution that parsed this blueprint.
        """
        return self.get('upload_execution')


class BlueprintsClient(object):

    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'blueprints'
        self._wrapper_cls = Blueprint

    def _prepare_put_request(self,
                             archive_location,
                             application_file_name,
                             visibility,
                             progress_callback,
                             async_upload,
                             labels=None):
        query_params = {'visibility': visibility, 'async_upload': async_upload}
        if application_file_name is not None:
            query_params['application_file_name'] = \
                urlquote(application_file_name)
        if labels is not None:
            labels_params = []
            for label in labels:
                if (not isinstance(label, dict)) or len(label) != 1:
                    raise CloudifyClientError(
                        'Labels must be a list of 1-entry dictionaries: '
                        '[{<key1>: <value1>}, {<key2>: [<value2>, <value3>]}, '
                        '...]')

                [(key, value)] = label.items()
                value = value.replace('=', '\\=').replace(',', '\\,')
                labels_params.append('{0}={1}'.format(key, value))
            query_params['labels'] = ','.join(labels_params)

        # For a Windows path (e.g. "C:\aaa\bbb.zip") scheme is the
        # drive letter and therefore the 2nd condition is present
        if urlparse(archive_location).scheme and \
                not os.path.exists(archive_location):
            # archive location is URL
            query_params['blueprint_archive_url'] = archive_location
            data = None
        else:
            # archive location is a system path
            data = bytes_stream_utils.request_data_file_stream(
                archive_location,
                progress_callback=progress_callback,
                client=self.api)

        return query_params, data

    def _upload(self,
                archive_location,
                blueprint_id,
                application_file_name=None,
                visibility=VisibilityState.TENANT,
                progress_callback=None,
                async_upload=False,
                labels=None):
        query_params, data = self._prepare_put_request(
            archive_location,
            application_file_name,
            visibility,
            progress_callback,
            async_upload,
            labels
        )
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=blueprint_id)
        return self.api.put(
            uri,
            params=query_params,
            data=data,
            expected_status_code=201
        )

    def _validate(self,
                  archive_location,
                  blueprint_id,
                  application_file_name=None,
                  visibility=VisibilityState.TENANT,
                  progress_callback=None):
        query_params, data = self._prepare_put_request(
            archive_location,
            application_file_name,
            visibility,
            progress_callback,
            async_upload=False,
        )
        uri = '/{self._uri_prefix}/{id}/validate'.format(self=self,
                                                         id=blueprint_id)
        self.api.put(
            uri,
            params=query_params,
            data=data,
            expected_status_code=204
        )

    def _validate_blueprint_size(self, path, tempdir, skip_size_limit):
        blueprint_directory = os.path.dirname(path) or os.getcwd()
        size, files = utils.get_folder_size_and_files(blueprint_directory)

        try:
            config = self.api.get('/config', params={'scope': 'rest'})
        except CloudifyClientError as err:
            if err.status_code == 404:
                config = {}
            else:
                raise

        size_limit = config.get('blueprint_folder_max_size_mb', {}).get(
            'value', 50)
        files_limit = config.get('blueprint_folder_max_files', {}).get(
            'value', 10000)

        if not skip_size_limit:
            error_message = '{0}, move some resources from the blueprint ' \
                            'folder to an external location or upload the ' \
                            'blueprint folder as a zip file.'
            if size > size_limit * 1000000:
                raise Exception(error_message.format(
                    'Blueprint folder size exceeds {} MB'.format(size_limit)))
            if files > files_limit:
                raise Exception(error_message.format(
                    'Number of files in blueprint folder exceeds {}'.format(
                        files_limit)))
        tar_path = utils.tar_blueprint(path, tempdir)
        return tar_path, os.path.basename(path)

    def list(self, _include=None, sort=None, is_descending=False,
             filter_id=None, filter_rules=None, **kwargs):
        """
        Returns a list of currently stored blueprints.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param filter_id: A filter ID to filter the blueprints list by
        :param filter_rules: A list of filter rules to filter the
               blueprints list by
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.BlueprintState.fields
        :return: Blueprints list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort
        if _include:
            params['_include'] = ','.join(_include)
        if filter_id:
            params['_filter_id'] = filter_id

        if filter_rules:
            response = self.api.post('/searches/blueprints', params=params,
                                     data={'filter_rules': filter_rules})
        else:
            response = self.api.get('/{self._uri_prefix}'.format(self=self),
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
                        progress_callback=None,
                        async_upload=False,
                        labels=None):
        """Publishes a blueprint archive to the Cloudify manager.

        :param archive_location: Path or Url to the archive file.
        :param blueprint_id: Id of the uploaded blueprint.
        :param blueprint_filename: The archive's main blueprint yaml filename.
        :param visibility: The visibility of the blueprint, can be 'private',
                           'tenant' or 'global'.
        :param progress_callback: Progress bar callback method
        :param labels: The blueprint's labels. A list of 1-entry
            dictionaries: [{<key1>: <value1>}, {<key2>: <value2>}, ...]'
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
            progress_callback=progress_callback,
            async_upload=async_upload,
            labels=labels)
        if not async_upload:
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
               progress_callback=None,
               skip_size_limit=True,
               async_upload=False,
               labels=None):
        """
        Uploads a blueprint to Cloudify's manager.

        :param path: Main blueprint yaml file path.
        :param entity_id: Id of the uploaded blueprint.
        :param visibility: The visibility of the blueprint, can be 'private',
                           'tenant' or 'global'.
        :param progress_callback: Progress bar callback method
        :param skip_size_limit: Indicator whether to check size limit on
                           blueprint folder
        :param labels: The blueprint's labels. A list of 1-entry
            dictionaries: [{<key1>: <value1>}, {<key2>: <value2>}, ...]'
        :return: Created response.

        Blueprint path should point to the main yaml file of the response
        to be uploaded. Its containing folder will be packed to an archive
        and get uploaded to the manager.
        Blueprint ID parameter is available for specifying the
        response's unique Id.
        """
        tempdir = tempfile.mkdtemp()
        try:
            tar_path, application_file = self._validate_blueprint_size(
                path, tempdir, skip_size_limit)

            response = self._upload(
                tar_path,
                blueprint_id=entity_id,
                application_file_name=application_file,
                visibility=visibility,
                progress_callback=progress_callback,
                async_upload=async_upload,
                labels=labels)
            if not async_upload:
                return self._wrapper_cls(response)
        finally:
            shutil.rmtree(tempdir)

    def validate(self,
                 path,
                 entity_id,
                 blueprint_filename=None,
                 visibility=VisibilityState.TENANT,
                 progress_callback=None,
                 skip_size_limit=True):
        """
        Validates a blueprint with Cloudify's manager.

        :param path: Main blueprint yaml file path.
        :param entity_id: Id of the uploaded blueprint.
        :param blueprint_filename: The archive's main blueprint yaml filename.
        :param visibility: The visibility of the blueprint, can be 'private',
                           'tenant' or 'global'.
        :param progress_callback: Progress bar callback method
        :param skip_size_limit: Indicator whether to check size limit on
                           blueprint folder

        Blueprint path should point to the main yaml file of the response
        to be uploaded. Its containing folder will be packed to an archive
        and get uploaded to the manager.
        Validation is basically an upload without the storage part being done.
        """
        tempdir = tempfile.mkdtemp()
        tar_path = None
        application_file = None
        try:
            if not urlparse(path).scheme or os.path.exists(path):
                # path is not a URL, create archive
                tar_path, application_file = self._validate_blueprint_size(
                    path, tempdir, skip_size_limit)

            self._validate(
                tar_path or path,
                blueprint_id=entity_id,
                application_file_name=application_file or blueprint_filename,
                visibility=visibility,
                progress_callback=progress_callback)
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

    def delete(self, blueprint_id, force=False):
        """
        Deletes the blueprint whose id matches the provided blueprint id.

        :param blueprint_id: The id of the blueprint to be deleted.
        :param force: Delete blueprint even if there is a blueprint
                      currently importing it.
        :return: Deleted blueprint.
        """
        assert blueprint_id

        self.api.delete(
            '/{self._uri_prefix}/{id}'.format(self=self, id=blueprint_id),
            params={'force': force})

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

    def update(self, blueprint_id, update_dict):
        """
        Update a blueprint.

        Used for updating the blueprint's state (and error) while uploading,
        and updating the blueprint's other attributes upon a successful upload.
        This method is for internal use only.

        :param blueprint_id: Blueprint's id to update.
        :param update_dict: Dictionary of attributes and values to be updated.
        :return: The updated blueprint.
        """
        response = self.api.patch('/{self._uri_prefix}/{id}'.format(
            self=self, id=blueprint_id),
            data=update_dict
        )

        return self._wrapper_cls(response)

    def upload_archive(self, blueprint_id, archive_path):
        """
        Upload an archive for an existing a blueprint.

        Used for uploading the blueprint's archive, downloaded from a URL using
        a system workflow, to the manager's file server
        This method is for internal use only.

        :param blueprint_id: Blueprint's id to update.
        :param archive_path: Path of a local blueprint archive data to upload
            to the manager's file server. Valid only when the blueprint's
            current upload state is `Uploading`, and is not being updated.
        """
        archive_data = bytes_stream_utils.request_data_file_stream(
            archive_path,
            client=self.api)
        self.api.put('/{self._uri_prefix}/{id}/archive'.format(
            self=self, id=blueprint_id),
            data=archive_data
        )
