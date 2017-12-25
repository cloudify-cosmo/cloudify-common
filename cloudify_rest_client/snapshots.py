########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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
import urlparse
import contextlib

from cloudify_rest_client import bytes_stream_utils
from cloudify_rest_client.executions import Execution
from cloudify_rest_client.responses import ListResponse


class Snapshot(dict):
    """
    Cloudify snapshot.
    """

    def __init__(self, snapshot):
        super(Snapshot, self).__init__()
        self.update(snapshot)

    @property
    def id(self):
        """
        :return: The identifier of the snapshot.
        """
        return self.get('id')

    @property
    def created_at(self):
        """
        :return: Timestamp of snapshot creation.
        """
        return self.get('created_at')

    @property
    def created_by(self):
        """
        :return: The name of the snapshot creator.
        """
        return self.get('created_by')

    @property
    def status(self):
        """
        :return: Status of snapshot.
        """
        return self.get('status')

    @property
    def error(self):
        """
        :return: Error message, if any, from snapshot creation process.
        """
        return self.get('error', '')


class SnapshotsClient(object):
    """
    Cloudify's snapshot management client.
    """

    def __init__(self, api):
        self.api = api

    def get(self, snapshot_id, _include=None):
        """
        Returns a snapshot by its id.

        :param snapshot_id: Id of the snapshot to get.
        :param _include: List of fields to include in response.
        :return: Snapshot.
        """
        assert snapshot_id
        uri = '/snapshots/{0}'.format(snapshot_id)
        response = self.api.get(uri, _include=_include)
        return Snapshot(response)

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of currently stored snapshots.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.Execution.fields
        :return: Snapshots list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/snapshots', params=params, _include=_include)
        return ListResponse([Snapshot(item) for item in response['items']],
                            response['metadata'])

    def create(self,
               snapshot_id,
               include_metrics,
               include_credentials,
               include_logs=True,
               include_events=True):
        """
        Creates a new snapshot.

        :param snapshot_id: Snapshot id of the snapshot that will be created.
        :return: The created snapshot.
        """
        assert snapshot_id
        uri = '/snapshots/{0}'.format(snapshot_id)
        params = {
            'include_metrics': include_metrics,
            'include_credentials': include_credentials,
            'include_logs': include_logs,
            'include_events': include_events
        }
        response = self.api.put(uri, data=params, expected_status_code=201)
        return Execution(response)

    def delete(self, snapshot_id):
        """
        Deletes the snapshot whose id matches the provided snapshot id.

        :param snapshot_id: The id of the snapshot to be deleted.
        :return: Deleted snapshot.
        """
        assert snapshot_id
        response = self.api.delete('/snapshots/{0}'.format(snapshot_id))
        return Snapshot(response)

    def restore(self,
                snapshot_id,
                recreate_deployments_envs=True,
                force=False,
                restore_certificates=False,
                no_reboot=False):
        """
        Restores the snapshot whose id matches the provided snapshot id.

        :param snapshot_id: The id of the snapshot to be restored.
        :param recreate_deployments_envs: If manager should recreate
        deployment environments.
        :param force: Skip clearing the manager and checking whether it is
        actually clean.
        :param restore_certificates: Whether to try and restore the
        certificates from the snapshot.
        :param no_reboot: Do not reboot after certificates restore.
        """
        assert snapshot_id
        uri = '/snapshots/{0}/restore'.format(snapshot_id)
        params = {
            'recreate_deployments_envs': recreate_deployments_envs,
            'force': force,
            'restore_certificates': restore_certificates,
            'no_reboot': no_reboot
        }
        response = self.api.post(uri, data=params)
        return Execution(response)

    def upload(self,
               snapshot_path,
               snapshot_id,
               progress_callback=None):
        """
        Uploads snapshot archive to Cloudify's manager.

        :param snapshot_path: Path to snapshot archive.
        :param snapshot_id: Id of the uploaded snapshot.
        :param progress_callback: Progress bar callback method
        :return: Uploaded snapshot.

        Snapshot archive should be the same file that had been created
        and downloaded from Cloudify's manager as a result of create
        snapshot / download snapshot commands.
        """
        assert snapshot_path
        assert snapshot_id

        uri = '/snapshots/{0}/archive'.format(snapshot_id)
        query_params = {}

        if urlparse.urlparse(snapshot_path).scheme and \
                not os.path.exists(snapshot_path):
            query_params['snapshot_archive_url'] = snapshot_path
            data = None
        else:
            data = bytes_stream_utils.request_data_file_stream_gen(
                snapshot_path, progress_callback=progress_callback)

        response = self.api.put(uri, params=query_params, data=data,
                                expected_status_code=201)
        return Snapshot(response)

    def download(self, snapshot_id, output_file, progress_callback=None):
        """
        Downloads a previously created/uploaded snapshot archive from
        Cloudify's manager.

        :param snapshot_id: The id of the snapshot to be downloaded.
        :param progress_callback: Callback function for printing a progress bar
        :param output_file: The file path of the downloaded snapshot file
         (optional)
        :return: The file path of the downloaded snapshot.
        """
        uri = '/snapshots/{0}/archive'.format(snapshot_id)

        with contextlib.closing(self.api.get(uri, stream=True)) as response:
            output_file = bytes_stream_utils.write_response_stream_to_file(
                response, output_file, progress_callback=progress_callback)

            return output_file

    def update_status(self, snapshot_id, status, error=None):
        """
        Update snapshots with the provided status and optional error.
        :param snapshot_id: Id of the snapshot to update.
        :param status: Updated snapshot's status.
        :param error: Updated snapshot error (optional).
        :return: Updated snapshot.
        """
        uri = '/snapshots/{0}'.format(snapshot_id)
        params = {'status': status}
        if error:
            params['error'] = error
        self.api.patch(uri, data=params)
