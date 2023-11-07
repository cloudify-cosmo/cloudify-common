import contextlib

from cloudify_rest_client import bytes_stream_utils
from cloudify_rest_client.executions import Execution
from cloudify_rest_client.responses import ListResponse


class LogBundle(dict):
    """Cloudify log bundle."""

    def __init__(self, log_bundle):
        super(LogBundle, self).__init__()
        self.update(log_bundle)

    @property
    def id(self):
        """:return: The identifier of the log bundle."""
        return self.get('id')

    @property
    def created_at(self):
        """:return: Timestamp of log bundle creation."""
        return self.get('created_at')

    @property
    def created_by(self):
        """:return: The name of the log bundle creator."""
        return self.get('created_by')

    @property
    def status(self):
        """:return: Status of log bundle."""
        return self.get('status')

    @property
    def error(self):
        """:return: Error message, if any, from log bundle creation."""
        return self.get('error', '')


class LogBundlesClient(object):
    """Cloudify's log bundle management client."""

    def __init__(self, api):
        self.api = api
        self.base_url = '/log-bundles/'

    def get(self, log_bundle_id, _include=None):
        """Returns a log bundle by its id.
        :param log_bundle_id: Id of the log bundle to get.
        :param _include: List of fields to include in response.
        :return: LogBundle.
        """
        uri = self.base_url + log_bundle_id
        response = self.api.get(uri, _include=_include)
        return LogBundle(response)

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """Returns a list of currently stored log bundles.
        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.LogBundle.fields
        :return: LogBundles list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get(self.base_url.rstrip('/'),
                                params=params, _include=_include)
        return ListResponse([LogBundle(item) for item in response['items']],
                            response['metadata'])

    def create(self,
               log_bundle_id,
               queue=False):
        """Creates a new log bundle.
        :param log_bundle_id: ID of the log bundle that will be created.
        :param queue: Whether to queue if other system workflows are in
                      progress.
        :return: The execution for creating the log bundle.
        """
        uri = self.base_url + log_bundle_id
        params = {'queue': queue}
        response = self.api.put(uri, data=params, expected_status_code=201)
        return Execution(response)

    def delete(self, log_bundle_id):
        """Deletes the log bundle whose id matches the provided log bundle id.
        :param log_bundle_id: The id of the log bundle to be deleted.
        """
        uri = self.base_url + log_bundle_id
        self.api.delete(uri)

    def download(self, log_bundle_id, output_file, progress_callback=None):
        """Downloads a previously created log bundle from a manager.
        :param log_bundle_id: The id of the log bundle to be downloaded.
        :param progress_callback: Callback function for printing a progress bar
        :param output_file: The file path of the downloaded log bundle file
         (optional)
        :return: The file path of the downloaded log bundle.
        """
        uri = self.base_url + '{}/archive'.format(log_bundle_id)

        with contextlib.closing(self.api.get(uri, stream=True)) as response:
            output_file = bytes_stream_utils.write_response_stream_to_file(
                response, output_file, progress_callback=progress_callback)

            return output_file

    def upload_archive(self, log_bundle_id, archive_path):
        """Uploads a log bundle archive, e.g. created by mgmtworker.
        :param log_bundle_id: The id of the log bundle to be uploaded.
        :param archive_path: The file path of the log bundle archive to upload.
        """
        archive_data = bytes_stream_utils.request_data_file_stream(
            archive_path,
            client=self.api,
        )
        self.api.put(
            f"{self.base_url}{log_bundle_id}/archive",
            data=archive_data,
            expected_status_code=201,
        )

    def update_status(self, log_bundle_id, status, error=None):
        """Update log bundle with the provided status and optional error.
        :param log_bundle_id: Id of the log bundle to update.
        :param status: Updated log bundle's status.
        :param error: Updated log bundle error (optional).
        """
        uri = self.base_url + log_bundle_id
        params = {'status': status}
        if error:
            params['error'] = error
        self.api.patch(uri, data=params)
