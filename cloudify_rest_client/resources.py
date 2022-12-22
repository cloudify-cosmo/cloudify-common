import os
import tarfile
import tempfile
import zipfile

from cloudify.utils import get_manager_file_server_root
from cloudify_rest_client.exceptions import CloudifyClientError


class ResourcesClient:
    def __init__(self, api):
        self.api = api

    def download_deployment_workdir(self, deployment_id: str, dst_dir: str):
        """
        Download deployment's working directory from the manager.

        :param deployment_id: ID of the deployment to get
        :param dst_dir: source directory (local copy of deployment workdir)
        :return: location of the local copy of deployment's working directory
        """
        tenant_name = self.api.tenant_name
        uri = f'/resources/deployments/{tenant_name}/{deployment_id}/'

        result = self.api.get(
            uri,
            params={'archive': True},
            stream=True,
            url_prefix=False,
        )
        with tempfile.NamedTemporaryFile('wb') as tmp_file:
            for data in result.bytes_stream():
                tmp_file.write(data)
            tmp_file.seek(0)
            _extract_archive(tmp_file.name, dst_dir)

        return result

    def upload_deployment_workdir(self, deployment_id: str, src_dir: str):
        """
        Upload a directory as a deployment's working directory on the manager.

        :param deployment_id: ID of the deployment
        :param src_dir: source directory (local copy of deployment workdir)
        :return: result of HTTP PUT
        """
        tenant_name = self.api.tenant_name
        deployment_path = f'deployments/{tenant_name}/{deployment_id}/'
        if src_dir is None:
            src_dir = os.path.join(
                get_manager_file_server_root(),
                deployment_path,
            )
        uri = f'/resources/{deployment_path}'

        archive_file_name = _create_archive(src_dir)
        with open(archive_file_name, 'rb') as buffer:
            # Below is a not very aesthetically pleasing workaround for a known
            # bug in requests, which should be solved in requests 3.x
            # https://github.com/psf/requests/issues/4215
            data = buffer
            if _file_is_empty(archive_file_name):
                data = b''

            return self.api.put(
                uri,
                params={'extract': True},
                url_prefix=False,
                data=data,
            )


def _archive_type(file_name):
    if tarfile.is_tarfile(file_name):
        return 'tar'
    if zipfile.is_zipfile(file_name):
        return 'zip'


def _extract_archive(file_name, dst_dir=None):
    if dst_dir is None:
        dst_dir = tempfile.mkdtemp()
    archive_type = _archive_type(file_name)
    match archive_type.lower():
        case 'tar':
            with tarfile.open(file_name, 'r:*') as archive:
                archive.extractall(path=dst_dir)
            return dst_dir
        case 'zip':
            with zipfile.ZipFile(file_name) as archive:
                archive.extractall(path=dst_dir)
            return dst_dir
    raise CloudifyClientError(f'Unknown archive type {archive_type}')


def _create_archive(dir_name):
    _, tmp_file_name = tempfile.mkstemp(suffix='.tar.gz')
    with tarfile.open(tmp_file_name, 'w:gz') as archive:
        archive.add(dir_name, arcname='./')
    return tmp_file_name


def _file_is_empty(file_name):
    return os.stat(file_name).st_size == 0
