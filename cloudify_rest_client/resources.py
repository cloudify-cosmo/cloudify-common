import json
import os
import shutil
import tarfile
import tempfile
import zipfile
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import Dict, Tuple

import requests

from cloudify_rest_client.exceptions import CloudifyClientError

INDEX_JSON_FILENAME = '.cloudify-index.json'
LAST_MODIFIED_FMT = '%Y-%m-%dT%H:%M:%S%z'


class ResourcesClient:
    def __init__(self, api):
        self.api = api

    def _deployment_workdir_uri(self, deployment_id: str) -> str:
        return f'/resources/deployments/{self.api.tenant_name}/'\
               f'{deployment_id}/'

    def download_deployment_workdir(self, deployment_id: str, dst_dir: str):
        """
        Download deployment's working directory from the manager.

        :param deployment_id: ID of the deployment to get
        :param dst_dir: source directory (local copy of deployment workdir)
        :return: location of the local copy of deployment's working directory
        """
        uri = self._deployment_workdir_uri(deployment_id)

        if os.path.isfile(os.path.join(dst_dir, INDEX_JSON_FILENAME)):
            return self._download_deployment_workdir_files(uri, dst_dir)

        return self._download_deployment_workdir_archive(uri, dst_dir)

    def _download_deployment_workdir_archive(self, uri: str, dst_dir: str):
        try:
            response = self.api.get(
                uri,
                params={'archive': True},
                stream=True,
                url_prefix=False,
            )
        except requests.RequestException as exception:
            raise CloudifyClientError(
                f"Unable to download single deployment's file "
                f"from {uri}") from exception

        with tempfile.NamedTemporaryFile('wb') as tmp_file:
            for data in response.bytes_stream():
                tmp_file.write(data)
            tmp_file.seek(0)
            _extract_archive(tmp_file.name, dst_dir)

    def _download_deployment_workdir_files(self, uri: str, dst_dir: str):
        manager_files = self._fetch_manager_directory_index(uri)
        local_files = self._read_local_directory_index(dst_dir)

        for file_path, file_mtime in \
                self._metadata_diff(manager_files, local_files):
            self._download_single_file(uri, dst_dir, file_path, file_mtime)

        self._save_local_index(dst_dir, manager_files)

    def _download_single_file(self,
                              base_uri: str,
                              dst_dir: str,
                              file_path: str,
                              file_mtime: str):
        try:
            response = self.api.get(
                f'{base_uri}{file_path}',
                stream=True,
                url_prefix=False,
            )
        except requests.RequestException as exception:
            raise CloudifyClientError(
                f"Unable to download single deployment's file "
                f"from {base_uri}{file_path}") from exception
        with tempfile.NamedTemporaryFile('wb',
                                         dir=dst_dir,
                                         delete=False) as tmp_file:
            for chunk in response.bytes_stream():
                tmp_file.write(chunk)
        absolute_file_path = os.path.join(dst_dir, file_path)
        file_timestamp = datetime.fromisoformat(file_mtime).timestamp()
        shutil.move(tmp_file.name, absolute_file_path)
        os.utime(absolute_file_path, (file_timestamp, file_timestamp))

    def upload_deployment_workdir(self, deployment_id: str, src_dir: str):
        """
        Upload a directory as a deployment's working directory on the manager.

        :param deployment_id: ID of the deployment
        :param src_dir: source directory (local copy of deployment workdir)
        :return: result of HTTP PUT
        """
        uri = self._deployment_workdir_uri(deployment_id)

        if os.path.isfile(os.path.join(src_dir, INDEX_JSON_FILENAME)):
            return self._upload_deployment_workdir_files(uri, src_dir)

        return self._upload_deployment_workdir_archive(uri, src_dir)

    def _upload_deployment_workdir_archive(self, uri: str, src_dir: str):
        archive_file_name = _create_archive(src_dir)
        return self._upload_single_file(uri, archive_file_name, extract=True)

    def _upload_deployment_workdir_files(self, uri: str, src_dir: str):
        manager_files = self._fetch_manager_directory_index(uri)
        local_files = self._generate_directory_metadata(src_dir)

        self._save_local_index(src_dir, local_files)

        for file_path, _ in \
                self._metadata_diff(local_files, manager_files):
            return self._upload_single_file(
                f'{uri}{file_path}',
                os.path.join(src_dir, file_path),
            )

    def _upload_single_file(self, uri: str, file_path: str, extract=False):
        with open(file_path, 'rb') as buffer:
            # Below is a not very aesthetically pleasing workaround for a known
            # bug in requests, which should be solved in requests 3.x
            # https://github.com/psf/requests/issues/4215
            data = buffer
            if _file_is_empty(file_path):
                data = b''

            response = self.api.put(
                uri,
                params={'extract': 'yes'} if extract else {},
                url_prefix=False,
                data=data,
            )
            if not response.ok:
                raise CloudifyClientError(
                    f"Unable to upload deployment's file {file_path} "
                    f"into {uri}: {response}")
            return response

    def upload_deployment_file(
            self,
            deployment_id: str,
            target_file_path: str,
            src_file: str):
        """Upload a single file to the deployment's working directory"""
        uri = self._deployment_workdir_uri(deployment_id)
        return self._upload_single_file(f'{uri}{target_file_path}', src_file)

    @contextmanager
    def sync_deployment_workdir(self, deployment_id: str, local_dir: str):
        self.download_deployment_workdir(deployment_id, local_dir)
        before_metadata = self._read_local_directory_index(local_dir)
        try:
            yield
        finally:
            after_metadata = self._generate_directory_metadata(local_dir)
            for file_path, _ in \
                    self._metadata_diff(before_metadata, after_metadata):
                self.upload_deployment_file(
                    deployment_id,
                    file_path,
                    os.path.join(local_dir, file_path),
                )

    def _fetch_manager_directory_index(self, uri: str) -> Dict[str, str]:
        return self.api.get(uri, url_prefix=False)

    def _read_local_directory_index(self, dst_dir: str) -> Dict[str, str]:
        index_file_path = os.path.join(dst_dir, INDEX_JSON_FILENAME)
        with open(index_file_path, encoding='utf-8') as file:
            return json.load(file)

    def _generate_directory_metadata(self, local_dir: str) -> Dict[str, str]:
        metadata = {}
        for dir_path, _, file_names in os.walk(local_dir):
            for name in file_names:
                absolute_file_path = os.path.join(dir_path, name)
                file_path = os.path.relpath(absolute_file_path, local_dir)
                if file_path == INDEX_JSON_FILENAME:
                    continue
                file_mtime = datetime.fromtimestamp(
                    os.stat(absolute_file_path).st_mtime,
                    tz=timezone.utc,
                )
                metadata[file_path] = file_mtime.isoformat()
        return metadata

    def _metadata_diff(
            self,
            master_metadata: Dict[str, str],
            auxiliary_metadata: Dict[str, str]
    ) -> Tuple[str, str]:
        for file_path, file_mtime in master_metadata.items():
            if file_path not in auxiliary_metadata or\
                    auxiliary_metadata[file_path] != file_mtime:
                yield file_path, file_mtime

    def _save_local_index(self, local_dir: str, file_metadata: Dict[str, str]):
        with open(os.path.join(local_dir, INDEX_JSON_FILENAME),
                  'wt',
                  encoding='utf-8') as index_file:
            json.dump(file_metadata, index_file)


def _archive_type(file_name) -> str:
    if tarfile.is_tarfile(file_name):
        return 'tar'
    if zipfile.is_zipfile(file_name):
        return 'zip'
    return ''


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
    raise CloudifyClientError(f'Unknown archive type: `{archive_type}`')


def _create_archive(dir_name):
    _, tmp_file_name = tempfile.mkstemp(suffix='.tar.gz')
    with tarfile.open(tmp_file_name, 'w:gz') as archive:
        archive.add(dir_name, arcname='./')
    return tmp_file_name


def _file_is_empty(file_name):
    return os.stat(file_name).st_size == 0
