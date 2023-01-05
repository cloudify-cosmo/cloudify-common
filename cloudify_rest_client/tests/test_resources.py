import os
import shutil
import tempfile
import typing
import uuid
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest

from cloudify.constants import LOCAL_RESOURCES_ROOT_ENV_KEY
from cloudify_rest_client.deployments import Deployment
from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify_rest_client.resources import ResourcesClient


class _MockCall:
    function_name: str
    args: typing.Tuple[typing.Any]
    kwargs: typing.Dict[str, typing.Any]
    result: typing.Any

    def __init__(self, function_name, args, kwargs, result):
        self.function_name = function_name
        self.args = args
        self.kwargs = kwargs
        self.result = result

    def __str__(self):
        return _call_fmt(self.function_name, self.args, self.kwargs)

    def matches(self, function_name: str, args, kwargs):
        return (
            (not self.function_name or self.function_name == function_name) and
            (not self.args or self.args == args) and
            (not self.kwargs or self.kwargs == kwargs)
        )


class _MockHTTPClient:
    expected_calls: typing.List[_MockCall]

    def __init__(self, tenant_name):
        self.tenant_name = tenant_name
        self.expected_calls = []

    def expect_call(
        self,
        function_name: str,
        args: typing.Tuple[typing.Any],
        kwargs: typing.Dict[str, typing.Any],
        result: typing.Any,
    ):
        self.expected_calls.append(
            _MockCall(function_name, args, kwargs, result)
        )

    def _called(self, function_name: str, *args, **kwargs):
        if not self.expected_calls:
            raise Exception('Unexpected call: '
                            f'{_call_fmt(function_name, args, kwargs)}')
        expected_call, *self.expected_calls = self.expected_calls
        if expected_call.matches(function_name, args, kwargs):
            return expected_call.result
        raise Exception(f'Expected call {expected_call}, got: '
                        f'{_call_fmt(function_name, args, kwargs)}')

    def get(self, *args, **kwargs):
        return self._called('get', *args, **kwargs)

    def put(self, *args, **kwargs):
        return self._called('put', *args, **kwargs)


def _call_fmt(
    function_name: str,
    args: typing.Tuple[typing.Any],
    kwargs: typing.Dict[str, typing.Any]
) -> str:
    args_quoted = [f"'{a}'" if isinstance(a, str) else a for a in args] \
        if args else []
    kwargs_formatted = [f'{k}={v}' for k, v in kwargs.items()] \
        if kwargs else []
    return f"{function_name}({', '.join(args_quoted)}, " \
           f"{', '.join(kwargs_formatted)})"


@pytest.fixture(scope='function')
def client() -> ResourcesClient:
    os.environ[LOCAL_RESOURCES_ROOT_ENV_KEY] = '/tmp/resources'
    resources_client = ResourcesClient(api=_MockHTTPClient('default_tenant'))
    yield resources_client
    assert not resources_client.api.expected_calls


@pytest.fixture(scope='function')
def deployment() -> Deployment:
    return Deployment({'id': uuid.uuid4().hex})


def test_download_deployment_workdir_archive(client, deployment):
    client.api.expect_call(
        'get',
        (_deployment_uri(deployment),),
        {'params': {'archive': True}, 'stream': True, 'url_prefix': False},
        Mock(ok=True, bytes_stream=lambda: [])
    )
    with tempfile.TemporaryDirectory() as tmp_dir:
        with pytest.raises(CloudifyClientError,
                           match="Unknown archive type: ``"):
            client.download_deployment_workdir(deployment.id, tmp_dir)


def test_download_deployment_workdir_single_file_mtime(client, deployment):
    deployment_dir = _deployment_dir(deployment)
    os.makedirs(deployment_dir)
    with open(os.path.join(deployment_dir, '.cloudify-index.json'),
              'wt', encoding='utf-8') as index:
        index.write('{"res.txt": "1999-12-31T18:00:00+00:00"}')

    client.api.expect_call(
        'get',
        (_deployment_uri(deployment),),
        {'url_prefix': False},
        {'res.txt': '2023-01-03T06:00:00+00:00'}
    )
    _assert_file_downloaded(client, deployment, 'res.txt')


def test_download_deployment_workdir_new_file(client, deployment):
    _, _, mtime = _prepare_not_synced_file(deployment, 'dont-sync.txt')

    client.api.expect_call(
        'get',
        (_deployment_uri(deployment),),
        {'url_prefix': False},
        {'dont-sync.txt': mtime.isoformat(),
         'res.txt': '2023-01-03T06:00:00+00:00'}
    )

    _assert_file_downloaded(client, deployment, 'res.txt')


def test_upload_deployment_workdir_archive(client, deployment):
    deployment_dir = _deployment_dir(deployment)
    os.makedirs(deployment_dir)
    res_file_path = os.path.join(deployment_dir, 'res.txt')
    with open(res_file_path, 'wt', encoding='utf-8') as resource:
        resource.write('Hello from test')

    client.api.expect_call(
        'put',
        (deployment_dir.replace('/tmp', '', 1) + '/',),
        {},
        Mock(ok=True)
    )
    try:
        client.upload_deployment_workdir(deployment.id, deployment_dir)
    finally:
        shutil.rmtree(deployment_dir)


def test_upload_deployment_workdir_single_file_mtime(client, deployment):
    deployment_dir = _deployment_dir(deployment)
    os.makedirs(deployment_dir)
    res_file_path = os.path.join(deployment_dir, 'res.txt')
    with open(res_file_path, 'wt', encoding='utf-8') as resource:
        resource.write('Hello from test')
    mtime = datetime.fromtimestamp(
        os.stat(res_file_path).st_mtime,
        tz=timezone.utc,
    )
    with open(os.path.join(deployment_dir, '.cloudify-index.json'),
              'wt', encoding='utf-8') as index:
        index.write(f'{{"res.txt": "{mtime.isoformat()}"}}')

    client.api.expect_call(
        'get',
        (f'/resources/deployments/default_tenant/{deployment.id}/',),
        {'url_prefix': False},
        {'res.txt': '1999-12-31T18:00:00+00:00'}
    )

    client.api.expect_call(
        'put',
        (deployment_dir.replace('/tmp', '', 1) + '/res.txt',),
        None,  # don't verify kwargs: they contain a non-deterministic `data`
        Mock(ok=True)
    )
    try:
        client.upload_deployment_workdir(deployment.id, deployment_dir)
    finally:
        shutil.rmtree(deployment_dir)


def test_upload_deployment_workdir_new_file(client, deployment):
    deployment_dir, _, mtime = _prepare_not_synced_file(deployment,
                                                        'dont-sync.txt')

    res_file_path = os.path.join(deployment_dir, 'res.txt')
    with open(res_file_path, 'wt', encoding='utf-8') as resource:
        resource.write('Hello from test')

    client.api.expect_call(
        'get',
        (f'/resources/deployments/default_tenant/{deployment.id}/',),
        {'url_prefix': False},
        {'dont-sync.txt': mtime.isoformat()}
    )
    client.api.expect_call(
        'put',
        (deployment_dir.replace('/tmp', '', 1) + '/res.txt',),
        None,  # don't verify kwargs: they contain a non-deterministic `data`
        Mock(ok=True)
    )
    try:
        client.upload_deployment_workdir(deployment.id, deployment_dir)
    finally:
        shutil.rmtree(deployment_dir)


def _deployment_dir(deployment: Deployment) -> str:
    return os.path.join(
        os.environ[LOCAL_RESOURCES_ROOT_ENV_KEY],
        'deployments',
        'default_tenant',
        deployment.id,
    )


def _deployment_uri(deployment: Deployment) -> str:
    return f'/resources/deployments/default_tenant/{deployment.id}/'


def _assert_file_downloaded(client, deployment, file_name):
    deployment_dir = _deployment_dir(deployment)
    client.api.expect_call(
        'get',
        (f'{_deployment_uri(deployment)}{file_name}',),
        {'stream': True, 'url_prefix': False},
        Mock(ok=True, bytes_stream=lambda: [b'Hello from test'])
    )

    try:
        client.download_deployment_workdir(deployment.id, deployment_dir)
        res_file_path = os.path.join(deployment_dir, file_name)
        assert os.path.isfile(res_file_path)
        with open(res_file_path, 'rt', encoding='utf-8') as resource:
            assert resource.read().strip() == 'Hello from test'
    finally:
        shutil.rmtree(deployment_dir, ignore_errors=True)


def _prepare_not_synced_file(deployment, file_name):
    deployment_dir = _deployment_dir(deployment)
    os.makedirs(deployment_dir)
    dont_sync_file_path = os.path.join(deployment_dir, file_name)
    with open(dont_sync_file_path, 'wt', encoding='utf-8') as resource:
        resource.write('Does not require download')
    mtime = datetime.fromtimestamp(
        os.stat(dont_sync_file_path).st_mtime,
        tz=timezone.utc,
    )
    with open(os.path.join(deployment_dir, '.cloudify-index.json'),
              'wt', encoding='utf-8') as index:
        index.write(f'{{"{file_name}": "{mtime.isoformat()}"}}')
    return deployment_dir, dont_sync_file_path, mtime
