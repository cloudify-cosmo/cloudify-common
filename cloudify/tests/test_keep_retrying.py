from collections import namedtuple

import pytest
from mock.mock import patch
from requests.exceptions import HTTPError

from cloudify.context import CloudifyContext


def mock_ctx():
    Ctx = namedtuple(
        'Ctx',
        ['bypass_maintenance', 'execution_token', 'rest_host', 'tenant_name', ]
    )
    return Ctx(bypass_maintenance=False,
               execution_token='token',
               rest_host='non.existent.host.name',
               tenant_name='default_tenant')


@patch('cloudify.utils._get_current_context', mock_ctx)
@patch('cloudify.utils.get_manager_name', return_value='test-manager')
@patch('cloudify.utils.get_manager_rest_service_port', return_value=8888)
@patch('cloudify.utils.get_local_rest_certificate', return_value=None)
def test_update_operation_fail_on_other_exception(*_):
    c = CloudifyContext(ctx={'tenant': {'name': 'default_tenant'},
                             'task_id': 'testing_task'})
    with patch('cloudify_rest_client.client.HTTPClient.do_request') as client:
        client.side_effect = HTTPError()
        pytest.raises(
            HTTPError,
            c.update_operation,
            'foobar')
