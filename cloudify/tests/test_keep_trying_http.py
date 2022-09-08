from collections import namedtuple

import pytest
from mock.mock import patch
from requests.exceptions import ConnectionError, HTTPError, Timeout

from cloudify.context import CloudifyContext
from cloudify.utils import keep_trying_http


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


@keep_trying_http(total_timeout_sec=0.001)
def one_millisecond_function(exception_class):
    raise exception_class()


def mock_me():
    pass


@keep_trying_http(total_timeout_sec=1, max_delay_sec=0)
def one_second_function():
    return mock_me()


def test_keep_trying_on_http_error():
    with patch('time.sleep') as sleep:
        pytest.raises(HTTPError,
                      one_millisecond_function,
                      HTTPError)
    assert sleep.call_count == 0


def test_keep_trying_on_timeout():
    with patch('time.sleep') as sleep:
        pytest.raises(Timeout,
                      one_millisecond_function,
                      Timeout)
    assert sleep.call_count > 0


def test_keep_trying_success():
    with patch('cloudify.tests.test_keep_trying_http.mock_me') as mock:
        mock.side_effect = [ConnectionError(), Timeout(), 'value']
        result = one_second_function()
    assert mock.call_count == 3
    assert result == 'value'


def test_keep_trying_on_connection_error():
    with patch('cloudify.tests.test_keep_trying_http.mock_me') as mock:
        mock.side_effect = [ConnectionError()] * 10 + [HTTPError(), 'value']
        pytest.raises(HTTPError,
                      one_second_function)
    assert mock.call_count == 11
