# import pytest
#
from cloudify_rest_client.exceptions import CloudifyClientError
#


def test_exception_no_traceback_nor_code():
    ex = CloudifyClientError('Error message')
    assert str(ex) == 'Error message'


def test_exception_no_traceback_with_code():
    ex = CloudifyClientError('Error message', status_code=123)
    assert str(ex) == '123: Error message'


def test_exception_with_traceback_no_code():
    ex = CloudifyClientError('Error message',
                             server_traceback='This\nis\nsome error\n')
    assert str(ex) == 'Error message (some error)'


def test_exception_with_traceback_and_code():
    ex = CloudifyClientError('Error message',
                             server_traceback='This\nis\nsome error\n',
                             status_code=234)
    assert str(ex) == '234: Error message (some error)'
