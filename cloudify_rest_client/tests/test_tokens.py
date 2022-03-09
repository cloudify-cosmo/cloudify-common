import pytest

from cloudify_rest_client.tokens import Token

from . import MockClient


def test_token_create_bad_expiration_format():
    client = MockClient()

    with pytest.raises(Exception, match='not.*legal.*format'):
        client.tokens.create(expiration='wrong')

    client.mock_do_request.assert_not_called()


def test_token_create():
    client = MockClient()

    result = client.tokens.create()

    client.assert_last_mock_call(endpoint='/tokens',
                                 data='{}',
                                 expected_method='post')

    assert isinstance(result, Token)


def test_token_list():
    client = MockClient()

    client.mock_do_request.return_value = {'items': [{}, {}, {}],
                                           'metadata': {}}

    result = client.tokens.list()

    client.assert_last_mock_call(endpoint='/tokens')

    assert len(result) == 3
    assert all(isinstance(item, Token) for item in result)


def test_token_get():
    client = MockClient()

    token_id = 'some_token'

    result = client.tokens.get(token_id)

    client.assert_last_mock_call(endpoint='/tokens/{}'.format(token_id))

    assert isinstance(result, Token)


def test_token_delete():
    client = MockClient()

    token_id = 'some_token'

    client.tokens.delete(token_id)

    client.assert_last_mock_call(endpoint='/tokens/{}'.format(token_id),
                                 expected_method='delete',
                                 expected_status_code=(200, 204))
