from base64 import b64encode

from . import MockClient


def test_username_password():
    username = 'testuser'
    password = 'testpassword'
    client = MockClient(username=username, password=password)

    client.manager.get_status()

    auth_b64 = b64encode(
        '{}:{}'.format(username, password).encode('utf-8')).decode('utf-8')
    expected = 'Basic {}'.format(auth_b64)

    client.check_last_auth_headers(auth=expected)


def test_token():
    token = 'abc123'
    client = MockClient(token=token)

    client.manager.get_status()

    client.check_last_auth_headers(token=token)
