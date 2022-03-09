from . import MockClient


def test_username_password():
    client = MockClient(username='testuser', password='testpassword')

    client.manager.get_status()

    expected = 'Basic dGVzdHVzZXI6dGVzdHBhc3N3b3Jk'
    client.check_last_auth_headers(auth=expected)


def test_token():
    token = 'abc123'
    client = MockClient(token=token)

    client.manager.get_status()

    client.check_last_auth_headers(token=token)
