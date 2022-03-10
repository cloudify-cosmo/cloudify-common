from cloudify.utils import parse_utc_datetime
from cloudify_rest_client.responses import ListResponse


class Token(dict):

    def __init__(self, token):
        super(Token, self).__init__()
        self.update(token)

    @property
    def value(self):
        """
        :returns: The value of the token.
        """
        return self.get('value')

    @property
    def role(self):
        """
        :returns: The role of the user associated with the token.
        """
        return self.get('role')

    @property
    def username(self):
        """:returns: The username associated with the token."""
        return self.get('username')

    @property
    def description(self):
        """:returns: The description of the token."""
        return self.get('description')

    @property
    def expiration_date(self):
        """:returns: The expiration date of the token."""
        return self.get('expiration_date')

    @property
    def last_used(self):
        """:returns: The last time the token was used."""
        return self.get('last_used')

    @property
    def id(self):
        """:returns: The ID of the token."""
        return self.get('id')


class TokensClient(object):

    def __init__(self, api):
        self.api = api

    def list(self, **kwargs):
        """Get a list of tokens.
        :param kwargs: Optional fields or filter arguments as defined in the
                       restservice.
        """
        response = self.api.get('/tokens', params=kwargs)

        return ListResponse(
            [Token(item) for item in response['items']],
            response['metadata']
        )

    def get(self, token_id):
        """Get details of an existing authentication token.
        :param token_id: The ID of the token to get.

        :return: Token
        """
        return Token(self.api.get('/tokens/{}'.format(token_id)))

    def delete(self, token_id):
        """Delete an existing token, revoking its access."""
        self.api.delete('/tokens/{}'.format(token_id))

    def create(self, description=None, expiration=None):
        """Create a new authentication token for the current user.
        :param description: The description of the token.
        :param expiration: The expiration date of the token.
                           Can be provided in format YYYY-MM-DD HH:mm
                           or relative to current time,
                           e.g. '+10h' for 10 hours
                           or '+30 minutes' for 30 minutes
                           or '+1 day' for 1 day
        :return: Token
        """
        data = {}
        if description:
            data['description'] = description
        if expiration:
            parse_utc_datetime(expiration)
            data['expiration_date'] = expiration
        return Token(self.api.post(
            '/tokens',
            data=data,
        ))
