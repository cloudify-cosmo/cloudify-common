class IdentityProviderClient(object):
    """
    Cloudify's identity provider client.
    """

    def __init__(self, api):
        self.api = api

    def get(self):
        """
        :return: Cloudify configured identity provider.
        """
        return self.api.get('/idp')
