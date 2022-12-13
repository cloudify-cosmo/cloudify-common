from cloudify_rest_client.responses import ListResponse
from cloudify_rest_client.constants import VisibilityState


class SecretsProvider(dict):
    def __init__(self, secrets_provider):
        super(SecretsProvider, self).__init__()
        self.update(secrets_provider)

    @property
    def name(self):
        """
        :return: Secrets Provider's name.
        :rtype: str
        """
        return self.get('name')

    @property
    def type(self):
        """
        :return: Secrets Provider's type.
        :rtype: str
        """
        return self.get('type')

    @property
    def connection_parameters(self):
        """
        :return: Secrets Provider's connection parameters.
        :rtype: dict
        """
        return self.get('connection_parameters')

    @property
    def created_at(self):
        """
        :return: Secrets Provider's created date.
        :rtype: datetime
        """
        return self.get('created_at')

    @property
    def updated_at(self):
        """
        :return: Secrets Provider's updated date.
        :rtype: datetime
        """
        return self.get('updated_at')

    @property
    def visibility(self):
        """
        :return: Secrets Provider's visibility.
        :rtype: str
        """
        return self.get('visibility')

    @property
    def tenant_name(self):
        """
        :return: Secrets Provider's tenant name.
        :rtype: str
        """
        return self.get('tenant_name')


class SecretsProvidersClient(object):
    def __init__(self, api):
        self.api = api

    def get(self, name):
        return self.api.get(
            f'/secrets-providers/{name}',
            wrapper=SecretsProvider,
        )

    def create(
            self,
            name,
            _type,
            connection_parameters=None,
            visibility=VisibilityState.TENANT,
    ):
        """
        Create Secrets Provider.

        :param name: Secrets Provider name
        :type name: str
        :param _type: Secrets Provider type
        :type _type: str
        :param connection_parameters: Secrets Provider connection parameters
        :type connection_parameters: dict
        :param visibility: The visibility of the secret, can be 'private',
                           'tenant' or 'global'
        :type visibility: str
        :returns: New secrets provider metadata
        :rtype: Dict[str]
        """
        data = {
            'name': name,
            'type': _type,
            'visibility': visibility,
        }

        if connection_parameters:
            data['connection_parameters'] = connection_parameters

        return self.api.put(
            '/secrets-providers',
            data=data,
            wrapper=SecretsProvider,
        )

    def update(
            self,
            name,
            _type,
            connection_parameters,
            visibility=VisibilityState.TENANT,
    ):
        """
        Update an existing Secrets Provider.

        :param name: Secrets Provider name
        :type name: str
        :param _type: Secrets Provider type
        :type _type: str
        :param connection_parameters: Secrets Provider connection parameters
        :type connection_parameters: dict
        :param visibility: The visibility of the Secrets Provider, can be
                           'private', 'tenant' or 'global'
        :type visibility: str
        :returns: New secrets provider metadata
        :rtype: SecretsProvider
        """
        data = {
            'name': name,
            'type': _type,
            'connection_parameters': connection_parameters,
            'visibility': visibility,
        }
        data = dict((k, v) for k, v in data.items() if v is not None)

        return self.api.patch(
            f'/secrets-providers/{name}',
            data=data,
            wrapper=SecretsProvider,
        )

    def delete(self, name):
        """
        Delete a Secrets Provider.
        """
        return self.api.delete(f'/secrets-providers/{name}')

    def list(self):
        """
        Returns a list of currently stored Secrets Providers.

        :return: Secrets Parameters list.
        """
        return self.api.get(
            '/secrets-providers',
            wrapper=ListResponse.of(SecretsProvider),
        )

    def check(
            self,
            name='',
            _type='',
            connection_parameters=None,
    ):
        """
        Test a Secrets Provider connectivity.

        :param name: Secrets Provider name
        :type name: str
        :param _type: Secrets Provider type
        :type _type: str
        :param connection_parameters: Secrets Provider connection parameters
        :type connection_parameters: dict
        :param test: Determine if API call should only test a Secrets Provider
        :type visibility: bool
        :returns: New secrets provider metadata
        :rtype: SecretsProvider
        """
        data = {
            'name': name,
            'type': _type,
            'test': True,
        }

        if connection_parameters:
            data['connection_parameters'] = connection_parameters

        return self.api.put('/secrets-providers', data=data)
