from cloudify_rest_client import utils
from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify_rest_client.responses import ListResponse
from cloudify_rest_client.constants import VisibilityState


class SecretsProvider(dict):
    def __init__(self, secrets_provider):
        super(SecretsProvider, self).__init__()
        self.update(secrets_provider)

    @property
    def id(self):
        """
        :return: Secrets Provider's id.
        :rtype: str
        """
        return self.get('id')

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
        response = self.api.get(f'/secrets-providers/{name}')

        return SecretsProvider(response)

    def create(
            self,
            name,
            _type,
            connection_parameters=None,
            visibility=VisibilityState.TENANT,
            created_at=None,
            created_by=None,
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
        :param created_at: Override the creation timestamp. Internal use only.
        :param created_by: Override the creator. Internal use only.
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

        if created_at:
            data['created_at'] = created_at

        if created_by:
            data['created_by'] = created_by

        response = self.api.put('/secrets-providers', data=data)

        return SecretsProvider(response)

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

        response = self.api.patch(f'/secrets-providers/{name}', data=data)

        return SecretsProvider(response)

    def delete(self, name):
        """
        Delete a Secrets Provider.
        """
        self.api.delete(f'/secrets-providers/{name}')

    def list(self, _include=None, **kwargs):
        """
        Returns a list of currently stored Secrets Providers.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param kwargs: Additional params are filter keys.
        :return: Secrets Parameters list.
        """
        params = kwargs
        if _include:
            params['_include'] = ','.join(_include)

        response = self.api.get('/secrets-providers', params=params)

        return ListResponse(
            [SecretsProvider(item) for item in response['items']],
            response['metadata'],
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

        response = self.api.put(
            '/secrets-providers',
            data=data,
            expected_status_code=204,
        )

        return response

    def dump(self, secrets_provider_ids=None):
        """Generate secrets' providers' attributes for a snapshot.

        :param secrets_provider_ids: A list of secrets' provider identifiers,
         if not empty, used to select specific secrets' providers to be dumped.
        :returns: A generator of dictionaries, which describe secrets'
         providers' attributes.
        """
        for entity in utils.get_all(
                self.api.get,
                '/secrets-providers',
                _include=['id', 'created_at', 'name', 'visibility', 'type',
                          'connection_parameters', 'created_by', 'created_at'],
        ):
            entity_id = entity.pop('id')
            if not secrets_provider_ids or entity_id in secrets_provider_ids:
                yield entity

    def restore(self, entities, logger):
        """Restore secrets' providers from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         secrets' providers to be restored.
        :param logger: A logger instance.
        """
        for entity in entities:
            entity['_type'] = entity.pop('type', None)
            try:
                self.create(**entity)
            except CloudifyClientError as exc:
                logger.error("Error restoring secrets provider "
                             f"{entity['id']}: {exc}")
