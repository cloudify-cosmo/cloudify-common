from cloudify_rest_client.responses import ListResponse
from cloudify_rest_client.constants import VisibilityState


class Secret(dict):

    def __init__(self, secret):
        super(Secret, self).__init__()
        self.update(secret)

    @property
    def key(self):
        """
        :return: The key of the secret.
        """
        return self.get('key')

    @property
    def value(self):
        """
        :return: The value of the secret.
        """
        return self.get('value')

    @value.setter
    def value(self, value):
        self.update({'value': value})

    @property
    def created_at(self):
        """
        :return: Secret creation date.
        """
        return self.get('created_at')

    @property
    def updated_at(self):
        """
        :return: Secret modification date.
        """
        return self.get('updated_at')

    @property
    def visibility(self):
        """
        :return: Secret visibility.
        """
        return self.get('visibility')

    @property
    def is_hidden_value(self):
        """
        :return: If the secret's value is hidden or not.
        """
        return self.get('is_hidden_value')

    @property
    def tenant_name(self):
        """
        :return: the secret's tenant name
        """
        return self.get('tenant_name')

    @property
    def schema(self):
        """
        :return: the secret's JSON schema
        """
        return self.get('schema')

    @property
    def provider_name(self):
        """
        :return: the secret's provider name
        """
        return self.get('provider_name')

    @property
    def provider_options(self):
        """
        :return: the secret's provider options, e.g. additional path
            where a secret exists:
            {"path": "vault_sub_path"}

        :rtype: dict
        """
        return self.get('provider_options')


class SecretsClient(object):

    def __init__(self, api):
        self.api = api

    def create(
            self,
            key,
            value,
            update_if_exists=False,
            is_hidden_value=False,
            visibility=VisibilityState.TENANT,
            schema=None,
            provider_name=None,
            provider_options=None,
    ):
        """Create secret.

        :param key: Secret key
        :type key: unicode
        :param value: Secret value
        :type value: any
        :param update_if_exists:
            Update secret value if secret key already exists
        :type update_if_exists: bool
        :param is_hidden_value:
            Hidden-value secret means the secret's value will not be visible
            to all users, only to admins and to the creator of the secret
        :type is_hidden_value: bool
        :param visibility: The visibility of the secret, can be 'private',
                           'tenant' or 'global'
        :type visibility: unicode
        :param schema: A JSON schema against which the secret will be validated
        :type schema: dict
        :param provider_name: A Secrets Provider name
        :type provider_name: str
        :param provider_options: A Secrets Provider options
        :type schema: dict
        :returns: New secret metadata
        :rtype: Dict[str]

        """
        data = {
            'value': value,
            'update_if_exists': update_if_exists,
            'is_hidden_value': is_hidden_value,
            'visibility': visibility,
        }
        if schema:
            data['schema'] = schema

        if provider_name:
            data['provider_name'] = provider_name

        if provider_options:
            data['provider_options'] = provider_options

        response = self.api.put('/secrets/{0}'.format(key), data=data)
        return Secret(response)

    def update(
            self,
            key,
            value=None,
            visibility=None,
            is_hidden_value=None,
            provider_name=None,
            provider_options=None,
            **kwargs,
    ):
        kwargs.update({
            'value': value,
            'visibility': visibility,
            'is_hidden_value': is_hidden_value,
            'provider_name': provider_name,
            'provider_options': provider_options,
        })
        data = dict((k, v) for k, v in kwargs.items() if v is not None)
        response = self.api.patch('/secrets/{0}'.format(key), data=data)
        return Secret(response)

    def get(self, key):
        response = self.api.get('/secrets/{0}'.format(key))
        return Secret(response)

    def export(self, _include=None, **kwargs):
        """
        Returns a list of secrets to be exported

        :param kwargs: The parameters given from the user
        :return: Secrets' list
        """
        params = kwargs
        response = self.api.get('/secrets/share/export', params=params,
                                _include=_include)
        return response

    def import_secrets(self, secrets_list, tenant_map=None,
                       passphrase=None, override_collisions=False):
        """

        :param secrets_list: The secrets list to import.
        :param tenant_map: Tenant map - origin_tenant:destination_tenant.
        :param passphrase: The passphrase to decrypt the secrets.
        :param override_collisions: True if the existing secrets should be
                                    overridden by the imported ones.
        :return: A dictionary containing the secrets that were overridden,
                 the secrets that were collided and the secrets that were not
                 created due to errors.
        """
        data = {
            'secrets_list': secrets_list,
            'tenant_map': tenant_map,
            'passphrase': passphrase,
            'override_collisions': override_collisions
        }
        data = dict((k, v) for k, v in data.items() if v is not None)
        response = self.api.post('/secrets/share/import', data=data)
        return response

    def list(self, sort=None, is_descending=False,
             filter_rules=None, constraints=None, **kwargs):
        """
        Returns a list of currently stored secrets.

        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param filter_rules: A list of filter rules to filter the secrets
               list by
        :param constraints: A list of DSL constraints for secret_id data
               type.  The purpose is similar to the `filter_rules`, but syntax
               differs.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.Secret.fields
        :return: Secrets list.
        """
        if constraints and filter_rules:
            raise ValueError('provide either DSL constraints or '
                             'filter_id/filter_rules, not both')

        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        if filter_rules:
            response = self.api.post('/searches/secrets', params=params,
                                     data={'filter_rules': filter_rules})
        elif constraints:
            response = self.api.post('/searches/secrets', params=params,
                                     data={'constraints': constraints})
        else:
            response = self.api.get('/secrets', params=params)

        return ListResponse([Secret(item) for item in response['items']],
                            response['metadata'])

    def delete(self, key):
        self.api.delete('/secrets/{0}'.format(key))

    def set_global(self, key):
        """
        Updates the secret's visibility to global

        :param key: Secret's key to update.
        :return: The secret.
        """
        data = {'visibility': VisibilityState.GLOBAL}
        return self.api.patch(
            '/secrets/{0}/set-visibility'.format(key),
            data=data
        )

    def set_visibility(self, key, visibility):
        """
        Updates the secret's visibility

        :param key: Secret's key to update.
        :param visibility: The visibility to update, should be 'tenant'
                           or 'global'.
        :return: The secret.
        """
        data = {'visibility': visibility}
        return self.api.patch(
            '/secrets/{0}/set-visibility'.format(key),
            data=data
        )

    def dump(self):
        """Generate secrets' attributes for a snapshot.

        :returns: A generator of dictionaries, which describe secrets'
         attributes.
        """
        return self.export(
                _include=['key', 'value', 'visibility', 'is_hidden_value',
                          'encrypted', 'tenant_name', 'creator', 'created_at',
                          'provider', 'provider_options'],
                _include_metadata=True,
        )

    def restore(self, entities, logger):
        """Restore secrets from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         secrets to be restored.
        :param logger: A logger instance.
        """
        response = self.import_secrets(secrets_list=entities)
        collisions = response.get('colliding_secrets')
        if collisions:
            logger.warn('The following secrets existed: %s', collisions)
        errors = response.get('secrets_errors')
        if errors:
            yield {'errors': errors}
