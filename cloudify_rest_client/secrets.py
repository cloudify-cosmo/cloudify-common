########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

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


class SecretsClient(object):

    def __init__(self, api):
        self.api = api

    def create(self,
               key,
               value,
               update_if_exists=False,
               is_hidden_value=False,
               visibility=VisibilityState.TENANT):
        """Create secret.

        :param key: Secret key
        :type key: unicode
        :param value: Secret value
        :type value: unicode
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
        :returns: New secret metadata
        :rtype: Dict[str]

        """
        data = {
            'value': value,
            'update_if_exists': update_if_exists,
            'is_hidden_value': is_hidden_value,
            'visibility': visibility
        }
        response = self.api.put('/secrets/{0}'.format(key), data=data)
        return Secret(response)

    def update(self, key, value=None, visibility=None, is_hidden_value=None):
        data = {
            'value': value,
            'visibility': visibility,
            'is_hidden_value': is_hidden_value
        }

        # Remove the keys with value None
        data = dict((k, v) for k, v in data.items() if v is not None)
        response = self.api.patch('/secrets/{0}'.format(key), data=data)
        return Secret(response)

    def get(self, key):
        response = self.api.get('/secrets/{0}'.format(key))
        return Secret(response)

    def export(self, **kwargs):
        """
        Returns a list of secrets to be exported

        :param kwargs: The parameters given from the user
        :return: Secrets' list
        """
        params = kwargs
        response = self.api.get('/secrets/share/export', params=params)
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

    def list(self, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of currently stored secrets.

        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.Secret.fields
        :return: Secrets list.
        """

        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

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
