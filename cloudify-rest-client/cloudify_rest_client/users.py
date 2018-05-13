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


class User(dict):

    def __init__(self, user):
        super(User, self).__init__()
        self.update(user)

    @property
    def username(self):
        """
        :return: The username of the user.
        """
        return self.get('username')

    @property
    def role(self):
        """
        :return: The role of the user.
        """
        return self.get('role')

    @property
    def group_system_roles(self):
        """
        :return: The roles assigned to the user via groups.
        """
        return self.get('group_system_roles')

    @property
    def groups(self):
        """
        :return: The list of groups to which the user is connected.
        """
        return self.get('groups')

    @property
    def tenants(self):
        """
        :return: The list of tenants to which the user is connected.
        """
        return self.get('tenants')

    @property
    def user_tenants(self):
        """
        :return: The list of tenants to which the user is connected directly
        (not via groups).
        """
        return self.get('tenant_roles', {}).get('direct')

    @property
    def group_tenants(self):
        """
        :return: The list of tenants to which the user is connected via groups.
        """
        return self.get('tenant_roles', {}).get('groups')

    @property
    def active(self):
        """
        :return: Whether the user is active.
        """
        return self.get('active')

    @property
    def last_login_at(self):
        """
        :return: The last time the user logged in.
        """
        return self.get('last_login_at')


class UsersClient(object):

    def __init__(self, api):
        self.api = api

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of currently stored users.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.Users.fields
        :return: Users list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/users',
                                _include=_include,
                                params=params)
        return ListResponse([User(item) for item in response['items']],
                            response['metadata'])

    def create(self, username, password, role):
        data = {'username': username, 'password': password, 'role': role}
        response = self.api.put('/users', data=data, expected_status_code=201)
        return User(response)

    def set_password(self, username, new_password):
        data = {'password': new_password}
        response = self.api.post('/users/{0}'.format(username), data=data)
        return User(response)

    def set_role(self, username, new_role):
        data = {'role': new_role}
        response = self.api.post('/users/{0}'.format(username), data=data)
        return User(response)

    def get(self, username, **kwargs):
        response = self.api.get(
            '/users/{0}'.format(username),
            params=kwargs
        )
        return User(response)

    def get_self(self, **kwargs):
        response = self.api.get('/user', params=kwargs)
        return User(response)

    def delete(self, username):
        response = self.api.delete('/users/{0}'.format(username))
        return User(response)

    def activate(self, username):
        response = self.api.post(
            '/users/active/{0}'.format(username),
            data={'action': 'activate'}
        )
        return User(response)

    def deactivate(self, username):
        response = self.api.post(
            '/users/active/{0}'.format(username),
            data={'action': 'deactivate'}
        )
        return User(response)
