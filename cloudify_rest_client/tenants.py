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

DEFAULT_TENANT_ROLE = 'user'


class Tenant(dict):

    def __init__(self, tenant):
        super(Tenant, self).__init__()
        self.update(tenant)

    @property
    def name(self):
        """
        :return: The name of the tenant.
        """
        return self.get('name')

    @property
    def users(self):
        """
        :return: The users connected to the tenant and their roles in it.
        """
        return self.get('users')

    @property
    def direct_users(self):
        """
        :return: The users connected directly to the tenant (not via groups)
        and their roles in it.
        """
        return self.get('user_roles', {}).get('direct')

    @property
    def group_users(self):
        """
        :return: The users connected to the tenant via groups and their roles
        in it.
        """
        return self.get('user_roles', {}).get('groups')

    @property
    def groups(self):
        """
        :return: The list of user groups connected the tenant.
        """
        return self.get('groups')


class TenantsClient(object):

    def __init__(self, api):
        self.api = api

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of currently stored tenants.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.Tenants.fields
        :return: Tenants list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/tenants',
                                _include=_include,
                                params=params)
        return ListResponse([Tenant(item) for item in response['items']],
                            response['metadata'])

    def create(self, tenant_name):
        response = self.api.post(
            '/tenants/{0}'.format(tenant_name),
            expected_status_code=201
        )

        return Tenant(response)

    def add_user(self, username, tenant_name, role):
        """Add user to a tenant.

        :param username: Name of the user to add to the tenant
        :param tenant_name: Name of the tenant to which the user is added
        :param role: Name of the role assigned to the user in the tenant

        """
        data = {
            'username': username,
            'tenant_name': tenant_name,
            'role': role,
        }
        response = self.api.put('/tenants/users', data=data)
        return Tenant(response)

    def update_user(self, username, tenant_name, role):
        """Update user in a tenant.

        :param username: Name of the user to add to the tenant
        :type username: str
        :param tenant_name: Name of the tenant to which the user is added
        :type tenant_name: str
        :param role: Name of the role assigned to the user in the tenant
        :type role: str

        """
        data = {
            'username': username,
            'tenant_name': tenant_name,
            'role': role,
        }
        response = self.api.patch('/tenants/users', data=data)
        return Tenant(response)

    def remove_user(self, username, tenant_name):
        data = {'username': username, 'tenant_name': tenant_name}
        response = self.api.delete('/tenants/users', data=data)
        return Tenant(response)

    def add_user_group(self, group_name, tenant_name, role):
        """Add user group to a tenant.

        :param group_name: Name of the group to add to the tenant
        :param tenant_name: Name of the tenant to which the group is added
        :param role: Name of the role assigned to the members of the group

        """
        data = {
            'group_name': group_name,
            'tenant_name': tenant_name,
            'role': role,
        }
        response = self.api.put('/tenants/user-groups', data=data)
        return Tenant(response)

    def update_user_group(self, group_name, tenant_name, role):
        """Update user group in a tenant.

        :param group_name: Name of the user to add to the tenant
        :type group_name: str
        :param tenant_name: Name of the tenant to which the group is added
        :type tenant_name: str
        :param role: Name of the role assigned to the user in the tenant
        :type role: str

        """
        data = {
            'group_name': group_name,
            'tenant_name': tenant_name,
            'role': role,
        }
        response = self.api.patch('/tenants/user-groups', data=data)
        return Tenant(response)

    def remove_user_group(self, group_name, tenant_name):
        """Remove user group from tenant.

        :param group_name: Name of the user to add to the tenant
        :type group_name: str
        :param tenant_name: Name of the tenant to which the user is added
        :type tenant_name: str

        """
        data = {'group_name': group_name, 'tenant_name': tenant_name}
        response = self.api.delete('/tenants/user-groups', data=data)
        return Tenant(response)

    def get(self, tenant_name, **kwargs):
        response = self.api.get(
            '/tenants/{0}'.format(tenant_name),
            params=kwargs
        )
        return Tenant(response)

    def delete(self, tenant_name):
        response = self.api.delete('/tenants/{0}'.format(tenant_name))
        return Tenant(response)
