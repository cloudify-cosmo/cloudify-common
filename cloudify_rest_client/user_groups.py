from cloudify_rest_client import constants, utils
from cloudify_rest_client.responses import ListResponse


class Group(dict):

    def __init__(self, group):
        super(Group, self).__init__()
        self.update(group)

    @property
    def name(self):
        """
        :return: The name of the group.
        """
        return self.get('name')

    @property
    def users(self):
        """
        :return: The list of users connected to the group.
        """
        return self.get('users')

    @property
    def role(self):
        """
        :return: The role of the group.
        """
        return self.get('role')

    @property
    def tenants(self):
        """
        :return: The list of tenants to which the group is connected.
        """
        return self.get('tenants')

    @property
    def ldap_dn(self):
        """
        :return: If using ldap, will return the group's distinguished name.
        """
        return self.get('ldap_dn')


class UserGroupsClient(object):

    def __init__(self, api):
        self.api = api

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of currently stored user groups.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.BlueprintState.fields
        :return: Blueprints list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/user-groups',
                                _include=_include,
                                params=params)
        return ListResponse([Group(item) for item in response['items']],
                            response['metadata'])

    def create(self, group_name, role, ldap_group_dn=None):
        data = {
            'group_name': group_name,
            'ldap_group_dn': ldap_group_dn,
            'role': role
        }
        response = self.api.post('/user-groups',
                                 data=data,
                                 expected_status_code=201)
        return Group(response)

    def get(self, group_name, **kwargs):
        response = self.api.get(
            '/user-groups/{0}'.format(group_name),
            params=kwargs
        )
        return Group(response)

    def delete(self, group_name):
        self.api.delete('/user-groups/{0}'.format(group_name))

    def set_role(self, group_name, new_role):
        data = {'role': new_role}
        response = self.api.post('/user-groups/{0}'.format(group_name),
                                 data=data)
        return Group(response)

    def add_user(self, username, group_name):
        data = {'username': username, 'group_name': group_name}
        response = self.api.put('/user-groups/users', data=data)
        return Group(response)

    def remove_user(self, username, group_name):
        data = {'username': username, 'group_name': group_name}
        self.api.delete('/user-groups/users', data=data)

    def dump(self, output_dir,
             entities_per_file=constants.DUMP_ENTITIES_PER_FILE):
        data = utils.get_all(
                self.api.get,
                '/user-groups',
                params={'_get_data': True},
                _include=['name', 'ldap_dn', 'tenants', 'role']
        )
        return utils.dump_all('user_groups', data, entities_per_file,
                              output_dir)
