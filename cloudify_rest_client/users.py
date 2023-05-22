from cloudify_rest_client import utils
from cloudify_rest_client.exceptions import CloudifyClientError
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
    def created_at(self):
        """
        :return: The user creation time.
        """
        return self.get('created_at')

    @property
    def last_login_at(self):
        """
        :return: The last time the user logged in.
        """
        return self.get('last_login_at')

    @property
    def first_login_at(self):
        """
        :return: The first time the user logged in.
        """
        return self.get('first_login_at')

    @property
    def is_locked(self):
        """
        :return: Whether a user is locked or not.
        """
        return self.get('is_locked')


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

    def create(self, username, password, role, is_prehashed=None,
               created_at=None, first_login_at=None, last_login_at=None):
        data = {'username': username, 'password': password, 'role': role}
        if is_prehashed is not None:
            data['is_prehashed'] = is_prehashed
        if created_at:
            data['created_at'] = created_at
        if first_login_at:
            data['first_login_at'] = first_login_at
        if last_login_at:
            data['last_login_at'] = last_login_at
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

    def set_show_getting_started(self, username, flag_value):
        data = {'show_getting_started': flag_value}
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
        self.api.delete('/users/{0}'.format(username))

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

    def unlock(self, username, **kwargs):
        response = self.api.post(
            '/users/unlock/{0}'.format(username),
            params=kwargs
        )
        return User(response)

    def dump(self):
        """Generate users' attributes for a snapshot.

        :returns: A generator of dictionaries, which describe users'
         attributes.
        """
        return utils.get_all(
                self.api.get,
                '/users',
                params={'_get_data': True, '_include_hash': True},
                _include=['username', 'role', 'tenant_roles',
                          'first_login_at', 'last_login_at', 'created_at'],
        )

    def restore(self, entities, logger):
        """Restore users from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         users to be restored.
        :param logger: A logger instance.
        :returns: A generator of dictionaries, which describe additional data
         used for snapshot restore entities post-processing.
        """
        for entity in entities:
            if entity['username'] == 'admin':
                if logger:
                    logger.debug('Skipping creation of admin user')
                continue
            entity['password'] = entity.pop('password_hash')
            entity['is_prehashed'] = True
            tenant_roles = entity.pop('tenant_roles')
            try:
                self.create(**entity)
                yield {entity['username']: tenant_roles}
            except CloudifyClientError as exc:
                logger.error("Error restoring user "
                             f"{entity['username']}: {exc}")
