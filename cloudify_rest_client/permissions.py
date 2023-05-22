from cloudify_rest_client import utils
from cloudify_rest_client.executions import CloudifyClientError
from cloudify_rest_client.responses import ListResponse


class Permission(dict):
    def __init__(self, permission):
        super(Permission, self).__init__()
        self.update(permission)


class PermissionsClient(object):
    def __init__(self, api):
        self.api = api
        self._wrapper_cls = Permission
        self._uri_prefix = '/permissions'

    def list(self, role=None):
        """List permissions, for all roles, or for one role if specified.

        :param role: only list permissions for this role
        :return: permissions for the given role, or all roles
        """
        url = self._uri_prefix
        if role:
            url = '{0}/{1}'.format(url, role)
        response = self.api.get(url)
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def add(self, permission, role):
        """Allow role the specified permission

        :param permission: the permission name to allow
        :param role: the role name
        """
        self.api.put('{0}/{1}/{2}'.format(self._uri_prefix, role, permission))

    def delete(self, permission, role):
        """Disallow role the specified permission

        :param permission: the permission name to disallow
        :param role: the role name
        """
        self.api.delete(
            '{0}/{1}/{2}'.format(self._uri_prefix, role, permission))

    def dump(self):
        """Generate permissions' attributes for a snapshot.

        :returns: A generator of dictionaries, which describe permissions'
         attributes.
        """
        return utils.get_all(
            self.api.get,
            self._uri_prefix,
        )

    def restore(self, entities, logger):
        """Restore permissions from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         permissions to be restored.
        :param logger: A logger instance.
        """
        existing_perms = self.list()
        for entity in entities:
            if entity in existing_perms:
                if logger:
                    logger.debug('Skipping existing perm: %s', entity)
                continue
            try:
                self.add(**entity)
            except CloudifyClientError as exc:
                logger.error(f"Error restoring permission {entity}: {exc}")
