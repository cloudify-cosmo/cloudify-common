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


class ConfigItem(dict):
    """A configuration entry"""
    def __init__(self, config):
        super(ConfigItem, self).__init__()
        self.update(config)

    @property
    def name(self):
        """Name of the configuration entry"""
        return self.get('name')

    @property
    def value(self):
        """The setting value"""
        return self.get('value')

    @property
    def schema(self):
        """JSON schema of the configuration value, if any.

        When changing the setting, the new value must conform to this schema.
        """
        return self.get('schema')

    @property
    def scope(self):
        """Components affected by this configuration entry.

        Eg. mgmtworker, rest, or agents
        """
        return self.get('scope')

    @property
    def updater_name(self):
        """Name of the user who last changed this value"""
        return self.get('updater_name')

    @property
    def updated_at(self):
        """Time this value was last changed at"""
        return self.get('updated_at')

    @property
    def is_editable(self):
        """Whether or not it is possible to change this setting.

        Settings that are not marked editable can still be changed by
        setting force=True, however that might lead to unexpected results.
        Use with caution.
        """
        return self.get('is_editable')


class ManagerItem(dict):
    """A manager entry"""
    def __init__(self, manager):
        super(ManagerItem, self).__init__()
        self.update(manager)

    @property
    def id(self):
        """
        Manager's ID in the DB (unique, incremental)
        type: int
        """
        return self.get('id')

    @property
    def hostname(self):
        """
        Manager's hostname (unique)
        type: string
        """
        return self.get('name')

    @property
    def private_ip(self):
        """
        Manager's private IP
        type: string
        """
        return self.get('private_ip')

    @property
    def public_ip(self):
        """
        Manager's public IP
        type: string
        """
        return self.get('public_ip')

    @property
    def version(self):
        """
        Manager's version
        type: string
        """
        return self.get('version')

    @property
    def edition(self):
        """
        Manager's edition
        type: string
        """
        return self.get('edition')

    @property
    def distribution(self):
        """
        Manager's distribution
        type: string
        """
        return self.get('distribution')

    @property
    def distro_release(self):
        """
        Manager's distribution release
        type: string
        """
        return self.get('distro_release')

    @property
    def fs_sync_node_id(self):
        """
        Manager's FS sync node id - used by Syncthing replication
        type: string
        """
        return self.get('fs_sync_node_id')


class ManagerClient(object):

    def __init__(self, api):
        self.api = api

    def get_status(self):
        """
        :return: Cloudify's management machine status.
        """
        response = self.api.get('/status')
        return response

    def get_config(self, name=None, scope=None):
        """Get configuration of the manager.

        If name is provided, only return that single value. If scope is
        provided, return all values for that scope.
        """
        if name and scope:
            raise ValueError('Pass either name or scope, not both')
        if name:
            response = self.api.get('/config/{0}'.format(name))
            return ConfigItem(response)

        if scope:
            response = self.api.get('/config', params={'scope': scope})
        else:
            response = self.api.get('/config')
        return ListResponse([ConfigItem(item) for item in response['items']],
                            response['metadata'])

    def put_config(self, name, value, force=False):
        """Update a given setting.

        Note that the new value must conform to the schema, if any.

        :param force: Force changing non-editable settings
        """
        response = self.api.put('/config/{0}'.format(name), data={
            'value': value,
            'force': force
        })
        return ConfigItem(response)

    def add_manager(self, hostname, private_ip, public_ip, version,
                    edition, distribution, distro_release, fs_sync_node_id=''):
        """
        Add a new manager to the managers table
        """
        manager = {
            'hostname': hostname,
            'private_ip': private_ip,
            'public_ip': public_ip,
            'version': version,
            'edition': edition,
            'distribution': distribution,
            'distro_release': distro_release
        }
        if fs_sync_node_id:
            manager['fs_sync_node_id'] = fs_sync_node_id
        response = self.api.post('/managers', data=manager)
        return ManagerItem(response)

    def remove_manager(self, hostname):
        """
        Remove a manager from the managers table

        Will be used for clustering when a manager needs to be removed from
        the cluster, not necessarily for uninstalling the manager
        :param hostname: The manager's hostname
        """
        response = self.api.delete('/managers', data={
            'hostname': hostname
        })
        return ManagerItem(response)

    def update_manager(self, hostname, fs_sync_node_id):
        """
        Updating a manager's FS sync node id used by Syncthing replication

        :param hostname: hostname of the manager to update
        :param fs_sync_node_id: Syncthing node ID
        """
        response = self.api.put('/managers', data={
            'hostname': hostname,
            'fs_sync_node_id': fs_sync_node_id
        })
        return ManagerItem(response)

    def get_managers(self, hostname=None, _include=None):
        """
        Get all the managers in the managers table or
        Get a specific manager based on 'hostname'
        :param hostname: hostname of manager to return
        :param _include: list of columns to include in the returned list
        """
        if hostname:
            response = self.api.get('/managers', params={'hostname': hostname},
                                    _include=_include)
        else:
            response = self.api.get('/managers', _include=_include)
        return ListResponse(
            [ManagerItem(item) for item in response['items']],
            response['metadata']
        )

    def get_version(self):
        """
        :return: Cloudify's management machine version information.
        """
        response = self.api.get('/version', versioned_url=False)
        return response

    def ssl_status(self):
        """
        Get manager's ssl state (enabled/disabled)
        """
        try:
            response = self.api.get('/ssl')
        except TypeError as e:
            if "'unicode' object does not support item assignment" \
                    in e.message:
                raise Exception('Manager is working with SSL, '
                                'but your local client is not')
            else:
                raise
        return response

    def set_ssl(self, state):
        """
        Set manager's ssl to state (true/false)
        """
        data = {'state': state}
        response = self.api.post('/ssl', data)
        return response

    def get_context(self, _include=None):
        """
        Gets the context which was stored on management machine bootstrap.
        The context contains Cloudify specific information and Cloud provider
        specific information.

        :param _include: List of fields to include in response.
        :return: Context stored in manager.
        """
        response = self.api.get('/provider/context', _include=_include)
        return response

    def create_context(self, name, context):
        """
        Creates context in Cloudify's management machine.
        This method is usually invoked right after management machine
        bootstrap with relevant Cloudify and cloud provider
        context information.

        :param name: Cloud provider name.
        :param context: Context as dict.
        :return: Create context result.
        """
        data = {'name': name, 'context': context}
        response = self.api.post('/provider/context',
                                 data,
                                 expected_status_code=201)
        return response

    def update_context(self, name, context):

        """
        Updates context in Cloudify's management machine.
        The context is imperative for the manager to function properly,
        only use this method if you know exactly what you are doing.
        Note that if the provider context does not exist, this call will
        result with an error.

        :param name: Cloud provider name.
        :param context: Context as dict.

        """

        data = {'name': name, 'context': context}
        response = self.api.post('/provider/context', data,
                                 expected_status_code=200,
                                 params={'update': 'true'})
        return response
