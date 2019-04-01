#########
# Copyright (c) 2019 Cloudify Platform Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

from cloudify_rest_client.responses import ListResponse


class PluginsUpdate(dict):

    def __init__(self, update):
        self.update(update)

    @property
    def id(self):
        return self['id']

    @property
    def state(self):
        return self['state']

    @property
    def blueprint_id(self):
        return self['blueprint_id']

    @property
    def temp_blueprint_id(self):
        return self['temp_blueprint_id']

    @property
    def execution_id(self):
        return self['execution_id']

    @property
    def deployments_to_update(self):
        return self['deployments_to_update']

    @property
    def created_at(self):
        return self['created_at']

    @property
    def forced(self):
        return self['forced']


class PluginsUpdateClient(object):
    """
    Cloudify's plugins update management client.
    """

    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'plugins-updates'
        self._wrapper_cls = PluginsUpdate

    def get(self, plugins_update_id, _include=None, **kwargs):
        """
        Gets a plugins update by its id.

        :param plugins_update_id: PluginsUpdate's id to get.
        :param _include: List of fields to include in response.
        :return: The plugins update details.
        """
        assert plugins_update_id
        uri = '/{self._uri_prefix}/{id}'.format(
            self=self, id=plugins_update_id)
        response = self.api.get(uri, _include=_include, params=kwargs)
        return self._wrapper_cls(response)

    def _wrap_list(self, response):
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of available plugins updates.
        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.PluginsUpdate.fields
        :return: Plugins list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                _include=_include,
                                params=params)
        return self._wrap_list(response)

    def update_plugins(self, blueprint_id, force=False):
        """
        Updates the plugins in all the deployments that use the given
        blueprint.

        :param blueprint_id: blueprint ID to perform the update with.
        :param force: if to forcefully update when other non-active plugins
         updates exists associated with this blueprint.
        :return: a PluginUpdate object.
        """
        params = {
            'force': force
        }
        response = self.api.post(
            '/{self._uri_prefix}/{}/update/initiate'.format(blueprint_id,
                                                            self=self),
            params=params
        )
        return PluginsUpdate(response)

    def finalize_plugins_update(self, plugins_update_id):
        """
        Finalize the plugins update (for internal use).

        :return: a PluginUpdate object.
        """
        response = self.api.post(
            '/{self._uri_prefix}/{}/update/finalize'.format(plugins_update_id,
                                                            self=self)
        )
        return PluginsUpdate(response)
