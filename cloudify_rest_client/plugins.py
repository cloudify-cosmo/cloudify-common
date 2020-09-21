########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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

import os
import contextlib

from cloudify_rest_client import bytes_stream_utils
from cloudify_rest_client._compat import urlparse
from cloudify_rest_client.responses import ListResponse
from cloudify_rest_client.constants import VisibilityState


class Plugin(dict):
    """
    Cloudify plugin.
    """
    def __init__(self, plugin):
        super(Plugin, self).__init__()
        self.update(plugin)

    @property
    def id(self):
        """
        :return: The identifier of the plugin.
        """
        return self.get('id')

    @property
    def package_name(self):
        """
        :return: The plugin package name.
        """
        return self.get('package_name')

    @property
    def archive_name(self):
        """
        :return: The plugin archive name.
        """
        return self.get('archive_name')

    @property
    def package_source(self):
        """
        :return: The plugin source.
        """
        return self.get('package_source')

    @property
    def package_version(self):
        """
        :return: The package version.
        """
        return self.get('package_version')

    @property
    def supported_platform(self):
        """
        :return: The plugins supported platform.
        """
        return self.get('supported_platform')

    @property
    def distribution(self):
        """
        :return: The plugin compiled distribution.
        """
        return self.get('distribution')

    @property
    def distribution_version(self):
        """
        :return: The plugin compiled distribution version.
        """
        return self.get('distribution_version')

    @property
    def distribution_release(self):
        """
        :return: The plugin compiled distribution release.
        """
        return self.get('distribution_release')

    @property
    def wheels(self):
        """
        :return: The plugins included wheels.
        """
        return self.get('wheels')

    @property
    def excluded_wheels(self):
        """
        :return: The plugins excluded wheels.
        """
        return self.get('excluded_wheels')

    @property
    def supported_py_versions(self):
        """
        :return: The plugins supported python versions.
        """
        return self.get('supported_py_versions')

    @property
    def uploaded_at(self):
        """
        :return: The plugins upload time.
        """
        return self.get('uploaded_at')

    @property
    def created_by(self):
        """
        :return: The name of the plugin creator.
        """
        return self.get('created_by')

    @property
    def file_server_path(self):
        """
        :return: The path to the plugin.yaml file on the file server.
        """
        return self.get('file_server_path')

    @property
    def yaml_url_path(self):
        """
        :return: The virtual path from which the plugin.yaml file can be
        referenced in blueprints.
        """
        return self.get('yaml_url_path')

    @property
    def title(self):
        """
        :return: The title assigned to the plugin during upload.
        """
        return self.get('title')

    @property
    def tenant_name(self):
        """
        :return: Name of the tenant that owns this plugin.
        """
        return self.get('tenant_name')

    @property
    def installation_state(self):
        """Plugin installation state.

        The installation state is a dict containing the installation
        state details, eg.:
        {
            "managers": {
                "manager1": {
                    "state": "installed"
                }
            },
            "agents": {
                "agent1": {
                    "state": "failed",
                    "error": "error text here"
                }
            }
        }
        This means that the plugin is installed on manager1, and has
        failed installation on agent1, and installation has not been
        attempted on any other manager or agent.

        :return: Plugin installation state, with details per agent/manager,

        """
        return self.get('installation_state')


class PluginsClient(object):
    """
    Cloudify's plugin management client.
    """
    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'plugins'
        self._wrapper_cls = Plugin

    def get(self, plugin_id, _include=None, **kwargs):
        """
        Gets a plugin by its id.

        :param plugin_id: Plugin's id to get.
        :param _include: List of fields to include in response.
        :return: The plugin details.
        """
        assert plugin_id
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=plugin_id)
        response = self.api.get(uri, _include=_include, params=kwargs)
        return self._wrapper_cls(response)

    def _wrap_list(self, response):
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata']
        )

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of available plugins.
        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.Execution.fields
        :return: Plugins list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                _include=_include,
                                params=params)
        return self._wrap_list(response)

    def delete(self, plugin_id, force=False):
        """
        Deletes the plugin whose id matches the provided plugin id.
        :param plugin_id: The id of the plugin to be deleted.
        :param force: Delete plugin even if there is a deployment
                      currently using it.
        :return: Deleted plugin by its ID.
        """
        assert plugin_id
        data = {
            'force': force
        }
        self.api.delete('/plugins/{0}'.format(plugin_id), data=data)

    def upload(self,
               plugin_path,
               plugin_title=None,
               visibility=VisibilityState.TENANT,
               progress_callback=None):
        """Uploads a plugin archive to the manager

        :param plugin_path: Path to plugin archive.
        :param plugin_title: Plugin title to be used e.g. in UI for
                             presentation purposes in Topology widget.
        :param visibility: The visibility of the plugin, can be 'private',
                           'tenant' or 'global'
        :param progress_callback: Progress bar callback method
        :return: Plugin object
        """
        assert plugin_path
        query_params = {'visibility': visibility}
        if plugin_title:
            query_params['title'] = plugin_title
        timeout = self.api.default_timeout_sec
        if urlparse(plugin_path).scheme and not os.path.exists(plugin_path):
            query_params['plugin_archive_url'] = plugin_path
            data = None
            # if we have a timeout set, let's only use a connect timeout,
            # and skip the read timeout - this request can take a long
            # time before the server actually returns a response
            if timeout is not None and isinstance(timeout, (int, float)):
                timeout = (timeout, None)
        else:
            data = bytes_stream_utils.request_data_file_stream(
                plugin_path,
                progress_callback=progress_callback,
                client=self.api)

        response = self.api.post(
            '/{self._uri_prefix}'.format(self=self),
            params=query_params,
            data=data,
            timeout=timeout,
            expected_status_code=201
        )
        if 'metadata' in response and 'items' in response:
            # This is a list of plugins - for caravan
            return self._wrap_list(response)
        else:
            return self._wrapper_cls(response)

    def download(self, plugin_id, output_file, progress_callback=None):
        """Downloads a previously uploaded plugin archive from the manager

        :param plugin_id: The plugin ID of the plugin to be downloaded.
        :param output_file: The file path of the downloaded plugin file
        :param progress_callback: Callback function - can be used to print
        a progress bar
        :return: The file path of the downloaded plugin.
        """
        assert plugin_id
        uri = '/plugins/{0}/archive'.format(plugin_id)
        with contextlib.closing(self.api.get(uri, stream=True)) as response:
            output_file = bytes_stream_utils.write_response_stream_to_file(
                response, output_file, progress_callback=progress_callback)

            return output_file

    def set_global(self, plugin_id):
        """
        Updates the plugin's visibility to global

        :param plugin_id: Plugin's id to update.
        :return: The plugin.
        """
        data = {'visibility': VisibilityState.GLOBAL}
        return self.api.patch(
            '/plugins/{0}/set-visibility'.format(plugin_id),
            data=data
        )

    def set_visibility(self, plugin_id, visibility):
        """
        Updates the plugin's visibility

        :param plugin_id: Plugin's id to update.
        :param visibility: The visibility to update, should be 'tenant'
                           or 'global'.
        :return: The plugin.
        """
        data = {'visibility': visibility}
        return self.api.patch(
            '/plugins/{0}/set-visibility'.format(plugin_id),
            data=data
        )

    def install(self, plugin_id, managers=None, agents=None):
        """Force the plugin installation on the given managers and agents

        :param plugin_id: The id of the plugin to be installed.

        :return: Plugin representation, with updated installation_state
        """
        if not managers and not agents:
            raise RuntimeError(
                'Specify managers or agents to install the plugin on')
        data = {
            'action': 'install',
        }
        if managers:
            data['managers'] = managers
        if agents:
            data['agents'] = agents
        response = self.api.post('/plugins/{0}'.format(plugin_id), data=data)
        return Plugin(response)

    def set_state(self, plugin_id, state, agent_name=None,
                  manager_name=None, error=None):
        """Update plugin installation state.

        Set the plugin installation state to state, for the given agent_name
        or manager_name, optionally with an error text.
        """
        if agent_name and manager_name:
            raise RuntimeError('Specify agent_name OR manager_name, not both')
        data = {
            'state': state
        }
        if agent_name:
            data['agent'] = agent_name
        elif manager_name:
            data['manager'] = manager_name
        if error:
            data['error'] = error
        response = self.api.put('/plugins/{0}'.format(plugin_id), data=data)
        return Plugin(response)
