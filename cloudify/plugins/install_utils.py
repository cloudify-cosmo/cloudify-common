#########
# Copyright (c) 2019 Cloudify Technologies Ltd. All rights reserved
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
#

INSTALLING_PREFIX = 'installing-'


def remove_status_prefix(plugin):
    """
    Removes the `installing-` prefix from the plugin's archive_name.
    """
    if plugin.archive_name.startswith(INSTALLING_PREFIX):
        plugin.archive_name = plugin.archive_name[11:]
        return plugin


def is_plugin_installing(new_plugin, plugin):
    """
    Plugins that are currently being installed have an `installing-` prefix.
    """
    return plugin.archive_name == '{0}{1}'.format(INSTALLING_PREFIX,
                                                  new_plugin.archive_name)
