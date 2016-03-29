########
# Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
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


class Maintenance(dict):

    def __init__(self, maintenance_state):
        self.update(maintenance_state)

    @property
    def status(self):
        """
        :return: maintenance mode's status (activated, activating, deactivated)
        """
        return self.get('status')


class MaintenanceModeClient(object):

    def __init__(self, api):
        self.api = api

    def status(self):
        """

        :return: Maintenance mode state.
        """
        uri = '/maintenance'
        response = self.api.get(uri)
        return Maintenance(response)

    def activate(self):
        """
        Activates maintenance mode.

        :return: Maintenance mode state.
        """
        uri = '/maintenance/activate'
        response = self.api.post(uri)
        return Maintenance(response)

    def deactivate(self):
        """
        Deactivates maintenance mode.

        :return: Maintenance mode state.
        """
        uri = '/maintenance/deactivate'
        response = self.api.post(uri)
        return Maintenance(response)
