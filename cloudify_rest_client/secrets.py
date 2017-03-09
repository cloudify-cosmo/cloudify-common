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


class Secret(dict):

    def __init__(self, secret):
        super(Secret, self).__init__()
        self.update(secret)

    @property
    def key(self):
        """
        :return: The key of the secret.
        """
        return self.get('key')

    @property
    def value(self):
        """
        :return: The value of the secret.
        """
        return self.get('value')

    @property
    def created_at(self):
        """
        :return: Secret creation date.
        """
        return self.get('created_at')

    @property
    def updated_at(self):
        """
        :return: Secret modification date.
        """
        return self.get('updated_at')


class SecretsClient(object):

    def __init__(self, api):
        self.api = api

    def create(self, key, value):
        data = {'value': value}
        response = self.api.put('/secrets/{0}'.format(key), data=data)

        return Secret(response)

    def update(self, key, value):
        data = {'value': value}
        response = self.api.patch('/secrets/{0}'.format(key), data=data)

        return Secret(response)

    def get(self, key):
        response = self.api.get('/secrets/{0}'.format(key))
        return Secret(response)
