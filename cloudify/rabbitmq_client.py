########
# Copyright (c) 2018 Cloudify Platform Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
############


import requests
from urllib import quote


RABBITMQ_MANAGEMENT_PORT = 15671
USERNAME_PATTERN = 'rabbitmq_user_{0}'


class RabbitMQClient(object):
    def __init__(self, host, username, password, port=RABBITMQ_MANAGEMENT_PORT,
                 scheme='https', **request_kwargs):
        self._host = host
        self._port = port
        self._scheme = scheme
        request_kwargs.setdefault('auth', (username, password))
        self._request_kwargs = request_kwargs

    @property
    def base_url(self):
        return '{0}://{1}:{2}'.format(self._scheme, self._host, self._port)

    def _do_request(self, request_method, url, **kwargs):
        request_kwargs = self._request_kwargs.copy()
        request_kwargs.update(kwargs)
        request_kwargs.setdefault('headers', {})\
            .setdefault('Content-Type', 'application/json',)

        url = '{0}/api/{1}'.format(self.base_url, url)
        response = request_method(url, **request_kwargs)
        response.raise_for_status()
        return response

    def get_vhost_names(self):
        vhosts = self._do_request(requests.get, 'vhosts').json()
        return [vhost['name'] for vhost in vhosts]

    def create_vhost(self, vhost):
        vhost = quote(vhost, '')
        self._do_request(requests.put, 'vhosts/{0}'.format(vhost))

    def delete_vhost(self, vhost):
        vhost = quote(vhost, '')
        self._do_request(requests.delete, 'vhosts/{0}'.format(vhost))

    def get_users(self):
        return self._do_request(requests.get, 'users').json()

    def create_user(self, username, password, tags=''):
        self._do_request(requests.put, 'users/{0}'.format(username),
                         json={'password': password, 'tags': tags})

    def delete_user(self, username):
        self._do_request(requests.delete, 'users/{0}'.format(username))

    def set_vhost_permissions(self, vhost, username, configure='', write='',
                              read=''):
        vhost = quote(vhost, '')
        self._do_request(requests.put,
                         'permissions/{0}/{1}'.format(vhost, username),
                         json={
                             'configure': configure,
                             'write': write,
                             'read': read
                         })
