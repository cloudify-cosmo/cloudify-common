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


import random
import requests

from cloudify._compat import urlquote
from cloudify.utils import ipv6_url_compat


RABBITMQ_MANAGEMENT_PORT = 15671
USERNAME_PATTERN = 'rabbitmq_user_{0}'


class RabbitMQClient(object):
    def __init__(self, hosts, username, password,
                 port=RABBITMQ_MANAGEMENT_PORT, scheme='https',
                 logger=None, **request_kwargs):
        self._hosts = list(hosts) if isinstance(hosts, list) else [hosts]
        self._hosts = [ipv6_url_compat(h) for h in self._hosts]
        random.shuffle(self._hosts)
        self._target_host = self._hosts.pop()
        self._port = port
        self._scheme = scheme
        self._logger = logger
        request_kwargs.setdefault('auth', (username, password))
        self._request_kwargs = request_kwargs

    @property
    def base_url(self):
        return '{0}://{1}:{2}'.format(
            self._scheme,
            self._target_host,
            self._port,
        )

    def _do_request(self, request_method, url, **kwargs):
        request_kwargs = self._request_kwargs.copy()
        request_kwargs.update(kwargs)
        request_kwargs.setdefault('headers', {})\
            .setdefault('Content-Type', 'application/json',)

        while True:
            full_url = '{0}/api/{1}'.format(self.base_url, url)
            try:
                response = request_method(full_url, **request_kwargs)
                response.raise_for_status()
                # Successful call, return the results
                break
            except Exception as err:
                base_message = (
                    'Failed making request to rabbitmq {url}. '
                    'Error was: {err_type}- {err_msg}. '.format(
                        url=full_url,
                        err_type=type(err),
                        err_msg=str(err),
                    )
                )
                if len(self._hosts):
                    if self._logger:
                        self._logger.warning(
                            base_message + 'Trying next host.'
                        )
                    self._target_host = self._hosts.pop()
                    continue
                else:
                    # We tried all hosts, and nothing worked
                    if self._logger:
                        self._logger.error(
                            base_message + 'No healthy hosts found.'
                        )
                    raise
        return response

    def get_vhost_names(self):
        vhosts = self._do_request(requests.get, 'vhosts').json()
        return [vhost['name'] for vhost in vhosts]

    def create_vhost(self, vhost, copy_policies=True):
        vhost = urlquote(vhost, '')
        self._do_request(requests.put, 'vhosts/{0}'.format(vhost))
        if copy_policies:
            default_policies = self.get_policies('/')
            for policy in default_policies:
                name = policy.pop('name')
                policy.pop('vhost')
                self.set_policy(vhost, name, policy)

    def set_policy(self, vhost, policy_name, policy):
        vhost = urlquote(vhost, '')
        policy_name = urlquote(policy_name, '')
        self._do_request(
            requests.put,
            'policies/{vhost}/{policy_name}'.format(
                vhost=vhost,
                policy_name=policy_name,
            ),
            json=policy,
        )

    def get_policies(self, vhost):
        vhost = urlquote(vhost, '')
        return self._do_request(
            requests.get,
            'policies/{vhost}'.format(vhost=vhost)
        ).json()

    def delete_vhost(self, vhost):
        vhost = urlquote(vhost, '')
        self._do_request(requests.delete, 'vhosts/{0}'.format(vhost))

    def get_users(self):
        return self._do_request(requests.get, 'users').json()

    def create_user(self, username, password, tags=''):
        self._do_request(requests.put, 'users/{0}'.format(username),
                         json={'password': password, 'tags': tags})

    def delete_user(self, username):
        self._do_request(requests.delete, 'users/{0}'.format(username))

    def delete_queue(self, vhost, queue):
        self._do_request(
            requests.delete, 'queues/{0}/{1}'.format(vhost, queue))

    def delete_exchange(self, vhost, exchange):
        self._do_request(
            requests.delete, 'exchanges/{0}/{1}'.format(vhost, exchange))

    def set_vhost_permissions(self, vhost, username, configure='', write='',
                              read=''):
        vhost = urlquote(vhost, '')
        self._do_request(requests.put,
                         'permissions/{0}/{1}'.format(vhost, username),
                         json={
                             'configure': configure,
                             'write': write,
                             'read': read
                         })
