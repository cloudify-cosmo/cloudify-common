########
# Copyright (c) 2017-2019 Cloudify Platform Ltd. All rights reserved
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

import types
import random
import requests
import itertools


from cloudify_rest_client import CloudifyClient
from cloudify_rest_client.client import HTTPClient
from cloudify_rest_client.exceptions import CloudifyClientError


class ClusterHTTPClient(HTTPClient):

    def __init__(self, *args, **kwargs):
        super(ClusterHTTPClient, self).__init__(*args, **kwargs)
        # from outside, we get self.host passed in as a list (optionally).
        # But we still need self.host to be the currently-used manager,
        # and we can store the list as self.hosts
        hosts = self.host if isinstance(self.host, list) else [self.host]
        random.shuffle(hosts)
        self.hosts = itertools.cycle(hosts)
        self.host = hosts[0]
        self.default_timeout_sec = self.default_timeout_sec or (5, None)
        self.retries = 30
        self.retry_interval = 3

    def do_request(self, *args, **kwargs):
        kwargs.setdefault('timeout', self.default_timeout_sec)

        copied_data = None
        if isinstance(kwargs.get('data'), types.GeneratorType):
            copied_data = itertools.tee(kwargs.pop('data'), self.retries)

        for retry in range(self.retries):
            manager_to_try = next(self.hosts)
            self.host = manager_to_try
            if copied_data is not None:
                kwargs['data'] = copied_data[retry]

            try:
                return super(ClusterHTTPClient, self).do_request(*args,
                                                                 **kwargs)
            except (requests.exceptions.ConnectionError) as error:
                self.logger.debug(
                    'Connection error when trying to connect to '
                    'manager {0}'.format(error)
                )
                continue

        raise CloudifyClientError('No active node in the cluster!')


class CloudifyClusterClient(CloudifyClient):
    client_class = ClusterHTTPClient
