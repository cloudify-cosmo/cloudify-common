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

import re
import types
import random
import requests
import itertools

from cloudify.utils import ipv6_url_compat

from cloudify_rest_client import CloudifyClient
from cloudify_rest_client.client import HTTPClient
from cloudify_rest_client.exceptions import CloudifyClientError


class ClusterHTTPClient(HTTPClient):

    def __init__(self, host, *args, **kwargs):
        # from outside, we get host passed in as a list (optionally).
        # But we still need self.host to be the currently-used manager,
        # and we can store the list as self.hosts
        # (copy the list so that outside mutations don't affect us)
        hosts = list(host) if isinstance(host, list) else [host]
        hosts = [ipv6_url_compat(h) for h in hosts]
        random.shuffle(hosts)
        self.hosts = itertools.cycle(hosts)
        super(ClusterHTTPClient, self).__init__(hosts[0], *args, **kwargs)
        self.default_timeout_sec = self.default_timeout_sec or (5, None)
        self.retries = 30
        self.retry_interval = 3

    def do_request(self, method, url, *args, **kwargs):
        kwargs.setdefault('timeout', self.default_timeout_sec)

        copied_data = None
        if isinstance(kwargs.get('data'), types.GeneratorType):
            copied_data = itertools.tee(kwargs.pop('data'), self.retries)

        errors = {}
        for retry in range(self.retries):
            manager_to_try = next(self.hosts)
            self.host = manager_to_try
            if copied_data is not None:
                kwargs['data'] = copied_data[retry]

            try:
                return super(ClusterHTTPClient, self).do_request(
                    method, url, *args, **kwargs)
            except (requests.exceptions.ConnectionError) as error:
                self.logger.debug(
                    'Connection error when trying to connect to '
                    'manager {0}'.format(error)
                )
                errors[manager_to_try] = error
                continue
            except CloudifyClientError as e:
                errors[manager_to_try] = e.status_code
                if e.response.status_code == 502:
                    continue
                if e.response.status_code == 404 and \
                        self._is_fileserver_download(e.response):
                    continue
                else:
                    raise

        raise CloudifyClientError(
            'HTTP Client error: {0} {1} ({2})'.format(
                method.__name__.upper(),
                url,
                ', '.join(
                    '{0}: {1}'.format(host, e) for host, e in errors.items()
                )
            ))

    def _is_fileserver_download(self, response):
        """Is this response a file-download response?

        404 responses to requests that download files, need to be retried
        with all managers in the cluster: if some file was not yet
        replicated, another manager might have this file.

        This is because the file replication is asynchronous.
        """
        if re.search('/(blueprints|snapshots)/', response.url):
            return True
        disposition = response.headers.get('Content-Disposition')
        if not disposition:
            return False
        return disposition.strip().startswith('attachment')


class CloudifyClusterClient(CloudifyClient):
    client_class = ClusterHTTPClient
