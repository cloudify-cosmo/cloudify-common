########
# Copyright © 2024 Dell Inc. or its subsidiaries. All Rights Reserved.

import itertools
import random
import re
import requests
import time
import types

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
        self.default_timeout_sec = self.default_timeout_sec or (5, 300)

    def do_request(self, method, url, *args, **kwargs):
        retry_interval = 1
        start_time = time.monotonic()
        kwargs.setdefault('timeout', self.default_timeout_sec)

        # if the data is a generator, unforunately we need to load it all into
        # memory in order to support retries, because if we do have to retry,
        # but we've already exhausted the generator, it's already too late.
        # Note: previous versions of this function attempted to save memory
        # using itertools.tee, but this is futile, because by the time all of
        # the data has been sent, it is already all stored in memory by tee
        # anyway, so we might as well attempt to do so up front.
        buffered_data = None
        if isinstance(kwargs.get('data'), types.GeneratorType):
            buffered_data = list(kwargs.pop('data'))

        while True:
            if buffered_data is not None:
                # if we have generator data loaded, make it an iterator,
                # because requests expects that, and not a list
                kwargs['data'] = iter(buffered_data)

            try:
                return super().do_request(method, url, *args, **kwargs)
            except Exception as e:
                do_retry = self._should_retry_request(
                    e, start_time, self.logger)

                if do_retry:
                    # we'll try the next host. If there's only one host, it's
                    # going to be the same one (self.hosts is a cycle())
                    self.host = next(self.hosts)

                    # exponential backoff of retries: increase delay by 50%
                    # each time, but only up to 30 seconds
                    time.sleep(retry_interval)
                    retry_interval = min(retry_interval * 1.5, 30)
                    continue
                else:
                    # we aren't going to retry - break out of the loop,
                    # allow the error to be raised
                    raise

        # should never reach here: the loop above should exit by either
        # a `return` or a `raise`. If we did reach here, it means the loop
        # exited in some other way
        raise RuntimeError('HTTP Client: unexpected error')

    def _should_retry_request(self, error, start_time, logger):
        elapsed = time.monotonic() - start_time

        if elapsed > 3600:
            # we've been trying this request for more than an hour? whatever it
            # was, it's really enough...
            logger.debug('Not retrying anymore after over 1 hour: %s', error)
            return False

        if isinstance(error, CloudifyClientError):
            response = error.response

            if (
                # fileserver downloads need to be retried, because we might be
                # running a cluster that needs to wait for files to be
                # replicated, and that might take quite some time
                response.status_code == 404
                and self._is_fileserver_download(response)
                # ...but surely not more than five minutes. Normally it takes
                # on the order of up to 10 seconds
                and elapsed < 300
            ):
                logger.debug('Retrying fileserver download: %s', error)
                return True

            if (
                # a 500 is oftentimes a development error, but we can hope it's
                # just a fluke - let's retry it and see
                response.status_code == 500
                # ...but only for a minute. Normally 500 errors are not going
                # to persist long
                and elapsed < 60
            ):
                logger.debug('Retrying after an error response: %s', error)
                return True

            if (
                # this is a 502 or a 503 (backend unavailable or timeout) and
                # those tend to be intermittent and only happen for some time,
                # e.g. when the service is being restarted
                response.status_code > 500
                # ...so we should retry these for a long time,
                # even up to an hour
                and elapsed < 3600
            ):
                logger.debug(
                    'Retrying after an unavailable response: %s', error)
                return True

            if (
                # 403 is normally an authorization error, but it might happen
                # when authorization backends are being restarted, or crashing,
                # so let's retry those a few times
                response.status_code == 403
                # ...only up to a minute, because there's little chance
                # retrying this will do any good
                and elapsed < 60
            ):
                logger.debug('Retrying on an unauthorized response: %s', error)
                return True

        elif isinstance(error, requests.exceptions.ConnectionError):
            logger.debug(
                'Connection error when trying to connect to manager: %s', error
            )
            if elapsed < 3600:
                # a connection error? the server is unavailable - perhaps it is
                # being restarted, or the user brough it down temporarily?
                # let's make sure to retry this, because this is usually
                # something very temporary
                return True

        # most likely not a connection error, but rather something
        # like a 409, 401, or a similar _real_ error. Don't retry these.
        return False

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
