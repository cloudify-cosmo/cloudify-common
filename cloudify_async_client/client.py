import json
import itertools
import logging
import numbers
import ssl
import types

import aiohttp.client_exceptions

from cloudify_rest_client import exceptions
from cloudify_rest_client.client import HTTPClientBase, CloudifyClient

from cloudify_async_client.audit_log import AuditLogAsyncClient


class AsyncHTTPClient(HTTPClientBase):
    def __init__(self, *args, **kwargs):
        session = kwargs.pop('session', None)
        timeout = kwargs.pop('timeout', None)
        super().__init__(*args, **kwargs)
        # can't use base class' timeout because it's a tuple there, and
        # aiohttp needs a single int
        self.default_timeout_sec = timeout or 5

        self._aiohttp = None
        if session is None:
            session = self.aiohttp.ClientSession(headers=self.headers)
        self._session = session

        if self.trust_all:
            self.ssl = ssl.create_default_context()
            self.ssl.check_hostname = False
            self.ssl.verify_mode = ssl.CERT_NONE
        else:
            self.ssl = ssl.create_default_context(cafile=self.cert)

    @property
    def aiohttp(self):
        if self._aiohttp is None:
            import aiohttp
            self._aiohttp = aiohttp
        return self._aiohttp

    async def do_request(
        self,
        method,
        uri,
        data,
        params,
        headers,
        expected_status_code,
        stream,
        verify,
        timeout,
        wrapper,
        versioned_url=True,
    ):
        session_method = getattr(self._session, method.lower(), None)
        if session_method is None:
            raise RuntimeError(f'Unknown method: {method}')

        copied_data = None
        if isinstance(data, types.GeneratorType):
            copied_data = itertools.tee(data, self.retries)
        elif isinstance(data, dict):
            data = json.dumps(data)

        errors = {}
        for retry in range(self.retries):
            manager_to_try = self.get_host()
            request_url = self.get_request_url(
                manager_to_try,
                uri,
                versioned=versioned_url,
            )
            if copied_data is not None:
                data = copied_data[retry]
            try:
                response = await session_method(
                    request_url,
                    data=data,
                    params=params,
                    headers=headers,
                    ssl=self.ssl,
                    timeout=timeout or self.default_timeout_sec,
                    auth=self.auth,
                )
                return await self.process_response(
                    response,
                    expected_status_code,
                    stream,
                    wrapper,
                )

            except aiohttp.client_exceptions.ClientSSLError as e:
                errors[manager_to_try] = exceptions.format_ssl_error(e)
                self.logger.debug(
                    'HTTP Client error: %s %s: %s', method, uri, e)
                continue
            except aiohttp.client_exceptions.ClientConnectionError as e:
                errors[manager_to_try] = exceptions.format_connection_error(e)
                self.logger.debug(
                    'HTTP Client error: %s %s: %s', method, uri, e)
                continue
            except exceptions.CloudifyClientError as e:
                self.logger.debug(
                    'HTTP Client error: %s %s: %s', method, uri, e)
                errors[manager_to_try] = e.status_code
                if e.status_code == 502:
                    continue
                if e.status_code == 404 and \
                        self._is_fileserver_download(e.response):
                    continue
                else:
                    raise

        mgr_errors = ', '.join(f'{host}: {e}' for host, e in errors.items())
        raise exceptions.CloudifyClientError(
            f'HTTP Client error: {method} {uri} ({mgr_errors})')

    async def process_response(
        self,
        response,
        expected_status_code,
        stream,
        wrapper
    ):
        if self.logger.isEnabledFor(logging.DEBUG):
            for hdr, hdr_content in response.request_info.headers.items():
                self.logger.debug('request header:  %s: %s', hdr, hdr_content)
            self.logger.debug('reply:  "%s %s" %s', response.status,
                              response.reason, response.content)
            for hdr, hdr_content in response.headers.items():
                self.logger.debug('response header:  %s: %s', hdr, hdr_content)

        if isinstance(expected_status_code, numbers.Number):
            expected_status_code = [expected_status_code]
        if response.status not in expected_status_code:
            raise exceptions.CloudifyClientError.from_response(
                response, response.status, response.request_info.url)

        if response.status == 204:
            return None

        if stream:
            return response

        response_json = await response.json()

        if response.history:
            response_json['history'] = response.history

        if wrapper:
            return wrapper(response_json)
        await response.close()

        return response_json

    async def close(self):
        await self._session.close()


class AsyncCloudifyClient(CloudifyClient):
    client_class = AsyncHTTPClient

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.auditlog = AuditLogAsyncClient(self._client)

    @property
    def community_contacts(self):
        raise RuntimeError('async client does not support community_contacts')

    @property
    def deployment_groups(self):
        raise RuntimeError('async client does not support deployment_groups')

    @property
    def log_bundles(self):
        raise RuntimeError('async client does not support log_bundles')

    def close(self):
        return self._client.close()
