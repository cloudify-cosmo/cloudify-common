from cloudify_async_client import CloudifyAsyncClient
from cloudify_rest_client.audit_log import AuditLogClient


class AuditLogAsyncClient(AuditLogClient):
    _client = None

    async def __aenter__(self):
        self._client = await self.async_client()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def stream(self, timeout=None, **kwargs):
        """
        Returns a list of AuditLogs.

        :param timeout: How long should the client keep the connection
                        opened and listen to the streamed data.
        :param kwargs:  Optional filter fields, currently `since`,
                        `creator_name` and `execution_id` are supported.
        :return:        ``ListResponse`` with of ``AuditLog`` items and
                        response metadata.
        """
        if not self._client:
            self._client = await self.async_client()
        response = await self._client.get(
            'audit/stream',
            params=kwargs,
            timeout=timeout
        )
        return response

    async def close(self):
        await self._client.close_session()

    async def async_client(self):
        headers = self.api.headers.copy()
        headers.update({'Content-type': 'text/event-stream'})
        client = CloudifyAsyncClient(
            host=self.api.host,
            port=self.api.port,
            protocol=self.api.protocol,
            cert=self.api.cert,
            headers=headers,
        )
        return client
