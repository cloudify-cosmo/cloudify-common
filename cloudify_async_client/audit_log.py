from cloudify_rest_client.audit_log import AuditLogClient


class AuditLogAsyncClient(AuditLogClient):
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
        response = await self.api.get(
            '/audit/stream',
            params=kwargs,
            timeout=timeout,
            stream=True,
        )
        return response
