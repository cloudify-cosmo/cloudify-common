import ssl


class CloudifyAsyncClient:
    host: str
    port: int
    protocol: str
    headers: dict
    cert: str

    def __init__(self, **kwargs):
        # only import asyncio and aiohttp if this is used
        # otherwise we pay the price
        # on every import of the rest-client
        import asyncio
        import aiohttp
        self._aiohttp = aiohttp
        self._asyncio = asyncio

        self.host = kwargs.pop('host', 'localhost')
        self.port = kwargs.pop('port', 443)
        self.protocol = kwargs.pop('protocol', 'https')
        self.headers = kwargs.pop('headers', {})
        self.cert = kwargs.pop('cert')
        self.ssl = ssl.create_default_context(cafile=self.cert)
        self.api_version = 'v3.1'
        self._session = None

    @property
    def url(self):
        return '{0}://{1}:{2}/api/{3}'.format(self.protocol, self.host,
                                              self.port, self.api_version)

    @property
    def session(self):
        if not self._session:
            self._session = self._aiohttp.ClientSession(
                headers=self.headers,
                loop=self._asyncio.get_event_loop(),
            )
        return self._session

    async def close_session(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def get(self, url, params=None, timeout=300, **kwargs):
        # only import aiohttp if this is used - otherwise we pay the price
        # on every import of the rest-client
        if isinstance(timeout, int) or isinstance(timeout, float):
            timeout = self._aiohttp.ClientTimeout(total=timeout)

        if params:
            # Format query parameters and pass params only if it is not empty
            p = {k: str(v) for k, v in params.items() if v is not None}
            if p:
                kwargs['params'] = p

        response = await self.session.get(
            f"{self.url}/{url}",
            ssl=self.ssl,
            timeout=timeout,
            **kwargs
        )
        return response
