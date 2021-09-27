import ssl

import aiohttp


class CloudifyAsyncClient:
    host: str
    port: int
    protocol: str
    headers: dict
    cert: str

    def __init__(self, **kwargs):
        self.host = kwargs.pop('host', 'localhost')
        self.port = kwargs.pop('port', 443)
        self.protocol = kwargs.pop('protocol', 'https')
        self.headers = kwargs.pop('headers', {})
        self.cert = kwargs.pop('cert')
        self.ssl = ssl.create_default_context(cafile=self.cert)
        self.api_version = 'v3.1'
        self.session = aiohttp.ClientSession(headers=self.headers)

    @property
    def url(self):
        return '{0}://{1}:{2}/api/{3}'.format(self.protocol, self.host,
                                              self.port, self.api_version)

    def get(self, url, **kwargs):
        timeout = kwargs.pop('timeout', 300)
        if timeout is None:
            timeout = 300
        if isinstance(timeout, int) or isinstance(timeout, float):
            timeout = aiohttp.ClientTimeout(total=timeout)

        # Format query parameters
        params = kwargs.copy()
        p = {k: str(v) for k, v in params.pop('params', {}).items()
             if v is not None}
        if p:
            params['params'] = p

        return self.session.get(f"{self.url}/{url}",
                                ssl=self.ssl,
                                timeout=timeout,
                                **params)
