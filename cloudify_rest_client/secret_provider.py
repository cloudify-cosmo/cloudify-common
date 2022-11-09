from cloudify_rest_client.responses import ListResponse
from cloudify_rest_client.constants import VisibilityState


class SecretProvider(dict):
    def __init__(self, secret):
        super(SecretProvider, self).__init__()
        self.update(secret)

    @property
    def name(self):
        return self.get('name')

    @property
    def type(self):
        return self.get('type')

    @property
    def connection_parameters(self):
        return self.get('connection_parameters')

    @property
    def created_at(self):
        return self.get('created_at')

    @property
    def updated_at(self):
        return self.get('updated_at')

    @property
    def visibility(self):
        return self.get('visibility')

    @property
    def tenant_name(self):
        return self.get('tenant_name')


class SecretProviderClient(object):
    def __init__(self, api):
        self.api = api

    def create(
            self,
            secret_provider_name,
            secret_provider_type,
            connection_parameters,
            tenant_name,
            visibility=VisibilityState.TENANT,
    ):
        data = {
            'name': secret_provider_name,
            'type': secret_provider_type,
            'connection_parameters': connection_parameters,
            'tenant_name': tenant_name,
            'visibility': visibility,
        }
        response = self.api.put('/secret-provider', data=data)

        return SecretProvider(response)

    def list(self):
        response = self.api.get('/secret-provider')

        return ListResponse(
            [SecretProvider(item) for item in response['items']],
            response['metadata'],
        )
