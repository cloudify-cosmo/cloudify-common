from cloudify_rest_client.responses import ListResponse

from .deployments import Deployment


class EnvironmentsClient(object):

    def __init__(self, api):
        self.api = api

    def list(self, sort=None, is_descending=False, **kwargs):
        response = self.api.get(
            '/deployments',
            params={'_all_tenants': True, '_environments_only': True}
        )
        return ListResponse(
            [Deployment(item) for item in response['items']],
            response['metadata'])

    def count(self, **kwargs):
        response = self.api.get(
            '/deployments',
            params={'_all_tenants': True,
                    '_environments_only': True,
                    '_get_all_results': True}
        )
        return len(response['items'])
