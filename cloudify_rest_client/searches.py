from cloudify_rest_client.responses import ListResponse

from .blueprints import Blueprint
from .deployments import Deployment


class DeploymentsSearchClient(object):
    def __init__(self, api):
        self.api = api

    def list(self, **kwargs):
        response = _list(self.api, 'deployments', **kwargs)
        return ListResponse([Deployment(item) for item in response['items']],
                            response['metadata'])


class BlueprintsSearchClient(object):
    def __init__(self, api):
        self.api = api

    def list(self, **kwargs):
        response = _list(self.api, 'blueprints', **kwargs)
        return ListResponse([Blueprint(item) for item in response['items']],
                            response['metadata'])


def _list(api, searched_resource, **kwargs):
    data = {}
    for key in ['include', 'filter_rules', 'filter_id']:
        value = kwargs.get(key)
        if value:
            data[key] = value
    data['all_tenants'] = kwargs.get('all_tenants', False)
    sort = kwargs.get('sort')
    is_descending = kwargs.get('is_descending')
    if sort:
        order = 'desc' if is_descending else 'asc'
        data['order'] = [{'attribute': sort, 'sort': order}]
    size = kwargs.get('size')
    offset = kwargs.get('offset')
    if size and offset:
        data['pagination'] = {'size': size, 'offset': offset}

    return api.post('/searches/{}'.format(searched_resource), data=data)
