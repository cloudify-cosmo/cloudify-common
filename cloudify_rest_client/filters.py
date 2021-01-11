import json

from cloudify_rest_client.responses import ListResponse
from cloudify_rest_client.constants import VisibilityState


class Filter(dict):
    def __init__(self, filter_obj):
        super(Filter, self).__init__()
        self.update(filter_obj)

    @property
    def id(self):
        return self.get('id')

    @property
    def value(self):
        return json.loads(self.get('value', {}))

    @property
    def labels_filter(self):
        return self.value.get('labels')

    @property
    def created_at(self):
        return self.get('created_at')

    @property
    def visibility(self):
        return self.get('visibility')

    @property
    def tenant_name(self):
        return self.get('tenant_name')


class FiltersClient(object):
    def __init__(self, api):
        self.api = api

    def create(self,
               filter_name,
               filter_rules,
               visibility=VisibilityState.TENANT):
        """Creates a new filter.

        Currently, this function only supports the creation of a labels filter

        :param filter_name: The filter name
        :param filter_rules: A list of filter rules. Filter rules must
               be one of: <key>=<value>, <key>=[<value1>,<value2>,...],
               <key>!=<value>, <key>!=[<value1>,<value2>,...], <key> is null,
               <key> is not null
        :param visibility: The visibility of the filter
        :return: The created filter
        """
        data = {
            'filter_rules': filter_rules,
            'visibility': visibility
        }
        response = self.api.put('/filters/{0}'.format(filter_name), data=data)
        return Filter(response)

    def list(self, **kwargs):
        """Returns a list of all filters.

        :param kwargs: Optional parameters. Can be: `_sort`, `_include`,
               `_size`, `_offset`, `_all_tenants'`, or `_search`
        :return: The filters list
        """
        response = self.api.get('/filters', params=kwargs)
        return ListResponse([Filter(item) for item in response['items']],
                            response['metadata'])
