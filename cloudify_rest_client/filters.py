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
        return self.get('value')

    @property
    def labels_filter_rules(self):
        return self.get('labels_filter_rules')

    @property
    def attrs_filter_rules(self):
        return self.get('attrs_filter_rules')

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


class FiltersClient(object):
    def __init__(self, api, filtered_resource):
        self.api = api
        self.uri = '/filters/' + filtered_resource

    def create(self,
               filter_id,
               filter_rules,
               visibility=VisibilityState.TENANT):
        """Creates a new filter.

        :param filter_id: The filter ID
        :param filter_rules: A list of filter rules. A filter rule is a
               dictionary of the form
               {
                   key: <key>,
                   values: [<list of values>],
                   operator: <LabelsOperator> or <AttrsOperator>,
                   type: <FilterRuleType>
              }
        :param visibility: The filter's visibility
        :return: The created filter
        """
        data = {
            'filter_rules': filter_rules,
            'visibility': visibility
        }
        response = self.api.put('{0}/{1}'.format(self.uri, filter_id),
                                data=data)
        return Filter(response)

    def list(self, sort=None, is_descending=False, **kwargs):
        """Returns a list of all filters.

        :param sort: Key for sorting the list
        :param is_descending: True for descending order, False for ascending
        :param kwargs: Optional parameters. Can be: `_sort`, `_include`,
               `_size`, `_offset`, `_all_tenants'`, or `_search`
        :return: The filters list
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get(self.uri, params=params)
        return ListResponse([Filter(item) for item in response['items']],
                            response['metadata'])

    def get(self, filter_id):
        response = self.api.get('{0}/{1}'.format(self.uri, filter_id))
        return Filter(response)

    def delete(self, filter_id):
        self.api.delete('{0}/{1}'.format(self.uri, filter_id))

    def update(self, filter_id, new_filter_rules=None, new_visibility=None):
        """Updates the filter's visibility or rules

        :param filter_id: The Id of the filter to update
        :param new_filter_rules: A new list of filter rules. A filter rule is a
               dictionary of the form
               {
                   key: <key>,
                   values: [<list of values>],
                   operator: <LabelsOperator> or <AttrsOperator>,
                   type: <FilterRuleType>
              }
        :param new_visibility: The new visibility to update
        :return: The updated filter
        """
        data = {}
        if not new_filter_rules and not new_visibility:
            raise RuntimeError('In order to update a filter, you must specify '
                               'either a new list of filter rules or a new '
                               'visibility')

        if new_visibility:
            data['visibility'] = new_visibility
        if new_filter_rules:
            data['filter_rules'] = new_filter_rules

        response = self.api.patch('{0}/{1}'.format(self.uri, filter_id),
                                  data=data)
        return Filter(response)


class BlueprintsFiltersClient(FiltersClient):
    def __init__(self, api):
        super(BlueprintsFiltersClient, self).__init__(api, 'blueprints')


class DeploymentsFiltersClient(FiltersClient):
    def __init__(self, api):
        super(DeploymentsFiltersClient, self).__init__(api, 'deployments')
