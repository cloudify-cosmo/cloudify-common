from cloudify_rest_client import constants, utils
from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify_rest_client.responses import ListResponse


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
        self._filtered_resource = filtered_resource
        self.uri = '/filters/' + filtered_resource

    def create(self,
               filter_id,
               filter_rules,
               visibility=constants.VisibilityState.TENANT,
               created_at=None,
               created_by=None):
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
        :param created_by: Override the creator. Internal use only.
        :param created_at: Override the creation timestamp. Internal use only.
        :return: The created filter
        """
        data = {
            'filter_rules': filter_rules,
            'visibility': visibility,
        }
        if created_at:
            data['created_at'] = created_at
        if created_by:
            data['created_by'] = created_by
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
        if '_include' in params:
            params['_include'] = ','.join(params['_include'])
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

    def dump(self, filter_ids=None):
        """Generate filters' attributes for a snapshot.

        :param filter_ids: A list of filter identifiers, if not empty,
         used to select specific filters to be dumped.
        :returns: A generator of dictionaries, which describe filters'
         attributes.
        """
        for entity in utils.get_all(
                self.api.get,
                self.uri,
                _include=['created_at', 'id', 'visibility', 'value',
                          'created_by', 'is_system_filter'],
        ):
            if entity.pop('is_system_filter'):
                continue
            if not filter_ids or entity['id'] in filter_ids:
                yield entity

    def restore(self, entities, logger):
        """Restore filters from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         filters to be restored.
        :param logger: A logger instance.
        """
        for entity in entities:
            entity['filter_id'] = entity.pop('id')
            entity['filter_rules'] = entity.pop('value')
            try:
                self.create(**entity)
            except CloudifyClientError as exc:
                logger.error("Error restoring filter "
                             f"{entity['filter_id']}: {exc}")


class BlueprintsFiltersClient(FiltersClient):
    def __init__(self, api):
        super(BlueprintsFiltersClient, self).__init__(api, 'blueprints')


class DeploymentsFiltersClient(FiltersClient):
    def __init__(self, api):
        super(DeploymentsFiltersClient, self).__init__(api, 'deployments')
