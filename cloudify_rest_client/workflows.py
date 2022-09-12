from cloudify_rest_client.responses import ListResponse


class Workflow(dict):

    def __init__(self, workflow):
        super(Workflow, self).__init__()
        self.update(workflow)

    @property
    def id(self):
        return self['name']

    @property
    def name(self):
        return self['name']

    @property
    def parameters(self):
        return self['parameters']

    @property
    def operation(self):
        return self['operation']

    @property
    def plugin(self):
        return self['plugin']

    @property
    def is_available(self):
        """Is this workflow available for running?"""
        return self.get('is_available', True)

    @property
    def availability_rules(self):
        """Rules defining if this workflow is available"""
        return self.get('availability_rules')


class WorkflowsClient(object):
    def __init__(self, api):
        self.api = api

    def list(self, filter_id=None, filter_rules=None, **kwargs):
        """
        Returns a list of workflows.

        :param filter_id: A filter ID to filter the deployments list by
        :param filter_rules: A list of filter rules to filter the
               deployments list by
        :param kwargs: Optional filter fields. for a list of available fields
               see the REST service's models.Deployment.fields
        :return: Workflows list.
        """
        params = kwargs
        if filter_id:
            params['_filter_id'] = filter_id

        if filter_rules:
            response = self.api.post('/searches/workflows', params=params,
                                     data={'filter_rules': filter_rules})
        else:
            response = self.api.get('/workflows', params=params)

        return ListResponse(
            [Workflow(item) for item in response['items']],
            response['metadata'])
