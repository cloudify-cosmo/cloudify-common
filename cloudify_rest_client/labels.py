from cloudify_rest_client.responses import ListResponse


class Label(dict):
    def __init__(self, label):
        super(Label, self).__init__()
        self.update(label)

    @property
    def key(self):
        return self['key']

    @property
    def value(self):
        return self['value']

    @property
    def created_at(self):
        return self['created_at']

    @property
    def creator_id(self):
        return self['creator_id']


class DeploymentsLabelsClient(object):
    def __init__(self, api):
        self.api = api

    def list_keys(self):
        """
        Returns all defined label keys, from all deployments.
        """
        response = self.api.get('/labels/deployments')
        return ListResponse(response['items'], response['metadata'])

    def list_key_values(self, label_key):
        """
        Returns all deployments labels' values for the specified key.

        :param label_key: The deployments labels' key to list the values for.
        """
        response = self.api.get('/labels/deployments/{0}'.format(label_key))
        return ListResponse(response['items'], response['metadata'])
