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


class _LabelsClient(object):
    def __init__(self, api, resource_name):
        self.api = api
        self.resource_name = resource_name

    def list_keys(self):
        """
        Returns all defined label keys, from all elements of the resource.
        """
        response = self.api.get('/labels/{0}'.format(self.resource_name))
        return ListResponse(response['items'], response['metadata'])

    def list_key_values(self, label_key):
        """
        Returns all deployments labels' values for the specified key.

        :param label_key: The resource labels' key to list the values for.
        """
        response = self.api.get(
            '/labels/{0}/{1}'.format(self.resource_name, label_key))
        return ListResponse(response['items'], response['metadata'])

    def get_reserved_labels_keys(self):
        """Returns the reserved labels keys (`csys-` prefixed)."""
        response = self.api.get('/labels/{0}'.format(self.resource_name),
                                params={'_reserved': True})
        return ListResponse(response['items'], response['metadata'])


class DeploymentsLabelsClient(_LabelsClient):
    def __init__(self, api):
        super(DeploymentsLabelsClient, self).__init__(api, 'deployments')


class BlueprintsLabelsClient(_LabelsClient):
    def __init__(self, api):
        super(BlueprintsLabelsClient, self).__init__(api, 'blueprints')
