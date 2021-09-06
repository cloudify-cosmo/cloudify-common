from cloudify_rest_client.responses import ListResponse


class AuditLog(dict):

    def __init__(self, audit_log):
        super(AuditLog, self).__init__()
        self.update(audit_log)

    @property
    def ref_table(self):
        return self['ref_table']

    @property
    def ref_id(self):
        return self['ref_id']

    @property
    def operation(self):
        return self['operation']

    @property
    def creator_name(self):
        return self['creator_name']

    @property
    def execution_id(self):
        return self['execution_id']

    @property
    def created_at(self):
        return self['created_at']


class AuditLogClient(object):
    def __init__(self, api):
        self.api = api

    def list(self, **kwargs):
        """
        Returns a list of AuditLogs.

        :param kwargs: Optional filter fields. for a list of available fields
               see the REST service's models.AuditLog.fields
        :return: AuditLogs list.
        """
        params = kwargs
        response = self.api.get('/audit', params=params)

        return ListResponse(
            [AuditLog(item) for item in response['items']],
            response['metadata'])
