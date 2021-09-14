from cloudify_rest_client.responses import DeletedResponse, ListResponse


class AuditLog(dict):
    """AuditLog describes a single entry in the `audit_log` table.  The table
    is used to to log `operation`s ('create', 'update', 'delete') performed on
    most of the database tables.  Fields `ref_table` and `ref_id` are used to
    address referenced records."""

    def __init__(self, audit_log):
        super(AuditLog, self).__init__()
        self.update(audit_log)

    @property
    def ref_table(self):
        """Referenced table name."""
        return self['ref_table']

    @property
    def ref_id(self):
        """Row ID in the referenced table."""
        return self['ref_id']

    @property
    def operation(self):
        """Operation performed on the row: 'create', 'update' or 'delete'."""
        return self['operation']

    @property
    def creator_name(self):
        """Username of the author of the change (if any)."""
        return self['creator_name']

    @property
    def execution_id(self):
        """Execution ID which performed the `operation` (if any)."""
        return self['execution_id']

    @property
    def created_at(self):
        """Operation timestamp."""
        return self['created_at']


class AuditLogClient(object):
    def __init__(self, api):
        self.api = api

    def list(self, **kwargs):
        """
        Returns a list of AuditLogs.

        :param kwargs: Optional filter fields.  For a list of available fields
                       see the REST service's models.AuditLog.fields.
        :return: AuditLogs list.
        """
        response = self.api.get('/audit', params=kwargs)

        return ListResponse(
            [AuditLog(item) for item in response['items']],
            response['metadata'])

    def delete(self, **kwargs):
        """
        Delete (some) of the AuditLogs.

        :param kwargs: Filter fields, 'before' is a required parameter, while
                       'creator_name' and 'execution_id' are optional.
        :return:       DeletedResponse describing deletion outcome - a number
                       of 'deleted' records.
        """
        response = self.api.delete('/audit', params=kwargs)
        return DeletedResponse(**response)
