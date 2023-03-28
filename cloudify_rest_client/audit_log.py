from cloudify_rest_client.responses import DeletedResponse, ListResponse


class AuditLog(dict):
    """AuditLog describes a single entry in the `audit_log` table.  The table
    is used to to log `operation`s ('create', 'update', 'delete') performed on
    most of the database tables.  Fields `ref_table`,`ref_id` and
    `ref_identifier` are used to address referenced records."""

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
    def ref_identifier(self):
        """Row identifier in the referenced table."""
        return self['ref_identifier']

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

    def list(self, get_all=False, **kwargs):
        """Returns a list of AuditLogs.

        :param get_all: A flag which disables pagination and causes all results
                        to be returned in a single call.
        :param kwargs:  Optional query parameters, which fall in 3 categories:
                        `order_by` and `desc` define sorting, if not provided,
                        results returned are ordered by ``created_at asc``;
                        `offset` and `size` define a single page size in case
                        `get_all` is False, ``size = 0`` means no pagination
                        and all the results will be returned in a single
                        response, if not provided, defaults are ``offset = 0``
                        and ``size = 100``; other parameters will be used to
                        define a filter, for a list of available fields see
                        the REST service's ``models.AuditLog.fields``.
        :return:        ``ListResponse`` with of ``AuditLog`` items and
                        response metadata.
        """
        params = kwargs.copy()
        if get_all:
            params['size'] = 0
            params['offset'] = 0
        response = self.api.get('/audit', params=params)

        return ListResponse(
            [AuditLog(item) for item in response['items']],
            response['metadata'])

    def delete(self, **kwargs):
        """Delete (some) of the AuditLogs.

        :param kwargs: Filter fields, 'before' is a required parameter, while
                       'creator_name' and 'execution_id' are optional.
        :return:       DeletedResponse describing deletion outcome - a number
                       of 'deleted' records.
        """
        response = self.api.delete('/audit', params=kwargs)
        return DeletedResponse(**response)

    def inject(self, logs):
        """Inject audit logs. Intended for internal use only.

        :param logs: List of dict log entries to inject.
        """
        self.api.post('/audit', data=logs)
