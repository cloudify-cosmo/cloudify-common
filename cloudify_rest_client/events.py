from datetime import datetime

from cloudify_rest_client.responses import ListResponse


class EventsClient(object):

    def __init__(self, api):
        self.api = api

    def list(self, include_logs=False, message=None, from_datetime=None,
             to_datetime=None, _include=None, sort=None, **kwargs):
        """List events

        :param include_logs: Whether to also get logs.
        :param message: an expression used for wildcard search events
                        by their message text
        :param from_datetime: search for events later or equal to datetime
        :param to_datetime: search for events earlier or equal to datetime
        :param _include: return only an exclusive list of fields
        :param sort: Key for sorting the list.
        :return: dict with 'metadata' and 'items' fields
        """

        uri = '/events'
        params = self._create_query(include_logs=include_logs,
                                    message=message,
                                    from_datetime=from_datetime,
                                    to_datetime=to_datetime,
                                    sort=sort,
                                    **kwargs)

        return self.api.get(
            uri,
            _include=_include,
            params=params,
            wrapper=ListResponse.of(dict),
        )

    def create(self, events=None, logs=None, execution_id=None,
               agent_name=None, manager_name=None,
               execution_group_id=None):
        """Create events & logs

        :param events: List of events to be created
        :param logs: List of logs to be created
        :param execution_id: Create logs/events for this execution
        :param execution_group_id: Create logs/events for this execution group
        :return: None
        """
        data = {
            'events': events,
            'logs': logs,
            'agent_name': agent_name,
            'manager_name': manager_name,
        }
        if execution_id:
            data['execution_id'] = execution_id
        if execution_group_id:
            data['execution_group_id'] = execution_group_id

        return self.api.post(
            '/events',
            data=data,
            expected_status_code=(201, 204),
        )

    def delete(self, deployment_id, include_logs=False, message=None,
               from_datetime=None, to_datetime=None, sort=None, **kwargs):
        """Delete events connected to a Deployment ID

        :param deployment_id: The ID of the deployment
        :param include_logs: Whether to also get logs.
        :param message: an expression used for wildcard search events
                        by their message text
        :param from_datetime: search for events later or equal to datetime
        :param to_datetime: search for events earlier or equal to datetime
        :param sort: Key for sorting the list.
        :return: dict with 'metadata' and 'items' fields
        """

        uri = '/events'
        params = self._create_query(include_logs=include_logs,
                                    message=message,
                                    from_datetime=from_datetime,
                                    to_datetime=to_datetime,
                                    sort=sort,
                                    deployment_id=deployment_id,
                                    **kwargs)

        return self.api.delete(
            uri, params=params,
            expected_status_code=200,
            wrapper=ListResponse.of(lambda x: x),
        )

    @staticmethod
    def _create_query(include_logs=False, message=None, from_datetime=None,
                      to_datetime=None, sort=None, **kwargs):
        params = kwargs
        if message:
            params['message.text'] = str(message)

        params['type'] = ['cloudify_event']
        if include_logs:
            params['type'].append('cloudify_log')

        timestamp_range = dict()

        if from_datetime:
            # if a datetime instance, convert to iso format
            timestamp_range['from'] = \
                from_datetime.isoformat() if isinstance(
                    from_datetime, datetime) else from_datetime

        if to_datetime:
            timestamp_range['to'] = \
                to_datetime.isoformat() if isinstance(
                    to_datetime, datetime) else to_datetime

        if timestamp_range:
            params['_range'] = params.get('_range', [])
            params['_range'].append('@timestamp,{0},{1}'
                                    .format(timestamp_range.get('from', ''),
                                            timestamp_range.get('to', '')))
        if sort:
            params['_sort'] = sort

        return params
