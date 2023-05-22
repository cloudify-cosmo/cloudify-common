import warnings
from datetime import datetime

from cloudify_rest_client import utils
from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify_rest_client.responses import ListResponse


class EventsClient(object):

    def __init__(self, api):
        self.api = api

    def get(self,
            execution_id,
            from_event=0,
            batch_size=100,
            include_logs=False):
        """
        Returns event for the provided execution id.

        :param execution_id: Id of execution to get events for.
        :param from_event: Index of first event to retrieve on pagination.
        :param batch_size: Maximum number of events to retrieve per call.
        :param include_logs: Whether to also get logs.
        :return: Events list and total number of currently available
         events (tuple).
        """
        warnings.warn('method is deprecated, use "{0}" method instead'
                      .format(self.list.__name__),
                      DeprecationWarning)

        response = self.list(execution_id=execution_id,
                             include_logs=include_logs,
                             _offset=from_event,
                             _size=batch_size,
                             _sort='@timestamp')
        events = response.items
        total_events = response.metadata.pagination.total
        return events, total_events

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

        response = self.api.get(uri, _include=_include, params=params)
        return ListResponse(response['items'], response['metadata'])

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

        self.api.post('/events', data=data, expected_status_code=(201, 204))

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

        response = self.api.delete(uri, params=params,
                                   expected_status_code=200)
        return ListResponse(response['items'], response['metadata'])

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

    def dump(self, execution_ids=None, execution_group_ids=None,
             include_logs=None, event_storage_ids=None):
        """Generate events' attributes for a snapshot.

        :param execution_ids: A list of executions' identifiers used to
         select events to be dumped.
        :param execution_group_ids: A list of execution groups' identifiers
         used to select events to be dumped.
        :param include_logs: A flag, which determines if `cloudify_log` entries
         should be included in the snapshot.
        :param event_storage_ids: A list of events' storage identifiers, if
         not empty, used to select specific events to be dumped.
        :returns: A generator of dictionaries, which describe events'
         attributes.
        """
        if execution_ids:
            for entity in self._dump_events(include_logs, 'execution_id',
                                            execution_ids):
                entity.update({'__source': 'executions'})
                if not event_storage_ids or \
                        entity['__entity']['_storage_id'] in event_storage_ids:
                    yield entity
        if execution_group_ids:
            for entity in self._dump_events(include_logs, 'execution_group_id',
                                            execution_group_ids):
                entity.update({'__source': 'execution_groups'})
                if not event_storage_ids or \
                        entity['__entity']['_storage_id'] in event_storage_ids:
                    yield entity

    def _dump_events(self, include_logs, event_source_id_key, source_ids):
        if not source_ids:
            return []
        params = {
            '_get_data': True,
            'type': ['cloudify_event'],
        }
        if include_logs:
            params['type'].append('cloudify_log')
        for source_id in source_ids:
            params[event_source_id_key] = source_id
            for entity in utils.get_all(
                    self.api.get,
                    '/events',
                    params=params,
                    _include=['_storage_id', 'timestamp', 'reported_timestamp',
                              'workflow_id', 'blueprint_id', 'deployment_id',
                              'deployment_display_name',
                              'message', 'error_causes', 'event_type',
                              'operation', 'source_id', 'target_id',
                              'node_instance_id', 'type', 'logger', 'level',
                              'manager_name', 'agent_name'],
            ):
                yield {'__entity': entity, '__source_id': source_id}

    def restore(self, entities, logger, source_type, source_id):
        """Restore events from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         node instances to be restored.
        :param logger: A logger instance.
        :param source_type: Type of events' "parent" entity: `executions`
         or `execution_groups`.
        :param source_id: An identifier of the entity, which these events
         belong to.
        """
        events = {}
        logs = {}
        logger_names = set()
        for entity in entities:
            manager = entity.pop('manager_name')
            agent = entity.pop('agent_name')
            logger_name = (manager, agent)
            logger_names.add(logger_name)
            if entity['type'] == 'cloudify_event':
                entity['context'] = {
                    'source_id': entity.pop('source_id'),
                    'target_id': entity.pop('target_id'),
                    # This looks wrong, but it's a legacy thing
                    'node_id': entity.pop('node_instance_id'),
                }
                entity['message'] = {
                    'text': entity.pop('message'),
                }
                events.setdefault(logger_name, []).append(entity)
            elif entity['type'] == 'cloudify_log':
                entity['context'] = {
                    'operation': entity.pop('operation'),
                    'source_id': entity.pop('source_id'),
                    'target_id': entity.pop('target_id'),
                    # This looks wrong, but it's a legacy thing
                    'node_id': entity.pop('node_instance_id'),
                }
                entity['message'] = {
                    'text': entity.pop('message'),
                }
                logs.setdefault(logger_name, []).append(entity)
            else:
                logger.warn(
                        'Log/event parsing failed on %s',
                        entity,
                )

        for logger_name in logger_names:
            manager, agent = logger_name
            kwargs = {
                'events': events.pop(logger_name, []),
                'logs': logs.pop(logger_name, []),
                'manager_name': manager,
                'agent_name': agent,
            }
            if source_type == 'executions':
                kwargs['execution_id'] = source_id
            elif source_type == 'execution_groups':
                kwargs['execution_group_id'] = source_id

            try:
                self.create(**kwargs)
            except CloudifyClientError as exc:
                logger.error(f'Error restoring events of {source_type} '
                             f'{source_id}: {exc}')
