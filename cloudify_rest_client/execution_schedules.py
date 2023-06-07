from datetime import datetime

from cloudify_rest_client import utils
from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify_rest_client.responses import ListResponse


class ExecutionSchedule(dict):
    """
    Cloudify execution schedule.
    """

    def __init__(self, execution_schedule):
        super(ExecutionSchedule, self).__init__()
        self.update(execution_schedule)

    @property
    def id(self):
        """
        :return: The name of the execution schedule.
        """
        return self.get('id')

    @property
    def deployment_id(self):
        """
        :return: The deployment's id which the scheduled execution is
        related to.
        """
        return self.get('deployment_id')

    @property
    def workflow_id(self):
        """
        :return: The id of the workflow which the scheduled execution runs.
        """
        return self.get('workflow_id')

    @property
    def parameters(self):
        """
        :return: The scheduled execution's parameters
        """
        return self.get('parameters') or {}

    @property
    def created_at(self):
        """
        :return: Timestamp of the execution schedule creation.
        """
        return self.get('created_at')

    @property
    def next_occurrence(self):
        """
        :return: The calculated next date and time in which the scheduled
        workflow should be executed at.
        """
        return self.get('next_occurrence')

    @property
    def since(self):
        """
        :return: The earliest date and time the scheduled workflow should be
        executed at.
        """
        return self.get('since')

    @property
    def until(self):
        """
        :return: The latest date and time the scheduled workflow may be
        executed at (if any).
        """
        return self.get('until')

    @property
    def stop_on_fail(self):
        """
        :return: Whether the scheduler should stop attempting to run the
        execution once it failed.
        """
        return self.get('stop_on_fail')

    @property
    def enabled(self):
        """
        :return: Whether this schedule is currently enabled.
        """
        return self.get('enabled')


class ExecutionSchedulesClient(object):

    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'execution-schedules'

    def create(self, schedule_id, deployment_id, workflow_id,
               execution_arguments=None, parameters=None,
               since=None, until=None, recurrence=None, count=None,
               weekdays=None, rrule=None, slip=0, stop_on_fail=False,
               created_by=None, created_at=None, enabled=True):
        """Schedules a deployment's workflow execution whose id is provided.

        :param schedule_id: Name for the schedule task. Used for listing,
            updating or deleting it later.
        :param deployment_id: The deployment's id to execute a workflow for.
        :param workflow_id: The workflow to be executed id.
        :param execution_arguments: A dict of arguments passed directly to the
            workflow execution. May contain the following keys:
            - allow_custom_parameters: bool
            - force: bool
            - dry_run: bool
            - queue: bool
            - wait_after_fail: integer
            See Executions for more details on these.
        :param parameters: Parameters for the workflow execution.
        :param since: A string representing the earliest date and time this
            workflow should be executed at. Must be provided.
        :param until: A string representing the latest date and time this
            workflow may be executed at. May be empty.
        :param recurrence: A string representing the frequency with which to
            run the execution, e.g. '2 weeks'. Must be provided if no `rrule`
            is given and `count` is other than 1.
        :param count: Maximum number of times to run the execution.
            If left empty, there's no limit on repetition.
        :param weekdays: A list of strings representing the weekdays on
            which to run the execution, e.g. ['su', 'mo', 'tu']. If left
            empty, the execution will run on any weekday.
        :param rrule: A string representing a scheduling rule in the
            iCalendar format, e.g. 'RRULE:FREQ=DAILY;INTERVAL=3', which means
            "run every 3 days". Mutually exclusive with `recurrence`, `count`
            and `weekdays`.
        :param slip: Maximum time window after the target time has passed,
            in which the scheduled execution can run (in minutes).
        :param stop_on_fail: If set to true, once the execution has failed,
            the scheduler won't make further attempts to run it.
        :param created_by: Override the creator. Internal use only.
        :param created_at: Override the creation timestamp. Internal use only.
        :param enabled: Boolean indicating whether the schedule should be
                        enabled.
        :return: The created execution schedule.
        """
        assert schedule_id
        assert deployment_id
        assert workflow_id
        assert since
        if isinstance(since, datetime):
            since = since.isoformat()
        if isinstance(until, datetime):
            until = until.isoformat()
        params = {'deployment_id': deployment_id}
        data = {
            'workflow_id': workflow_id,
            'execution_arguments': execution_arguments,
            'parameters': parameters,
            'since': since,
            'until': until,
            'recurrence': recurrence,
            'count': count,
            'weekdays': weekdays,
            'rrule': rrule,
            'slip': slip,
            'stop_on_fail': stop_on_fail,
            'enabled': enabled,
        }
        if created_by:
            data['created_by'] = created_by
        if created_at:
            data['created_at'] = created_at
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=schedule_id)
        response = self.api.put(uri,
                                data=data,
                                params=params,
                                expected_status_code=201)
        return ExecutionSchedule(response)

    def update(self, schedule_id, deployment_id, since=None, until=None,
               recurrence=None, count=None, weekdays=None, rrule=None,
               slip=None, stop_on_fail=None, enabled=None, workflow_id=None):
        """Updates scheduling parameters of an existing execution schedule
        whose id is provided.

        :param schedule_id: Name for the schedule task.
        :param deployment_id: The deployment to which the schedule belongs.
        :param since: A string representing the earliest date and time this
            workflow should be executed at. Must be provided if no `rrule` is
            given.
        :param until: A string representing the latest date and time this
            workflow may be executed at. May be empty.
        :param recurrence: A string representing the frequency with which to
            run the execution, e.g. '2 weeks'. Must be provided if no `rrule`
            is given and `count` is other than 1.
        :param count: Maximum number of times to run the execution.
            If left empty, there's no limit on repetition.
        :param weekdays: A list of strings representing the weekdays on
            which to run the execution, e.g. ['su', 'mo', 'tu']. If left
            empty, the execution will run on any weekday.
        :param rrule: A string representing a scheduling rule in the
            iCalendar format, e.g. 'RRULE:FREQ=DAILY;INTERVAL=3', which means
            "run every 3 days". Mutually exclusive with `recurrence`, `count`
            and `weekdays`.
        :param slip: Maximum time window after the target time has passed,
            in which the scheduled execution can run (in minutes).
        :param stop_on_fail: If set to true, once the execution has failed,
            the scheduler won't make further attempts to run it.
        :param enabled: Set to false to make the scheduler ignore this
            schedule, until set to true again.
        :param workflow_id: The new workflow that this schedule is going to run

        :return: The updated execution schedule.
        """
        assert schedule_id
        params = {'deployment_id': deployment_id}
        data = {
            'since': since.isoformat() if since else None,
            'until': until.isoformat() if until else None,
            'recurrence': recurrence,
            'count': count,
            'weekdays': weekdays,
            'rrule': rrule,
            'slip': slip,
            'enabled': enabled,
            'stop_on_fail': stop_on_fail,
            'workflow_id': workflow_id,
        }
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=schedule_id)
        response = self.api.patch(uri,
                                  data=data,
                                  params=params,
                                  expected_status_code=201)
        return ExecutionSchedule(response)

    def delete(self, schedule_id, deployment_id):
        """
        Deletes the execution schedule whose id matches the provided id.

        :param schedule_id: Name for the schedule task.
        :param deployment_id: The deployment to which the schedule belongs.
        """
        assert schedule_id
        params = {'deployment_id': deployment_id}
        self.api.delete('/{self._uri_prefix}/{id}'.format(self=self,
                                                          id=schedule_id),
                        params=params,
                        expected_status_code=204)

    def list(self, _include=None, sort=None, is_descending=False, **kwargs):
        """
        Returns a list of currently existing execution schedules.

        :param _include: List of fields to include in response.
        :param sort: Key for sorting the list.
        :param is_descending: True for descending order, False for ascending.
        :param kwargs: Optional filter fields. For a list of available fields
               see the REST service's models.Execution.fields
        :return: Schedules list.
        """
        params = kwargs
        if sort:
            params['_sort'] = '-' + sort if is_descending else sort

        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                params=params, _include=_include)
        return ListResponse([ExecutionSchedule(item)
                             for item in response['items']],
                            response['metadata'])

    def get(self, schedule_id, deployment_id, _include=None):
        """Get an execution schedule by its id.

        :param schedule_id: Name for the schedule task.
        :param deployment_id: The deployment to which the schedule belongs.
        :param _include: List of fields to include in response.
        :return: Execution.
        """
        assert schedule_id
        params = {'deployment_id': deployment_id}
        uri = '/{self._uri_prefix}/{id}'.format(self=self, id=schedule_id)
        response = self.api.get(uri, _include=_include, params=params)
        return ExecutionSchedule(response)

    def dump(self, execution_schedule_ids=None):
        """Generate execution schedules' attributes for a snapshot.

        :param execution_schedule_ids: A list of execution schedules'
         identifiers, if not empty, used to select specific execution
         schedules to be dumped.
        :returns: A generator of dictionaries, which describe execution
         schedules' attributes.
        """
        entities = utils.get_all(
                self.api.get,
                f'/{self._uri_prefix}',
                _include=['id', 'rule', 'deployment_id', 'workflow_id',
                          'created_at', 'since', 'until', 'stop_on_fail',
                          'parameters', 'execution_arguments', 'slip',
                          'enabled', 'created_by'],
        )
        if not execution_schedule_ids:
            return entities
        return (e for e in entities if e['id'] in execution_schedule_ids)

    def restore(self, entities, logger):
        """Restore execution schedules from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         execution schedules to be restored.
        :param logger: A logger instance.
        """
        for entity in entities:
            entity['schedule_id'] = entity.pop('id')
            entity['rrule'] = entity.pop('rule', {}).pop('rrule')
            try:
                self.create(**entity)
            except CloudifyClientError as exc:
                logger.error("Error restoring execution schedule "
                             f"{entity['schedule_id']}: {exc}")
