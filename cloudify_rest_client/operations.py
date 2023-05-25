from cloudify_rest_client import utils
from cloudify_rest_client.exceptions import CloudifyClientError
from cloudify_rest_client.responses import ListResponse


class Operation(dict):
    def __init__(self, operation):
        self.update(operation)

    @property
    def id(self):
        return self.get('id')

    @property
    def state(self):
        return self.get('state')

    @property
    def created_at(self):
        return self.get('created_at')

    @property
    def dependencies(self):
        return self.get('dependencies')

    @property
    def type(self):
        return self.get('type')

    @property
    def parameters(self):
        return self.get('parameters', {})

    @property
    def name(self):
        return self.get('name')

    @property
    def containing_subgraph(self):
        return self.get('parameters', {}).get('containing_subgraph')

    @property
    def info(self):
        return self.get('parameters', {}).get('info')

    @property
    def tasks_graph_id(self):
        return self.get('tasks_graph_id')


class OperationsClient(object):
    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'operations'
        self._wrapper_cls = Operation

    def list(
        self,
        graph_id=None,
        _offset=None,
        _size=None,
        execution_id=None,
        state=None,
        skip_internal=False,
        _include=None,
    ):
        """List operations for the given graph or execution.

        :param graph_id: list operations for this graph
        :param execution_id: list operations for all graphs of this execution.
            Mutually exclusive with graph_id.
        :param state: only list operations in this state
        :param skip_internal: skip "uninteresting" internal operations; this
            will skip all local tasks and NOP tasks, and only return remote
            and subgraph tasks
        :param _offset: pagination offset
        :param _size: pagination size
        """
        params = {}
        if graph_id and execution_id:
            raise RuntimeError(
                'Pass either graph_id or execution_id, not both')
        if graph_id:
            params['graph_id'] = graph_id
        if execution_id:
            params['execution_id'] = execution_id
        if state:
            params['state'] = state
        if skip_internal:
            params['skip_internal'] = True
        if _offset is not None:
            params['_offset'] = _offset
        if _size is not None:
            params['_size'] = _size
        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                params=params, _include=_include)
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata'])

    def get(self, operation_id):
        response = self.api.get('/{self._uri_prefix}/{id}'
                                .format(self=self, id=operation_id))
        return Operation(response)

    def create(self, operation_id, graph_id, name, type, parameters,
               dependencies):
        data = {
            'name': name,
            'graph_id': graph_id,
            'dependencies': dependencies,
            'type': type,
            'parameters': parameters
        }
        uri = '/operations/{0}'.format(operation_id)
        response = self.api.put(uri, data=data, expected_status_code=201)
        return Operation(response)

    def update(self, operation_id, state, result=None,
               exception=None, exception_causes=None,
               manager_name=None, agent_name=None):
        uri = '/operations/{0}'.format(operation_id)
        self.api.patch(uri, data={
            'state': state,
            'result': result,
            'exception': exception,
            'exception_causes': exception_causes,
            'manager_name': manager_name,
            'agent_name': agent_name,
        }, expected_status_code=(
            200,  # compat with pre-6.2 managers
            204
        ))

    def _update_operation_inputs(self, deployment_id=None, node_id=None,
                                 operation=None, key=None, rel_index=None):
        """Update stored operations' inputs

        This is internal and is only called in deployment-update, to
        update stored operations' inputs.

        :param deployment_id: update operations of this deployment
        :param node_id: update operations of this node
        :param operation: the operation name
        :param key: operations/source_operations/target_operations
        :param rel_index: when updating relationship operations, look at
            the relationship at this index
        """
        self.api.post('/operations', data={
            'action': 'update-stored',
            'deployment_id': deployment_id,
            'node_id': node_id,
            'operation': operation,
            'key': key,
            'rel_index': rel_index,
        }, expected_status_code=(200, 204))

    def delete(self, operation_id):
        uri = '/operations/{0}'.format(operation_id)
        self.api.delete(uri)

    def dump(self, execution_ids=None, operation_ids=None):
        """Generate operations' attributes for a snapshot.

        :param execution_ids: A list of executions' identifiers used to
         select operations to be dumped, should not be empty.
        :param operation_ids: A list of operations' identifiers, if not empty,
         used to select specific operations to be dumped.
        :returns: A generator of dictionaries, which describe operations'
         attributes.
        """
        if not execution_ids:
            return
        params = {}
        for execution_id in execution_ids:
            params['execution_id'] = execution_id
            for entity in utils.get_all(
                    self.api.get,
                    f'/{self._uri_prefix}',
                    params=params,
                    _include=['agent_name', 'created_at', 'dependencies', 'id',
                              'manager_name', 'name', 'parameters', 'state',
                              'type', 'tasks_graph_id'],
            ):
                if not operation_ids or entity['id'] in operation_ids:
                    yield {'__entity': entity, '__source_id': execution_id}


class TasksGraph(dict):
    def __init__(self, tasks_graph):
        self.update(tasks_graph)

    @property
    def id(self):
        return self.get('id')

    @property
    def execution(self):
        return self.get('execution_id')

    @property
    def name(self):
        return self.get('name')


class TasksGraphClient(object):
    def __init__(self, api):
        self.api = api
        self._uri_prefix = 'tasks_graphs'
        self._wrapper_cls = TasksGraph

    def list(self, execution_id, name=None, _include=None):
        params = {'execution_id': execution_id}
        if name:
            params['name'] = name
        response = self.api.get('/{self._uri_prefix}'.format(self=self),
                                params=params, _include=_include)
        return ListResponse(
            [self._wrapper_cls(item) for item in response['items']],
            response['metadata'])

    def create(self, execution_id, name, operations=None, created_at=None,
               graph_id=None):
        params = {
            'name': name,
            'execution_id': execution_id,
            'operations': operations
        }
        if created_at:
            params['created_at'] = created_at
        if graph_id:
            params['graph_id'] = graph_id
        uri = '/{self._uri_prefix}/tasks_graphs'.format(self=self)
        response = self.api.post(uri, data=params, expected_status_code=201)
        return TasksGraph(response)

    def update(self, tasks_graph_id, state):
        uri = '/tasks_graphs/{0}'.format(tasks_graph_id)
        response = self.api.patch(uri, data={'state': state})
        return TasksGraph(response)

    def dump(self, execution_ids=None, operations=None, tasks_graph_ids=None):
        """Generate tasks graphs' attributes for a snapshot.

        :param execution_ids: A list of executions' identifiers used to
         select tasks graphs to be dumped, should not be empty.
        :param operations: A list of operations, which might be associated
         with (some of the) tasks graphs.
        :param tasks_graph_ids: A list of tasks graphs' identifiers, if not
         empty, used to select specific tasks graphs to be dumped.
        :returns: A generator of dictionaries, which describe tasks graphs'
         attributes.
        """
        if not execution_ids:
            return
        params = {}
        for execution_id in execution_ids:
            params['execution_id'] = execution_id
            not_assigned_ops = operations.get(execution_id) or []
            for entity in utils.get_all(
                    self.api.get,
                    f'/{self._uri_prefix}',
                    params=params,
                    _include=['created_at', 'execution_id', 'name', 'id'],
            ):
                graph_ops = [op for op in not_assigned_ops
                             if op['tasks_graph_id'] == entity['id']]
                not_assigned_ops = [op for op in not_assigned_ops
                                    if op['tasks_graph_id'] != entity['id']]
                for operation in graph_ops:
                    operation.pop('tasks_graph_id')
                if graph_ops:
                    entity['operations'] = graph_ops

                if not tasks_graph_ids or entity['id'] in tasks_graph_ids:
                    yield {'__entity': entity, '__source_id': execution_id}

    def restore(self, entities, logger, execution_id):
        """Restore tasks graphs from a snapshot.

        :param entities: An iterable (e.g. a list) of dictionaries describing
         tasks graphs to be restored.
        :param logger: A logger instance.
        :param execution_id: An execution identifier for the entities.
        """
        for entity in entities:
            entity['graph_id'] = entity.pop('id')
            entity['execution_id'] = execution_id
            try:
                self.create(**entity)
            except CloudifyClientError as exc:
                logger.error("Error restoring tasks graph "
                             f"{entity['graph_id']}: {exc}")
