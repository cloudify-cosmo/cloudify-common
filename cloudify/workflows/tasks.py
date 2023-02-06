########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

import time
import threading
import types
from functools import lru_cache, wraps

from cloudify import exceptions, logs
from cloudify.workflows import api
from cloudify.manager import (
    get_rest_client,
    get_node_instance,
    update_execution_status,
)
from cloudify.constants import (
    MGMTWORKER_QUEUE,
    TASK_PENDING,
    TASK_SENDING,
    TASK_SENT,
    TASK_STARTED,
    TASK_RESCHEDULED,
    TASK_SUCCEEDED,
    TASK_FAILED,
    TERMINATED_STATES,
)
from cloudify.state import workflow_ctx, current_workflow_ctx
from cloudify.utils import get_func, uuid4
from cloudify.error_handling import serialize_known_exception
# imported for backwards compat:
from cloudify.constants import TASK_RESPONSE_SENT, INSPECT_TIMEOUT  # noqa


INFINITE_TOTAL_RETRIES = -1
DEFAULT_TOTAL_RETRIES = INFINITE_TOTAL_RETRIES
DEFAULT_RETRY_INTERVAL = 30
DEFAULT_SUBGRAPH_TOTAL_RETRIES = 0

DEFAULT_SEND_TASK_EVENTS = True
DISPATCH_TASK = 'cloudify.dispatch.dispatch'
_NOT_SET = object()


def with_execute_after(f):
    """Wrap a task's apply_async to delay it if requested.

    If a task has .execute_after set, the apply_async will actually
    only run after that time has passed.
    """
    @wraps(f)
    def _inner(*args, **kwargs):
        task = args[0]
        if api.has_cancel_request():
            return task.async_result
        if task.execute_after and task.execute_after > time.time():
            t = threading.Timer(
                task.execute_after - time.time(),
                _inner, args=args, kwargs=kwargs
            )
            t.daemon = True
            t.start()
            return task.async_result
        with current_workflow_ctx.push(task.workflow_context):
            return f(*args, **kwargs)
    return _inner


class WorkflowTask(object):
    """A base class for workflow tasks

    A WorkflowTask represents an operation to be executed by the mgmtworker
    or an agent.

    There's two main kinds of WorkflowTasks - a remote kind, to be sent
    to a remote mgmtworker/agent and executed there; and a local kind, to
    be executed in the current process, in a background thread.

    The interface of a WorkflowTask is its apply_async method, which
    returns a WorkflowTaskResult.
    """

    def __init__(self,
                 workflow_context,
                 task_id=None,
                 info=None,
                 on_success=None,
                 on_failure=None,
                 total_retries=DEFAULT_TOTAL_RETRIES,
                 retry_interval=DEFAULT_RETRY_INTERVAL,
                 timeout=None,
                 timeout_recoverable=None,
                 send_task_events=DEFAULT_SEND_TASK_EVENTS):
        """
        :param task_id: The id of this task (generated if none is provided)
        :param info: A short description of this task (for logging)
        :param on_success: A handler called when the task's execution
                           terminates successfully.
                           Expected to return one of
                           [HandlerResult.retry(), HandlerResult.cont()]
                           to indicate whether this task should be re-executed.
        :param on_failure: A handler called when the task's execution
                           fails.
                           Expected to return one of
                           [HandlerResult.retry(), HandlerResult.ignore(),
                            HandlerResult.fail()]
                           to indicate whether this task should be re-executed,
                           cause the engine to terminate workflow execution
                           immediately or simply ignore this task failure and
                           move on.
        :param total_retries: Maximum retry attempt for this task, in case
                              the handlers return a retry attempt.
        :param retry_interval: Number of seconds to wait between retries
        :param workflow_context: the CloudifyWorkflowContext instance
        """
        self.id = task_id or uuid4()
        self._state = TASK_PENDING
        self.async_result = WorkflowTaskResult(self)
        self.on_success = on_success
        self.on_failure = on_failure
        self.info = info
        self.total_retries = total_retries
        self.retry_interval = retry_interval
        self.timeout = timeout
        self.timeout_recoverable = timeout_recoverable
        self.is_terminated = False
        self.workflow_context = workflow_context
        self.send_task_events = send_task_events
        self.containing_subgraph = None

        self.current_retries = 0
        # timestamp for which the task should not be executed
        # by the task graph before reached, overridden by the task
        # graph during retries
        self.execute_after = None
        self.stored = False

        # ID of the task that is being retried by this task
        self.retried_task = None

        # error is a dict as returned by serialize_known_exception:
        # for remote tasks it is set when the AMQP dispatcher receives a
        # task error response
        self.error = None

        # this is set when a dependency errors out. Then, this task must not
        # run!
        self.dependency_error = False

    @classmethod
    def restore(cls, ctx, graph, task_descr):
        params = task_descr.parameters
        on_success = cls._deserialize_handler(params.pop('on_success', None))
        on_failure = cls._deserialize_handler(params.pop('on_failure', None))
        task = cls(
            workflow_context=ctx,
            task_id=task_descr.id,
            info=params['info'],
            **params['task_kwargs']
        )
        task._state = task_descr.state
        task.on_failure = on_failure
        task.on_success = on_success
        task.current_retries = params['current_retries']
        task.send_task_events = params['send_task_events']
        task.containing_subgraph = params['containing_subgraph']
        if 'retry_interval' in params:
            task.retry_interval = params['retry_interval']
        if 'total_retries' in params:
            task.total_retries = params['total_retries']
        task.stored = True
        return task

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, WorkflowTask):
            return self.id == other.id
        return self.id == other

    def __repr__(self):
        return '<{0} {1}>'.format(self.task_type, self.id)

    @property
    def task_type(self):
        return self.__class__.__name__

    def _serialize_handler(self, handler):
        """Serialize the on_failure/on_success handler.

        For functions, just their qualified import name will be stored
        (and later restored by simply importing it).
        For class instances, the qualified import name and optionally the
        __init__ kwargs returned by a .dump method on that class.
        """
        if not handler:
            return None
        if isinstance(handler, types.FunctionType):
            path = '{0}.{1}'.format(handler.__module__, handler.__name__)
        else:
            path = '{0}.{1}'.format(
                handler.__class__.__module__, handler.__class__.__name__)
        if '<' in path:  # eg. "f.<locals>.g"
            raise exceptions.NonRecoverableError(
                'Cannot serialize handler {0}'.format(handler))
        serialized = {'path': path}
        if hasattr(handler, 'dump'):
            serialized.update(handler.dump())
        return serialized

    @classmethod
    def _deserialize_handler(cls, data):
        if not data:
            return None
        elif 'path' in data:
            func = get_func(data.pop('path'))
            if isinstance(func, types.FunctionType):
                return func
            return func(**data)
        else:
            raise exceptions.NonRecoverableError(
                'Cannot deserialize: {0!r}'.format(data))

    def dump(self):
        return {
            'id': self.id,
            'name': self.name,
            'state': self._state,
            'type': self.task_type,
            'parameters': {
                'on_success': self._serialize_handler(self.on_success),
                'on_failure': self._serialize_handler(self.on_failure),
                'retried_task': self.retried_task,
                'current_retries': self.current_retries,
                'send_task_events': self.send_task_events,
                'retry_interval': self.retry_interval,
                'total_retries': self.total_retries,
                'info': self.info,
                'error': self.error,
                'containing_subgraph': getattr(
                    self.containing_subgraph, 'id', None),
                'task_kwargs': {},
            }
        }

    def apply_async(self):
        return self.async_result

    def is_remote(self):
        """
        :return: Is this a remote task
        """
        return not self.is_local()

    def is_local(self):
        """
        :return: Is this a local task
        """
        raise NotImplementedError('Implemented by subclasses')

    def is_nop(self):
        """
        :return: Is this a NOP task
        """
        return False

    def get_state(self):
        """
        Get the task state

        :return: The task state [pending, sending, sent, started,
                                 rescheduled, succeeded, failed]
        """
        return self._state

    def set_state(self, state, result=_NOT_SET, exception=None):
        """
        Set the task state

        :param state: The state to set [pending, sending, sent, started,
                                        rescheduled, succeeded, failed]
        """
        if state not in [TASK_PENDING, TASK_SENDING, TASK_SENT, TASK_STARTED,
                         TASK_RESCHEDULED, TASK_SUCCEEDED, TASK_FAILED]:
            raise RuntimeError('Illegal state set on task: {0} '
                               '[task={1}]'.format(state, str(self)))
        if self._state in TERMINATED_STATES:
            return
        self._state = state
        if self.stored:
            self._update_stored_state(
                state, result=result, exception=exception)
        if not self.stored:
            event = {}
            if result is not _NOT_SET:
                event['result'] = result
            elif exception:
                event['exception'] = exception
            try:
                self.workflow_context.internal.send_task_event(
                    state, self, event)
            except RuntimeError:
                pass
        if state in TERMINATED_STATES:
            self.is_terminated = True

    def _update_stored_state(self, state, result=_NOT_SET, exception=None):
        kwargs = {'state': state}
        if result is not _NOT_SET:
            kwargs['result'] = result
        if exception is not None:
            kwargs['exception'] = exception
        self.workflow_context.update_operation(self.id, **kwargs)

    def handle_task_terminated(self):
        if self.get_state() in (TASK_FAILED, TASK_RESCHEDULED):
            handler_result = self._handle_task_not_succeeded()
        else:
            handler_result = self._handle_task_succeeded()

        if handler_result.action == HandlerResult.HANDLER_RETRY:
            if (self.total_retries == INFINITE_TOTAL_RETRIES or
                    self.current_retries < self.total_retries or
                    handler_result.ignore_total_retries):
                if handler_result.retry_after is None:
                    handler_result.retry_after = self.retry_interval
                if handler_result.retried_task is None:
                    new_task = self.duplicate_for_retry(
                        time.time() + handler_result.retry_after)
                    handler_result.retried_task = new_task
            else:
                if self.is_subgraph and handler_result.retried_task:
                    self.graph.remove_task(handler_result.retried_task)
                handler_result.action = HandlerResult.HANDLER_FAIL

        if self.containing_subgraph:
            subgraph = self.containing_subgraph
            retried_task = None
            if handler_result.action == HandlerResult.HANDLER_FAIL:
                handler_result.action = HandlerResult.HANDLER_IGNORE
                # It is possible that two concurrent tasks failed.
                # we will only consider the first one handled
                if not subgraph.failed_task:
                    subgraph.failed_task = self
            elif handler_result.action == HandlerResult.HANDLER_RETRY:
                retried_task = handler_result.retried_task
            subgraph.task_terminated(task=self, new_task=retried_task)

        return handler_result

    def _handle_task_succeeded(self):
        """Call handler for task success"""
        if self.on_success:
            return self.on_success(self)
        else:
            return HandlerResult.cont()

    def _handle_task_not_succeeded(self):
        """
        Call handler for task which hasn't ended in 'succeeded' state
        (i.e. has either failed or been rescheduled)
        """
        result = self.async_result.result

        if isinstance(result, exceptions.OperationRetry):
            # operation explicitly requested a retry, so we ignore
            # the handler set on the task.
            handler_result = HandlerResult.retry()
            handler_result.retry_after = result.retry_after
            return handler_result

        if self.is_subgraph:
            self.error = self.failed_task.error

        if self.on_failure:
            handler_result = self.on_failure(self)
        else:
            handler_result = HandlerResult.retry()

            if isinstance(result, exceptions.NonRecoverableError):
                handler_result = HandlerResult.fail()
            elif isinstance(result, exceptions.RecoverableError):
                handler_result.retry_after = result.retry_after

        return handler_result

    def __str__(self):
        return '{0}({1})'.format(self.name, self.info or '')

    def duplicate_for_retry(self, execute_after):
        """
        :return: A new instance of this task with a new task id
        """
        dup = self._duplicate()
        dup.execute_after = execute_after
        dup.current_retries = self.current_retries + 1
        dup.retried_task = self.id
        if dup.cloudify_context and 'operation' in dup.cloudify_context:
            op_ctx = dup.cloudify_context['operation']
            op_ctx['retry_number'] = dup.current_retries
        dup.on_success = self.on_success
        dup.on_failure = self.on_failure
        return dup

    def _duplicate(self):
        raise NotImplementedError('Implemented by subclasses')

    @property
    def cloudify_context(self):
        raise NotImplementedError('Implemented by subclasses')

    @property
    def name(self):
        """
        :return: The task name
        """
        raise NotImplementedError('Implemented by subclasses')

    @property
    def short_description(self):
        """Short summary of the task.

        This is a human-readable representation of the task, should be
        clear and unambiguous enough for the operation to be able to
        uniquely identify the task.
        """
        return self.name

    @property
    def is_subgraph(self):
        return False


class RemoteWorkflowTask(WorkflowTask):
    """A WorkflowTask wrapping an AMQP based task

    This WorkflowTask will be sent via AMQP to a remote mgmtworker/agent.
    """
    def __init__(self,
                 kwargs,
                 cloudify_context,
                 workflow_context,
                 task_id=None,
                 task_queue=None,
                 task_target=None,
                 **kw):
        super(RemoteWorkflowTask, self).__init__(
            workflow_context,
            task_id,
            timeout=cloudify_context.get('timeout'),
            timeout_recoverable=cloudify_context.get('timeout_recoverable'),
            **kw)
        self._task_target = task_target
        self._task_queue = task_queue
        self._task_tenant = None
        self._kwargs = kwargs
        self._cloudify_context = cloudify_context
        self._cloudify_agent = None

    def __repr__(self):
        return '<{0} {1}: {2}>'.format(self.task_type, self.id, self.name)

    @classmethod
    def restore(cls, ctx, graph, task_descr):
        params = task_descr.parameters
        context = params['task_kwargs']['kwargs']['__cloudify_context']
        # RemoteWorkflowTask requires the context dict to be passed in
        params['task_kwargs']['cloudify_context'] = context

        # rest credentials for the operations are not stored, so can't be
        # resumed, but we'll use the up-to-date token from workflow ctx
        context['execution_token'] = ctx.execution_token
        return super(RemoteWorkflowTask, cls).restore(ctx, graph, task_descr)

    def dump(self):
        task = super(RemoteWorkflowTask, self).dump()

        # make a copy of kwargs.__cloudify_context, to remove the token
        # fields from the dumped representation, without mutating self._kwargs
        task['parameters']['task_kwargs'] = {'kwargs': self._kwargs.copy()}
        task['parameters']['task_kwargs']['kwargs']['__cloudify_context'] = \
            self._kwargs['__cloudify_context'].copy()
        for skipped_field in ['execution_token', 'rest_token']:
            task['parameters']['task_kwargs']['kwargs'][
                '__cloudify_context'].pop(skipped_field, None)
        return task

    @with_execute_after
    def apply_async(self):
        """Send the task to an agent.

        :return: a WorkflowTaskResult instance wrapping the async result
        """
        should_send = self._state in (TASK_PENDING, TASK_SENDING)
        if self._state == TASK_PENDING:
            self.set_state(TASK_SENDING)
        try:
            self._set_queue_kwargs()
            self.workflow_context.internal.handler.wait_for_result(
                self, self._task_target)
            if should_send:
                self.set_state(TASK_SENT)
                self.workflow_context.internal.handler.send_task(
                    self, self._task_target, self._task_queue)
        except (exceptions.NonRecoverableError,
                exceptions.RecoverableError) as e:
            self.error = serialize_known_exception(e)
            self.set_state(TASK_FAILED)
            self.async_result.result = e
        return self.async_result

    def is_local(self):
        return False

    def _duplicate(self):
        dup = RemoteWorkflowTask(kwargs=self._kwargs,
                                 task_queue=self.queue,
                                 task_target=self.target,
                                 cloudify_context=self.cloudify_context,
                                 workflow_context=self.workflow_context,
                                 task_id=None,  # we want a new task id
                                 info=self.info,
                                 total_retries=self.total_retries,
                                 retry_interval=self.retry_interval,
                                 send_task_events=self.send_task_events)
        dup.cloudify_context['task_id'] = dup.id
        return dup

    @property
    def name(self):
        """The task name"""
        return self.cloudify_context['task_name']

    @property
    def short_description(self):
        try:
            node_id = self.cloudify_context['node_id']
        except KeyError:
            node_id = '<unknown node>'
        try:
            operation_name = self.cloudify_context['operation']['name']
        except KeyError:
            operation_name = '<unknown operation>'
        return '{0} ({1}:{2})'.format(self.name, node_id, operation_name)

    @property
    def cloudify_context(self):
        return self._cloudify_context

    @property
    def target(self):
        """The task target (worker name)"""
        return self._task_target

    @property
    def queue(self):
        """The task queue"""
        return self._task_queue

    @property
    def kwargs(self):
        """kwargs to pass when invoking the task"""
        return self._kwargs

    def _set_queue_kwargs(self):
        rest_host = None
        if self._task_queue is None or self._task_target is None:
            queue, name, tenant, rest_host = self._get_queue_kwargs()
            if self._task_queue is None:
                self._task_queue = queue
            if self._task_target is None:
                self._task_target = name
            if self._task_tenant is None:
                self._task_tenant = tenant
        self.kwargs['__cloudify_context']['task_queue'] = self._task_queue
        self.kwargs['__cloudify_context']['task_target'] = self._task_target
        if rest_host:
            self.kwargs['__cloudify_context']['rest_host'] = rest_host

    @lru_cache()
    def _get_agent_settings(self, node_instance_id, deployment_id,
                            tenant=None):
        """Get the cloudify_agent dict and the tenant dict of the agent.

        This returns cloudify_agent of the actual agent, possibly available
        via deployment proxying.
        """
        client = get_rest_client(tenant)
        node_instance = client.node_instances.get(node_instance_id)
        host_id = node_instance.host_id
        if host_id == node_instance_id:
            host_node_instance = node_instance
        else:
            host_node_instance = client.node_instances.get(host_id)
        cloudify_agent = host_node_instance.runtime_properties.get(
            'cloudify_agent', {})

        # we found the actual agent, just return it
        if cloudify_agent.get('queue') and cloudify_agent.get('name'):
            return (
                cloudify_agent,
                self._get_tenant_dict(host_node_instance, tenant, client)
            )

        # this node instance isn't the real agent, check if it proxies to one.
        # Evaluate functions because proxy info might contain runtime
        # intrinsic functions (get_attributes/get_capabilities)
        node = client.nodes.get(
            deployment_id,
            host_node_instance.node_id,
            evaluate_functions=True
        )
        try:
            remote = node.properties['agent_config']['extra']['proxy']
            proxy_deployment = remote['deployment']
            proxy_node_instance = remote['node_instance']
            proxy_tenant = remote.get('tenant')
        except KeyError:
            # no queue information and no proxy - cannot continue
            missing = 'queue' if not cloudify_agent.get('queue') else 'name'
            raise exceptions.NonRecoverableError(
                '{0}: missing cloudify_agent.{1} runtime information. '
                'This most likely means that the Compute node was '
                'never started successfully'
                .format(host_node_instance.id, missing)
            )
        else:
            # the agent does proxy to another, recursively get from that one
            # (if the proxied-to agent in turn proxies to yet another one,
            # look up that one, etc)
            return self._get_agent_settings(
                node_instance_id=proxy_node_instance,
                deployment_id=proxy_deployment,
                tenant=proxy_tenant)

    def _get_tenant_dict(self, node_instance, tenant_name, client):
        if tenant_name is None or \
                tenant_name == self.cloudify_context['tenant']['name']:
            return self.cloudify_context['tenant']
        tenant = client.tenants.get(tenant_name)
        if tenant.get('rabbitmq_vhost') is None:
            raise exceptions.NonRecoverableError(
                '{0}: could not get RabbitMQ credentials for tenant {1}'
                .format(node_instance.id, tenant_name)
            )
        return tenant

    def _get_queue_kwargs(self):
        """Queue, name, and tenant of the agent that will execute the task

        This must return the values of the actual agent, possibly
        one that is available via deployment proxying.
        """
        executor = self.cloudify_context['executor']
        node_instance_id = self.cloudify_context['node_id']

        if executor == 'auto':
            # for executor=auto, we'll be a host_agent script if we do have
            # a host, otherwise CDA
            client = get_rest_client()
            node_instance = client.node_instances.get(node_instance_id)
            if node_instance.host_id:
                executor = 'host_agent'
            else:
                executor = 'central_deployment_agent'

        if executor == 'host_agent':
            if self._cloudify_agent is None:
                self._cloudify_agent, tenant = self._get_agent_settings(
                    node_instance_id=node_instance_id,
                    deployment_id=self.cloudify_context['deployment_id'],
                    tenant=None)
            return (self._cloudify_agent['queue'],
                    self._cloudify_agent['name'],
                    tenant,
                    self._cloudify_agent['rest_host'])
        else:
            return MGMTWORKER_QUEUE, MGMTWORKER_QUEUE, None, \
                self.workflow_context.rest_host


class LocalWorkflowTask(WorkflowTask):
    """A WorkflowTask wrapping a local callable"""

    def __init__(self,
                 local_task,
                 workflow_context,
                 node=None,
                 info=None,
                 total_retries=DEFAULT_TOTAL_RETRIES,
                 retry_interval=DEFAULT_RETRY_INTERVAL,
                 send_task_events=DEFAULT_SEND_TASK_EVENTS,
                 kwargs=None,
                 task_id=None,
                 name=None):
        """
        :param local_task: A callable
        :param workflow_context: the CloudifyWorkflowContext instance
        :param node: The CloudifyWorkflowNode instance (if in node context)
        :param info: A short description of this task (for logging)
        :param total_retries: Maximum retry attempt for this task, in case
                              the handlers return a retry attempt.
        :param retry_interval: Number of seconds to wait between retries
        :param kwargs: Local task keyword arguments
        :param name: optional parameter (default: local_task.__name__)
        """
        super(LocalWorkflowTask, self).__init__(
            info=info,
            total_retries=total_retries,
            retry_interval=retry_interval,
            task_id=task_id,
            workflow_context=workflow_context,
            send_task_events=send_task_events)
        self.local_task = local_task
        self.node = node
        self.kwargs = kwargs or {}
        self._name = name or local_task.__name__

    def __repr__(self):
        return '<{0} {1}: {2}>'.format(
            self.task_type, self.id, self.local_task)

    def dump(self):
        serialized = super(LocalWorkflowTask, self).dump()
        serialized['parameters']['task_kwargs'] = {'name': self._name}
        return serialized

    @classmethod
    def restore(cls, ctx, graph, task_descr):
        # task wasn't stored. Noqa because we do want to assign a lambda
        # here, that is a noop.
        local_task = lambda *a, **kw: None  # NOQA
        task_descr.parameters['task_kwargs']['local_task'] = local_task
        return super(LocalWorkflowTask, cls).restore(ctx, graph, task_descr)

    def _update_stored_state(self, state, **kwargs):
        # no need to store pre-run events, work up to it can safely
        # be redone; but do save when running cfy local, because then
        # regular plugin events are this class, and we want to display it
        if state in (TASK_SENT, TASK_STARTED) \
                and not self.workflow_context.local:
            return
        return super(LocalWorkflowTask, self)._update_stored_state(
            state, **kwargs)

    @with_execute_after
    def apply_async(self):
        """Execute the task in the local task thread pool

        :return: A wrapper for the task result
        """
        def local_task_wrapper():
            try:
                self.set_state(TASK_STARTED)
                result = self.local_task(**self.kwargs)
                self.set_state(TASK_SUCCEEDED, result=result)
                self.async_result.result = result
            except BaseException as e:
                if hasattr(e, 'wrapped_exc'):
                    e = e.wrapped_exc
                new_task_state = TASK_RESCHEDULED if isinstance(
                    e, exceptions.OperationRetry) else TASK_FAILED
                self.set_state(new_task_state, exception=e)
                self.async_result.result = e

        self.set_state(TASK_SENT)
        self.workflow_context.internal.add_local_task(local_task_wrapper)
        return self.async_result

    def is_local(self):
        return True

    def _duplicate(self):
        dup = LocalWorkflowTask(local_task=self.local_task,
                                workflow_context=self.workflow_context,
                                node=self.node,
                                info=self.info,
                                total_retries=self.total_retries,
                                retry_interval=self.retry_interval,
                                send_task_events=self.send_task_events,
                                kwargs=self.kwargs,
                                name=self.name)
        return dup

    @property
    def name(self):
        """The task name"""
        return self._name

    @property
    def cloudify_context(self):
        return self.kwargs.get('__cloudify_context')


class NOPLocalWorkflowTask(WorkflowTask):
    @property
    def name(self):
        """The task name"""
        return 'NOP'

    def _update_stored_state(self, state, **kwargs):
        # the task is always stored as pending - nothing to update
        pass

    def dump(self):
        stored = super(NOPLocalWorkflowTask, self).dump()
        stored['state'] = TASK_PENDING
        stored['parameters'].update({
            'info': None,
            'error': None
        })
        return stored

    @with_execute_after
    def apply_async(self):
        self.set_state(TASK_SUCCEEDED)
        self.async_result.result = None
        return self.async_result

    def is_nop(self):
        # If a nop has a success/failure handler, then it's not really
        # a nop, because it does _do_ something, and can't be just dropped
        # from the graph
        return self.on_success is None and self.on_failure is None

    def is_local(self):
        return True


class DryRunLocalWorkflowTask(LocalWorkflowTask):
    def apply_async(self):
        self.set_state(TASK_SUCCEEDED)
        self.async_result.result = None
        return self.async_result


class WorkflowTaskResult(object):
    """Deferred result of a WorkiflowTask

    This is returned by the WorkflowTask, and will eventually have
    the actual result set, when the task finishes. Use the on_result method
    to register callbacks to be called when the task result is set.
    """
    _NOT_SET = object()

    def __init__(self, task):
        self.task = task
        self._callbacks = []
        self._result = self._NOT_SET

    def on_result(self, f, *a, **kw):
        self._callbacks.append((f, a, kw))
        if self._result is not self._NOT_SET:
            f(self._result, *a, **kw)

    @property
    def result(self):
        if self._result is self._NOT_SET:
            raise RuntimeError('Result not set yet')
        return self._result

    @result.setter
    def result(self, result):
        if self._result is not self._NOT_SET:
            raise RuntimeError('Result already set')
        self._result = result
        for f, a, kw in self._callbacks:
            rv = f(self._result, *a, **kw)
            if rv is not None:
                self._result = rv

    def get(self, retry_on_failure=True):
        """Get the task result. Will block until the task execution ends.

        :return: The task result
        """
        done = threading.Event()

        api.cancel_callbacks.add(done.set)
        self.on_result(lambda _result: done.set())
        done.wait()
        api.cancel_callbacks.discard(done.set)

        if api.has_cancel_request():
            if self._result is self._NOT_SET:
                self.result = api.ExecutionCancelled()
                raise self.result

        ctx = self.task.workflow_context
        if not ctx.internal.graph_mode:
            ctx.internal.task_graph.remove_task(self.task)

        if self.task.get_state() in (TASK_FAILED, TASK_RESCHEDULED):
            handler_result = self.task.handle_task_terminated()
            if handler_result.retried_task and retry_on_failure:
                handler_result.retried_task.apply_async()
                return handler_result.retried_task.async_result.get()
            else:
                raise self.result
        return self._result


class HandlerResult(object):

    HANDLER_RETRY = 'handler_retry'
    HANDLER_FAIL = 'handler_fail'
    HANDLER_IGNORE = 'handler_ignore'
    HANDLER_CONTINUE = 'handler_continue'

    def __init__(self,
                 action,
                 ignore_total_retries=False,
                 retry_after=None):
        self.action = action
        self.ignore_total_retries = ignore_total_retries
        self.retry_after = retry_after

        # this field is filled by handle_terminated_task() below after
        # duplicating the task and updating the relevant task fields
        # or by a subgraph on_XXX handler
        self.retried_task = None

    def __repr__(self):
        return '<HandlerResult: {0}>'.format(self.action)

    @classmethod
    def retry(cls, ignore_total_retries=False, retry_after=None):
        return HandlerResult(cls.HANDLER_RETRY,
                             ignore_total_retries=ignore_total_retries,
                             retry_after=retry_after)

    @classmethod
    def fail(cls):
        return HandlerResult(cls.HANDLER_FAIL)

    @classmethod
    def cont(cls):
        return HandlerResult(cls.HANDLER_CONTINUE)

    @classmethod
    def ignore(cls):
        return HandlerResult(cls.HANDLER_IGNORE)


class _BuiltinTaskBase(WorkflowTask):
    def __init__(self, *args, **kwargs):
        kwargs.update(send_task_events=False, total_retries=0)
        super(_BuiltinTaskBase, self).__init__(*args, **kwargs)

    @property
    def name(self):
        return self.__class__.__name__

    def _duplicate(self):
        kwargs = {'workflow_context': self.workflow_context}
        kwargs.update(self.kwargs)
        return self.__class__(**kwargs)

    @property
    def cloudify_context(self):
        return {}

    @property
    def storage(self):
        """Shorthand for accessing the local storage.

        Only available in local workflows.
        """
        return workflow_ctx.internal.handler.storage

    @property
    def rest_client(self):
        """Shorthand for accessing the rest-client.

        Only available in remote workflows.
        """
        return workflow_ctx.internal.handler.rest_client

    def dump(self):
        serialized = super(_BuiltinTaskBase, self).dump()
        serialized['parameters']['task_kwargs'] = self.kwargs
        return serialized

    def apply_async(self):
        self.workflow_context.internal.add_local_task(self._run_task)
        return self.async_result

    def _run_task(self):
        func = self.local if self.workflow_context.local else self.remote
        try:
            result = func()
            self.set_state(TASK_SUCCEEDED, result=result)
            self.async_result.result = result
        except BaseException as e:
            self.workflow_context.logger.exception('error running %s', self)
            self.set_state(TASK_FAILED, exception=e)
            self.async_result.result = e

    def remote(self):
        pass

    def local(self):
        pass


class SetNodeInstanceStateTask(_BuiltinTaskBase):
    def __init__(self, node_instance_id, state, *args, **kwargs):
        self.kwargs = {'node_instance_id': node_instance_id, 'state': state}
        super(SetNodeInstanceStateTask, self).__init__(*args, **kwargs)
        self.info = state

    def remote(self):
        if self.stored:
            # no need to do anything - the server will update the state
            # automatically once we set this task state to SUCCEEDED
            return
        self.rest_client.node_instances.update(
            self.kwargs['node_instance_id'],
            state=self.kwargs['state'],
            force=True,
        )

    def local(self):
        self.storage.update_node_instance(
            self.kwargs['node_instance_id'],
            state=self.kwargs['state'],
            version=None)


class GetNodeInstanceStateTask(_BuiltinTaskBase):
    def __init__(self, node_instance_id, *args, **kwargs):
        self.kwargs = {'node_instance_id': node_instance_id}
        super(GetNodeInstanceStateTask, self).__init__(*args, **kwargs)
        self.info = node_instance_id

    def remote(self):
        return get_node_instance(self.kwargs['node_instance_id']).state

    def local(self):
        return self.storage.get_node_instance(
            self.kwargs['node_instance_id']).state


class SendNodeEventTask(_BuiltinTaskBase):
    def __init__(self, node_instance_id, event, additional_context,
                 *args, **kwargs):
        self.kwargs = {
            'node_instance_id': node_instance_id,
            'event': event,
            'additional_context': additional_context,
        }
        super(SendNodeEventTask, self).__init__(*args, **kwargs)
        self.info = event

    def remote(self):
        self._send(out_func=logs.manager_event_out)

    def local(self):
        self._send(out_func=logs.stdout_event_out)

    def _send(self, out_func):
        node_instance = workflow_ctx.get_node_instance(
            self.kwargs['node_instance_id'])
        logs.send_workflow_node_event(
            ctx=node_instance,
            event_type='workflow_node_event',
            message=self.kwargs['event'],
            additional_context=self.kwargs['additional_context'],
            out_func=out_func,
            # if this operation is stored, we don't need to send the log,
            # because updating the operation state will insert the log
            # automatically, on the manager side
            skip_send=self.stored)


class SendWorkflowEventTask(_BuiltinTaskBase):
    def __init__(self, event, event_type, event_args, additional_context,
                 *args, **kwargs):
        self.kwargs = {
            'event': event,
            'event_type': event_type,
            'event_args': event_args,
            'additional_context': additional_context,
        }
        super(SendWorkflowEventTask, self).__init__(*args, **kwargs)
        self.info = event

    # remote and local are the same
    def remote(self):
        return workflow_ctx.internal.send_workflow_event(
            event_type=self.kwargs['event_type'],
            message=self.kwargs['event'],
            args=self.kwargs['event_args'],
            additional_context=self.kwargs['additional_context']
        )

    def local(self):
        return self.remote()


class UpdateExecutionStatusTask(_BuiltinTaskBase):
    def __init__(self, status, *args, **kwargs):
        self.kwargs = {'status': status}
        super(UpdateExecutionStatusTask, self).__init__(*args, **kwargs)
        self.info = status

    def remote(self):
        update_execution_status(
            workflow_ctx.execution_id, self.kwargs['status'])

    def local(self):
        raise NotImplementedError(
            'Update execution status is not supported for '
            'local workflow execution')
