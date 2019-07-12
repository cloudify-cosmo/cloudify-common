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

import sys
import time
import uuid
import Queue

from cloudify import exceptions, logs
from cloudify.workflows import api
from cloudify.manager import (
    get_rest_client,
    get_node_instance,
    update_execution_status,
    update_node_instance
)
from cloudify.constants import MGMTWORKER_QUEUE
from cloudify.state import workflow_ctx
from cloudify.utils import exception_to_error_cause
# imported for backwards compat:
from cloudify.utils import INSPECT_TIMEOUT  # noqa

INFINITE_TOTAL_RETRIES = -1
DEFAULT_TOTAL_RETRIES = INFINITE_TOTAL_RETRIES
DEFAULT_RETRY_INTERVAL = 30
DEFAULT_SUBGRAPH_TOTAL_RETRIES = 0

DEFAULT_SEND_TASK_EVENTS = True

TASK_PENDING = 'pending'
TASK_SENDING = 'sending'
TASK_SENT = 'sent'
TASK_STARTED = 'started'
TASK_RESCHEDULED = 'rescheduled'
TASK_SUCCEEDED = 'succeeded'
TASK_FAILED = 'failed'

TERMINATED_STATES = [TASK_RESCHEDULED, TASK_SUCCEEDED, TASK_FAILED]

DISPATCH_TASK = 'cloudify.dispatch.dispatch'


def retry_failure_handler(task):
    """Basic on_success/on_failure handler that always returns retry"""
    return HandlerResult.retry()


class WorkflowTask(object):
    """A base class for workflow tasks"""

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
        self.id = task_id or str(uuid.uuid4())
        self._state = TASK_PENDING
        self.async_result = None
        self.on_success = on_success
        self.on_failure = on_failure
        self.info = info
        self.error = None
        self.total_retries = total_retries
        self.retry_interval = retry_interval
        self.timeout = timeout
        self.timeout_recoverable = timeout_recoverable
        self.terminated = Queue.Queue(maxsize=1)
        self.is_terminated = False
        self.workflow_context = workflow_context
        self.send_task_events = send_task_events
        self.containing_subgraph = None

        self.current_retries = 0
        # timestamp for which the task should not be executed
        # by the task graph before reached, overridden by the task
        # graph during retries
        self.execute_after = time.time()
        self.stored = False

        # ID of the task that is being retried by this task
        self.retried_task = None

    @classmethod
    def restore(cls, ctx, graph, task_descr):
        params = task_descr.parameters
        task = cls(
            workflow_context=ctx,
            task_id=task_descr.id,
            info=params['info'],
            **params['task_kwargs']
        )
        task._state = task_descr.state
        task.current_retries = params['current_retries']
        task.send_task_events = params['send_task_events']
        task.containing_subgraph = params['containing_subgraph']
        task.stored = True
        return task

    def __repr__(self):
        return '<{0} {1}>'.format(self.task_type, self.id)

    @property
    def task_type(self):
        return self.__class__.__name__

    def dump(self):
        self.stored = True
        return {
            'id': self.id,
            'name': self.name,
            'state': self._state,
            'type': self.task_type,
            'parameters': {
                'retried_task': self.retried_task,
                'current_retries': self.current_retries,
                'send_task_events': self.send_task_events,
                'info': self.info,
                'error': self.error,
                'containing_subgraph': getattr(
                    self.containing_subgraph, 'id', None),
                'task_kwargs': {},
            }
        }

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

    def set_state(self, state):
        """
        Set the task state

        :param state: The state to set [pending, sending, sent, started,
                                        rescheduled, succeeded, failed]
        """
        if state not in [TASK_PENDING, TASK_SENDING, TASK_SENT, TASK_STARTED,
                         TASK_RESCHEDULED, TASK_SUCCEEDED, TASK_FAILED]:
            raise RuntimeError('Illegal state set on task: {0} '
                               '[task={1}]'.format(state, str(self)))
        if self.stored:
            self._update_stored_state(state)
        if self._state in TERMINATED_STATES:
            return
        self._state = state
        if state in TERMINATED_STATES:
            self.is_terminated = True
            self.terminated.put_nowait(True)

    def _update_stored_state(self, state):
        self.workflow_context.update_operation(self.id, state=state)

    def wait_for_terminated(self, timeout=None):
        if self.is_terminated:
            return
        self.terminated.get(timeout=timeout)

    def handle_task_terminated(self):
        if self.get_state() in (TASK_FAILED, TASK_RESCHEDULED):
            handler_result = self._handle_task_not_succeeded()
        else:
            handler_result = self._handle_task_succeeded()

        if handler_result.action == HandlerResult.HANDLER_RETRY:
            if any([self.total_retries == INFINITE_TOTAL_RETRIES,
                    self.current_retries < self.total_retries,
                    handler_result.ignore_total_retries]):
                if handler_result.retry_after is None:
                    handler_result.retry_after = self.retry_interval
                if handler_result.retried_task is None:
                    new_task = self.duplicate_for_retry(
                        time.time() + handler_result.retry_after)
                    handler_result.retried_task = new_task
            else:
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
                    subgraph.set_state(TASK_FAILED)
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

        try:
            exception = self.async_result.result
        except Exception as e:
            exception = exceptions.NonRecoverableError(
                'Could not de-serialize '
                'exception of task {0} --> {1}: {2}'
                .format(self.name,
                        type(e).__name__,
                        str(e)))

        if isinstance(exception, exceptions.OperationRetry):
            # operation explicitly requested a retry, so we ignore
            # the handler set on the task.
            handler_result = HandlerResult.retry()
        elif self.on_failure:
            handler_result = self.on_failure(self)
        else:
            handler_result = HandlerResult.retry()

        if handler_result.action == HandlerResult.HANDLER_RETRY:
            if isinstance(exception, exceptions.NonRecoverableError):
                handler_result = HandlerResult.fail()
            elif isinstance(exception, exceptions.RecoverableError):
                handler_result.retry_after = exception.retry_after

        if not self.is_subgraph:
            causes = []
            if isinstance(exception, exceptions.RecoverableError):
                causes = exception.causes or []
                if self.current_retries == self.total_retries:
                    causes.append({
                        'type': exceptions.NonRecoverableError.__name__,
                        'message': 'RecoverableError has occurred '
                        'at last retry. Causes {}'
                            .format([c['message'] for c in causes]),
                        'traceback': ''
                    })
            if isinstance(exception, exceptions.NonRecoverableError):
                causes = exception.causes or []
                causes.append({
                    'type': exceptions.NonRecoverableError.__name__,
                    'message': 'NonRecoverableError has occurred. Causes: {}'
                        .format([c['message'] for c in causes]),
                    'traceback': ''
                })
            if isinstance(self, LocalWorkflowTask):
                tb = self.async_result._holder.error[1]
                causes.append(exception_to_error_cause(exception, tb))
            self.workflow_context.internal.send_task_event(
                state=self.get_state(),
                task=self,
                event={'exception': exception, 'causes': causes})

        return handler_result

    def __str__(self):
        suffix = self.info if self.info is not None else ''
        return '{0}({1})'.format(self.name, suffix)

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
    def is_subgraph(self):
        return False

    def _should_resume(self):
        """Has this task already been sent and should be resumed?"""
        return (self._state in (TASK_SENT, TASK_STARTED) and
                self.async_result is None)

    def _can_resend(self):
        return False


class RemoteWorkflowTask(WorkflowTask):
    """A WorkflowTask wrapping an AMQP based task"""

    # cache for registered tasks queries to agent workers
    cache = {}

    def __init__(self,
                 kwargs,
                 cloudify_context,
                 workflow_context,
                 task_queue=None,
                 task_target=None,
                 task_id=None,
                 info=None,
                 on_success=None,
                 on_failure=retry_failure_handler,
                 total_retries=DEFAULT_TOTAL_RETRIES,
                 retry_interval=DEFAULT_RETRY_INTERVAL,
                 send_task_events=DEFAULT_SEND_TASK_EVENTS):
        """
        :param kwargs: The keyword argument this task will be invoked with
        :param cloudify_context: the cloudify context dict
        :param task_queue: the cloudify context dict
        :param task_target: the cloudify context dict
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
        super(RemoteWorkflowTask, self).__init__(
            workflow_context,
            task_id,
            info=info,
            on_success=on_success,
            on_failure=on_failure,
            total_retries=total_retries,
            retry_interval=retry_interval,
            timeout=cloudify_context.get('timeout'),
            timeout_recoverable=cloudify_context.get('timeout_recoverable'),
            send_task_events=send_task_events)
        self._task_target = task_target
        self._task_queue = task_queue
        self._task_tenant = None
        self._kwargs = kwargs
        self._cloudify_context = cloudify_context
        self._cloudify_agent = None

    def dump(self):
        task = super(RemoteWorkflowTask, self).dump()
        task['parameters']['task_kwargs'] = {
            'cloudify_context': self.cloudify_context,
            'kwargs': self._kwargs
        }
        return task

    def _update_stored_state(self, state):
        # no need to store SENDING - all work after SENDING but before SENT
        # can safely be rerun
        if state == TASK_SENDING:
            return
        # final states are set by the agent side
        if state in TERMINATED_STATES:
            # Unless we failed sending the task
            if not (state == TASK_FAILED and self._state in (TASK_SENDING,
                                                             TASK_SENT)):
                return
        return super(RemoteWorkflowTask, self)._update_stored_state(state)

    def apply_async(self):
        """
        Call the underlying tasks' apply_async. Verify the worker
        is alive and send an event before doing so.

        :return: a RemoteWorkflowTaskResult instance wrapping the async result
        """
        # the task should be sent to the agent if either:
        # 1) this is the first execution of this task (state=pending)
        # 2) this is a resume (state=sent|started) and the task is a central
        #    deployment agent task
        should_send = self._state == TASK_PENDING or self._should_resume()
        if self._state == TASK_PENDING:
            self.set_state(TASK_SENDING)
        try:
            self._set_queue_kwargs()
            if self._can_resend():
                self.kwargs['__cloudify_context']['resume'] = True
            task = self.workflow_context.internal.handler.get_task(
                self, queue=self._task_queue, target=self._task_target,
                tenant=self._task_tenant)
            async_result = self.workflow_context.internal.handler\
                .wait_for_result(self, task)
            if should_send:
                self.workflow_context.internal.send_task_event(
                    TASK_SENDING, self)
                self.set_state(TASK_SENT)
                self.workflow_context.internal.handler.send_task(self, task)
            self.async_result = RemoteWorkflowTaskResult(self, async_result)
        except (exceptions.NonRecoverableError,
                exceptions.RecoverableError) as e:
            self.set_state(TASK_FAILED)
            self.async_result = RemoteWorkflowErrorTaskResult(self, e)
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
                                 on_success=self.on_success,
                                 on_failure=self.on_failure,
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
        if self._task_queue is None or self._task_target is None:
            queue, name, tenant = self._get_queue_kwargs()
            if self._task_queue is None:
                self._task_queue = queue
            if self._task_target is None:
                self._task_target = name
            if self._task_tenant is None:
                self._task_tenant = tenant
        self.kwargs['__cloudify_context']['task_queue'] = self._task_queue
        self.kwargs['__cloudify_context']['task_target'] = self._task_target

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
            return cloudify_agent, self._get_tenant_dict(tenant, client)

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
                'Missing cloudify_agent.{0} runtime information. '
                'This most likely means that the Compute node was '
                'never started successfully'.format(missing))
        else:
            # the agent does proxy to another, recursively get from that one
            # (if the proxied-to agent in turn proxies to yet another one,
            # look up that one, etc)
            return self._get_agent_settings(
                node_instance_id=proxy_node_instance,
                deployment_id=proxy_deployment,
                tenant=proxy_tenant)

    def _get_tenant_dict(self, tenant_name, client):
        if tenant_name is None or \
                tenant_name == self.cloudify_context['tenant']['name']:
            return self.cloudify_context['tenant']
        tenant = client.tenants.get(tenant_name)
        if tenant.get('rabbitmq_vhost') is None:
            raise exceptions.NonRecoverableError(
                'Could not get RabbitMQ credentials for tenant {0}'
                .format(tenant_name))
        return tenant

    def _get_queue_kwargs(self):
        """Queue, name, and tenant of the agent that will execute the task

        This must return the values of the actual agent, possibly
        one that is available via deployment proxying.
        """
        executor = self.cloudify_context['executor']
        if executor == 'host_agent':
            if self._cloudify_agent is None:
                self._cloudify_agent, tenant = self._get_agent_settings(
                    node_instance_id=self.cloudify_context['node_id'],
                    deployment_id=self.cloudify_context['deployment_id'],
                    tenant=None)
            return (self._cloudify_agent['queue'],
                    self._cloudify_agent['name'],
                    tenant)
        else:
            return MGMTWORKER_QUEUE, MGMTWORKER_QUEUE, None

    def _can_resend(self):
        return (self.cloudify_context['executor'] != 'host_agent' and
                self._should_resume())


class LocalWorkflowTask(WorkflowTask):
    """A WorkflowTask wrapping a local callable"""

    def __init__(self,
                 local_task,
                 workflow_context,
                 node=None,
                 info=None,
                 on_success=None,
                 on_failure=retry_failure_handler,
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
        :param kwargs: Local task keyword arguments
        :param name: optional parameter (default: local_task.__name__)
        """
        super(LocalWorkflowTask, self).__init__(
            info=info,
            on_success=on_success,
            on_failure=on_failure,
            total_retries=total_retries,
            retry_interval=retry_interval,
            task_id=task_id,
            workflow_context=workflow_context,
            send_task_events=send_task_events)
        self.local_task = local_task
        self.node = node
        self.kwargs = kwargs or {}
        self._name = name or local_task.__name__

    def dump(self):
        serialized = super(LocalWorkflowTask, self).dump()
        if hasattr(self.local_task, 'dump'):
            serialized_local_task = self.local_task.dump()
        else:
            serialized_local_task = None
        serialized['parameters']['task_kwargs'] = {
            'name': self._name,
            'local_task': serialized_local_task
        }
        return serialized

    @classmethod
    def restore(cls, ctx, graph, task_descr):
        local_task_descr = task_descr.parameters['task_kwargs'].get(
            'local_task')
        if local_task_descr:
            local_task = _LocalTask.restore(local_task_descr)
        else:
            # task wasn't stored. Noqa because we do want to assign a lambda
            # here, that is a noop.
            local_task = lambda *a, **kw: None  # NOQA
        task_descr.parameters['task_kwargs']['local_task'] = local_task
        return super(LocalWorkflowTask, cls).restore(ctx, graph, task_descr)

    def _update_stored_state(self, state):
        # no need to store SENT - work up to it can safely be redone
        if state == TASK_SENT:
            return
        return super(LocalWorkflowTask, self)._update_stored_state(state)

    def apply_async(self):
        """
        Execute the task in the local task thread pool
        :return: A wrapper for the task result
        """

        def local_task_wrapper():
            try:
                self.workflow_context.internal.send_task_event(TASK_STARTED,
                                                               self)
                result = self.local_task(**self.kwargs)
                self.workflow_context.internal.send_task_event(
                    TASK_SUCCEEDED, self, event={'result': str(result)})
                self.async_result._holder.result = result
                self.set_state(TASK_SUCCEEDED)
            except BaseException as e:
                new_task_state = TASK_RESCHEDULED if isinstance(
                    e, exceptions.OperationRetry) else TASK_FAILED
                exc_type, exception, tb = sys.exc_info()
                self.async_result._holder.error = (exception, tb)
                self.set_state(new_task_state)

        self.async_result = LocalWorkflowTaskResult(self)

        self.workflow_context.internal.send_task_event(TASK_SENDING, self)
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
                                on_success=self.on_success,
                                on_failure=self.on_failure,
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


# NOP tasks class
class NOPLocalWorkflowTask(LocalWorkflowTask):

    def __init__(self, workflow_context, **kwargs):
        super(NOPLocalWorkflowTask, self).__init__(lambda: None,
                                                   workflow_context)

    @property
    def name(self):
        """The task name"""
        return 'NOP'

    def _update_stored_state(self, state):
        # the task is always stored as succeeded - nothing to update
        pass

    def dump(self):
        stored = super(NOPLocalWorkflowTask, self).dump()
        stored['state'] = TASK_SUCCEEDED
        stored['parameters'].update({
            'info': None,
            'error': None
        })
        return stored

    def apply_async(self):
        self.set_state(TASK_SUCCEEDED)
        return LocalWorkflowTaskResult(self)

    def is_nop(self):
        return True


# Dry run tasks class
class DryRunLocalWorkflowTask(LocalWorkflowTask):

    def apply_async(self):
        self.workflow_context.internal.send_task_event(TASK_SENDING, self)
        self.workflow_context.internal.send_task_event(TASK_STARTED, self)
        self.workflow_context.internal.send_task_event(
            TASK_SUCCEEDED,
            self,
            event={'result': 'dry run'}
        )
        self.set_state(TASK_SUCCEEDED)
        return LocalWorkflowTaskResult(self)


class WorkflowTaskResult(object):
    """A base wrapper for workflow task results"""

    def __init__(self, task):
        self.task = task

    def _process(self, retry_on_failure):
        if self.task.workflow_context.internal.graph_mode:
            return self._get()
        task_graph = self.task.workflow_context.internal.task_graph
        while True:
            self._wait_for_task_terminated()
            handler_result = self.task.handle_task_terminated()
            task_graph.remove_task(self.task)
            try:
                result = self._get()
                if handler_result.action != HandlerResult.HANDLER_RETRY:
                    return result
            except Exception:
                if (not retry_on_failure or
                        handler_result.action == HandlerResult.HANDLER_FAIL):
                    raise
            self._sleep(handler_result.retry_after)
            self.task = handler_result.retried_task
            task_graph.add_task(self.task)
            self._check_execution_cancelled()
            self.task.apply_async()
            self._refresh_state()

    @staticmethod
    def _check_execution_cancelled():
        if api.has_cancel_request():
            raise api.ExecutionCancelled()

    def _wait_for_task_terminated(self):
        while True:
            self._check_execution_cancelled()
            try:
                self.task.wait_for_terminated(timeout=1)
                break
            except Queue.Empty:
                continue

    def _sleep(self, seconds):
        while seconds > 0:
            self._check_execution_cancelled()
            sleep_time = 1 if seconds > 1 else seconds
            time.sleep(sleep_time)
            seconds -= sleep_time

    def get(self, retry_on_failure=True):
        """
        Get the task result.
        Will block until the task execution ends.

        :return: The task result
        """
        return self._process(retry_on_failure)

    def _get(self):
        raise NotImplementedError('Implemented by subclasses')

    def _refresh_state(self):
        raise NotImplementedError('Implemented by subclasses')


class RemoteWorkflowErrorTaskResult(WorkflowTaskResult):

    def __init__(self, task, exception):
        super(RemoteWorkflowErrorTaskResult, self).__init__(task)
        self.exception = exception

    def _get(self):
        raise self.exception

    @property
    def result(self):
        return self.exception


class RemoteWorkflowTaskResult(WorkflowTaskResult):
    def __init__(self, task, async_result):
        super(RemoteWorkflowTaskResult, self).__init__(task)
        self.async_result = async_result

    def _get(self):
        return self.async_result.get()

    def _refresh_state(self):
        self.async_result = self.task.async_result.async_result

    @property
    def result(self):
        return self.async_result.result


class LocalWorkflowTaskResult(WorkflowTaskResult):
    """A wrapper for local workflow task results"""

    class ResultHolder(object):

        def __init__(self, result=None, error=None):
            self.result = result
            self.error = error

    def __init__(self, task):
        """
        :param task: The LocalWorkflowTask instance
        """
        super(LocalWorkflowTaskResult, self).__init__(task)
        self._holder = self.ResultHolder()

    def _get(self):
        if self._holder.error is not None:
            exception, traceback = self._holder.error
            raise exception, None, traceback
        return self._holder.result

    def _refresh_state(self):
        self._holder = self.task.async_result._holder

    @property
    def result(self):
        if self._holder.error:
            return self._holder.error[0]
        else:
            return self._holder.result


class StubAsyncResult(object):
    """Stub async result that always returns None"""
    result = None


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


# Local tasks implementation
# A local task is a callable that will be passed to a LocalWorkflowTask,
# and then executed in a separate thread (see LocalTasksProcessing). Those
# tasks can implement .restore (classmethod) and .dump methods in order to
# be resumable.
# The user-facing interface for those tasks are node_instance.set_state,
# .send_event, etc.

class _LocalTask(object):
    """Base class for local tasks, containing utilities."""

    # all local task disable sending task events
    workflow_task_config = {'send_task_events': False}

    @property
    def __name__(self):
        # utility, also making subclasses be similar to plain functions
        return self.__class__.__name__

    # avoid calling .__subclasses__() many times
    _subclass_cache = None

    @classmethod
    def restore(cls, task_descr):
        """Rehydrate a _LocalTask instance from a dict description.

        The dict will contain a 'task' key and possibly a 'kwargs' key.
        Choose the appropriate subclass and return an instance of it.
        """
        if cls._subclass_cache is None:
            cls._subclass_cache = dict(
                (subcls.__name__, subcls) for subcls in cls.__subclasses__()
            )
        task_class = cls._subclass_cache[task_descr['task']]
        return task_class(**task_descr.get('kwargs') or {})

    # split local/remote on this level. This allows us to reuse implementation,
    # avoiding the need for separate local/remote subclasses.
    def __call__(self):
        if workflow_ctx.local:
            return self.local()
        else:
            return self.remote()

    def local(self):
        raise NotImplementedError('Implemented by subclasses')

    def remote(self):
        raise NotImplementedError('Implemented by subclasses')

    @property
    def storage(self):
        """Shorthand for accessing the local storage.

        Only available in local workflows.
        """
        return workflow_ctx.internal.handler.storage


class _SetNodeInstanceStateTask(_LocalTask):
    """A local task that sets a node instance state."""

    def __init__(self, node_instance_id, state):
        self._node_instance_id = node_instance_id
        self._state = state

    def dump(self):
        return {
            'task': self.__name__,
            'kwargs': {
                'node_instance_id': self._node_instance_id,
                'state': self._state
            }
        }

    def remote(self):
        node_instance = get_node_instance(self._node_instance_id)
        node_instance.state = self._state
        update_node_instance(node_instance)
        return node_instance

    def local(self):
        self.storage.update_node_instance(
            self._node_instance_id,
            state=self._state,
            version=None)


class _GetNodeInstanceStateTask(_LocalTask):
    """A local task that gets a node instance state."""

    def __init__(self, node_instance_id):
        self._node_instance_id = node_instance_id

    def dump(self):
        return {
            'task': self.__name__,
            'kwargs': {
                'node_instance_id': self._node_instance_id
            }
        }

    def remote(self):
        return get_node_instance(self._node_instance_id).state

    def local(self):
        instance = self.storage.get_node_instance(
            self._node_instance_id)
        return instance.state


class _SendNodeEventTask(_LocalTask):
    """A local task that sends a node event."""
    def __init__(self, node_instance_id, event, additional_context):
        self._node_instance_id = node_instance_id
        self._event = event
        self._additional_context = additional_context

    def dump(self):
        return {
            'task': self.__name__,
            'kwargs': {
                'node_instance_id': self._node_instance_id,
                'event': self._event,
                'additional_context': self._additional_context
            }
        }

    # local/remote only differ by the used output function
    def remote(self):
        self.send(out_func=logs.amqp_event_out)

    def local(self):
        self.send(out_func=logs.stdout_event_out)

    def send(self, out_func):
        node_instance = workflow_ctx.get_node_instance(
            self._node_instance_id)
        logs.send_workflow_node_event(
            ctx=node_instance,
            event_type='workflow_node_event',
            message=self._event,
            additional_context=self._additional_context,
            out_func=out_func)


class _SendWorkflowEventTask(_LocalTask):
    """A local task that sends a workflow event."""
    def __init__(self, event, event_type, args, additional_context=None):
        self._event = event
        self._event_type = event_type
        self._args = args
        self._additional_context = additional_context

    def dump(self):
        return {
            'task': self.__name__,
            'kwargs': {
                'event': self._event,
                'event_type': self.event_type,
                'args': self._args,
                'additional_context': self._additional_context
            }
        }

    def __call__(self):
        return workflow_ctx.internal.send_workflow_event(
            event_type=self._event_type,
            message=self._event,
            args=self._args,
            additional_context=self._additional_context
        )


class _UpdateExecutionStatusTask(_LocalTask):
    """A local task that sets the execution status."""
    def __init__(self, status):
        self._status = status

    def dump(self):
        return {
            'task': self.__class__.__name__,
            'kwargs': {
                'status': self._status,
            }
        }

    def remote(self):
        update_execution_status(
            workflow_ctx.execution_id, self._status)

    def local(self):
        raise NotImplementedError(
            'Update execution status is not supported for '
            'local workflow execution')
