"""AMQP dispatcher: send tasks to remote mgmtworkers/agents

This makes an AMQP connection to the vhost defined by the task, and sends
the task encoded as JSON over it.
The task is received and executed by the remote agent/mgmtworker, and the
response is received here, based on the correlation-id.
When the response is received, the task's WorkflowTaskResult receives the
result and fires the callbacks.
"""

import json
import logging
import pika

from cloudify import amqp_client, exceptions, utils
from cloudify.constants import MGMTWORKER_QUEUE
from cloudify.error_handling import deserialize_known_exception
from cloudify.state import current_workflow_ctx
from cloudify.workflows.tasks import (
    TASK_FAILED,
    TASK_SUCCEEDED,
    TASK_RESCHEDULED,
)


class _WorkflowTaskHandler(object):
    """An AMQP handler to be used with the AMQPConnection for sending tasks

    This is the amqp-level component that sends the given task over AMQP,
    and reads incoming responses. The responses are going to be correlated
    with waited-for tasks, and if found, the result will be set.
    Only one of these handlers should be attached to any given AMQPConnection.
    """
    def __init__(self, workflow_ctx):
        self._logger = logging.getLogger('dispatch')
        self.workflow_ctx = workflow_ctx
        workflow_ctx.amqp_handlers.add(self)
        self._queue_name = 'execution_responses_{0}'.format(
            workflow_ctx.execution_id)
        self._connection = None
        self._responses = {}
        self._tasks = {}
        self._bound = set()

    def wait_for_task(self, task):
        if task.id in self._responses:
            response, delivery_tag = self._responses.pop(task.id)
            self._task_callback(task, response)
            self._connection.channel_method(
                'basic_ack', delivery_tag=delivery_tag)
        else:
            self._tasks[task.id] = task

    def register(self, connection, channel):
        self._connection = connection
        channel.exchange_declare(exchange=MGMTWORKER_QUEUE,
                                 auto_delete=False,
                                 durable=True,
                                 exchange_type='direct')
        channel.queue_declare(
            queue=self._queue_name, durable=True, auto_delete=False)

        channel.basic_consume(
            queue=self._queue_name, on_message_callback=self.process)

    def process(self, channel, method, properties, body):
        try:
            response = json.loads(body.decode('utf-8'))
        except ValueError:
            self._logger.error('Error parsing response: %s', body)
            channel.basic_ack(method.delivery_tag)
            return
        if properties.correlation_id in self._tasks:
            task = self._tasks.pop(properties.correlation_id)
            if not response['ok']:
                task.error = response.get('error')
            self._task_callback(task, response)
            channel.basic_ack(method.delivery_tag)
        else:
            self._responses[properties.correlation_id] = \
                (response, method.delivery_tag)

    def publish(self, target, message, correlation_id, routing_key):
        if target not in self._bound:
            self._bound.add(target)
            self._connection.channel_method(
                'queue_bind',
                queue=self._queue_name, exchange=target,
                routing_key=self._queue_name)
        self._connection.publish({
            'exchange': target,
            'body': json.dumps(message),
            'properties': pika.BasicProperties(
                reply_to=self._queue_name,
                correlation_id=correlation_id),
            'routing_key': routing_key,
        })
        if self._task_deletes_exchange(message):
            self._clear_bound_exchanges_cache()

    def _task_deletes_exchange(self, message):
        """Does this task delete an amqp exchange?

        Agent delete tasks are going to delete the agent's amqp exchange,
        so we'll have to bind it again, if the agent is reinstalled
        (eg. in a deployment-update workflow) - so we'll bust the ._bound
        cache in that case.
        """
        try:
            name = message['cloudify_task']['kwargs'][
                '__cloudify_context']['operation']['name']
            return name == 'cloudify.interfaces.cloudify_agent.delete'
        except (KeyError, TypeError):
            return False

    def _clear_bound_exchanges_cache(self):
        for handler in self.workflow_ctx.amqp_handlers:
            handler._bound.clear()

    def delete_queue(self):
        self._connection.channel_method(
            'queue_delete', queue=self._queue_name, if_empty=True, wait=True)

    def _task_callback(self, task, response):
        self._logger.debug('[%s] Response received - %s', task.id, response)
        try:
            if not response or task.is_terminated:
                return

            error = response.get('error')
            if error:
                exception = deserialize_known_exception(error)
                if isinstance(exception, exceptions.OperationRetry):
                    state = TASK_RESCHEDULED
                else:
                    state = TASK_FAILED
                self._set_task_state(task, state, exception=exception)
                task.async_result.result = exception
            else:
                state = TASK_SUCCEEDED
                result = response.get('result')
                self._set_task_state(task, state, result=result)
                task.async_result.result = result
        except Exception:
            self._logger.error('Error occurred while processing task',
                               exc_info=True)
            raise

    def _set_task_state(self, task, state, **kwargs):
        with current_workflow_ctx.push(task.workflow_context):
            task.set_state(state, **kwargs)


class TaskDispatcher(object):
    def __init__(self, workflow_ctx):
        self.workflow_ctx = workflow_ctx
        self._logger = logging.getLogger('dispatch')
        self._clients = {}

    def cleanup(self):
        for client, handler in self._clients.values():
            handler.delete_queue()
            client.close()

    def get_client(self, target):
        if target == MGMTWORKER_QUEUE:
            if None not in self._clients:
                client = amqp_client.get_client()
                handler = _WorkflowTaskHandler(self.workflow_ctx)
                client.add_handler(handler)
                client.consume_in_thread()
                self._clients[None] = (client, handler)
            client, handler = self._clients[None]
        else:
            tenant = utils.get_tenant()
            if tenant.rabbitmq_vhost not in self._clients:
                client = amqp_client.get_client(
                    amqp_user=tenant.rabbitmq_username,
                    amqp_pass=tenant.rabbitmq_password,
                    amqp_vhost=tenant.rabbitmq_vhost
                )
                handler = _WorkflowTaskHandler(self.workflow_ctx)
                client.add_handler(handler)
                client.consume_in_thread()
                self._clients[tenant.rabbitmq_vhost] = (client, handler)
            client, handler = self._clients[tenant.rabbitmq_vhost]
        return client, handler

    def send_task(self, task, target, queue):
        """Send the given RemoteWorkflowTask to the target, over the queue.

        The task is going to be encoded into json, and sent to AMQP
        together with a unique correlation-id (which is then a way to
        figure out which response message ties to which task).
        """
        client, handler = self.get_client(target)
        if target != MGMTWORKER_QUEUE and \
                not amqp_client.is_agent_alive(target, client, connect=False):
            raise exceptions.RecoverableError(
                'Timed out waiting for agent: {0}'.format(target))

        message = {
            'id': task.id,
            'cloudify_task': {'kwargs': task.kwargs}
        }
        handler.publish(queue, message, routing_key='operation',
                        correlation_id=task.id)
        self._logger.debug('Task [%s] sent', task.id)

    def wait_for_result(self, task, target):
        """Register the task to receive a result

        This registers the given RemoteWorkflowTask with the AMQP connection,
        so that when a response comes in, the RemoteWorkflowTask's result
        is set.
        """
        client, handler = self.get_client(target)
        handler.wait_for_task(task)
