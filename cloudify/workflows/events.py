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

from cloudify import logs, utils
from cloudify.constants import (
    TASK_SENDING,
    TASK_STARTED,
    TASK_RESCHEDULED,
    TASK_SUCCEEDED,
    TASK_FAILED,
)


def send_task_event_func_remote(task, event_type, message,
                                additional_context=None):
    _send_task_event_func(task, event_type, message,
                          out_func=logs.manager_event_out,
                          additional_context=additional_context)


def send_task_event_func_local(task, event_type, message,
                               additional_context=None):
    _send_task_event_func(task, event_type, message,
                          out_func=logs.stdout_event_out,
                          additional_context=additional_context)


def _send_task_event_func(task, event_type, message, out_func,
                          additional_context):
    if task.cloudify_context is None:
        logs.send_workflow_event(ctx=task.workflow_context,
                                 event_type=event_type,
                                 message=message,
                                 out_func=out_func,
                                 additional_context=additional_context)
    else:
        logs.send_task_event(cloudify_context=task.cloudify_context,
                             event_type=event_type,
                             message=message,
                             out_func=out_func,
                             additional_context=additional_context)


def _filter_task(task, state):
    return state != TASK_FAILED and not task.send_task_events


def get_event_type(state):
    try:
        return {
            TASK_SENDING: 'sending_task',
            TASK_STARTED: 'task_started',
            TASK_SUCCEEDED: 'task_succeeded',
            TASK_RESCHEDULED: 'task_rescheduled',
            TASK_FAILED: 'task_failed'
        }[state]
    except KeyError:
        raise RuntimeError('unhandled event type: {0}'.format(state))


def format_event_message(name, task_type, state, result=None, exception=None,
                         current_retries=0, total_retries=0, postfix=None):
    exception_str = utils.format_exception(exception) if exception else None
    try:
        message_template = {
            TASK_SENDING: "Sending task '{name}'",
            TASK_STARTED: "{type} started '{name}'",
            TASK_SUCCEEDED: "{type} succeeded '{name}'",
            TASK_RESCHEDULED: "{type} rescheduled '{name}'",
            TASK_FAILED: "{type} failed '{name}'",
        }[state]
    except KeyError:
        raise RuntimeError('unhandled task state: {0}'.format(state))
    message = message_template.format(
        name=name,
        type='Subgraph' if task_type == 'SubgraphTask' else 'Task'
    )

    if state == TASK_SUCCEEDED and result is not None:
        message = '{0} - {1}'.format(message, result)

    if state in (TASK_RESCHEDULED, TASK_FAILED) and exception_str:
        message = '{0} -> {1}'.format(message, exception_str)

    if postfix:
        message = '{0}{1}'.format(message, postfix)

    if current_retries > 0:
        retry = ' [retry {0}{1}]'.format(
            current_retries,
            '/{0}'.format(total_retries)
            if total_retries >= 0 else '')
        message = '{0}{1}'.format(message, retry)
    return message


def send_task_event(state, task, send_event_func, event):
    """
    Send a task event delegating to 'send_event_func'
    which will send events to RabbitMQ or use the workflow context logger
    in local context

    :param state: the task state (valid: ['sending', 'started', 'rescheduled',
                  'succeeded', 'failed'])
    :param task: a WorkflowTask instance to send the event for
    :param send_event_func: function for actually sending the event somewhere
    :param event: a dict with either a result field or an exception fields
                  follows celery event structure but used by local tasks as
                  well
    """
    if _filter_task(task, state):
        return

    if state in (TASK_FAILED, TASK_RESCHEDULED, TASK_SUCCEEDED) \
            and event is None:
        raise RuntimeError('Event for task {0} is None'.format(task.name))

    message = format_event_message(
        task.name, task.task_type, state,
        event.get('result'), event.get('exception'),
        task.current_retries, task.total_retries,
        postfix=' (dry run)' if task.workflow_context.dry_run else None
    )
    event_type = get_event_type(state)

    additional_context = {
        'task_current_retries': task.current_retries,
        'task_total_retries': task.total_retries
    }

    if state in (TASK_FAILED, TASK_RESCHEDULED):
        additional_context['task_error_causes'] = event.get('causes')

    send_event_func(task=task,
                    event_type=event_type,
                    message=message,
                    additional_context=additional_context)
