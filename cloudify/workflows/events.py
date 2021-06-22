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
                          out_func=logs.amqp_event_out,
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
    if state == TASK_SENDING:
        return 'sending_task'
    elif state == TASK_STARTED:
        return 'task_started'
    elif state == TASK_SUCCEEDED:
        return 'task_succeeded'
    elif state == TASK_RESCHEDULED:
        return 'task_rescheduled'
    elif state == TASK_FAILED:
        return 'task_failed'
    else:
        raise RuntimeError('unhandled event type: {0}'.format(state))


def format_event_message(name, state, result=None, exception=None,
                         current_retries=0, total_retries=0):
    exception_str = utils.format_exception(exception) if exception else None
    if state == TASK_SENDING:
        message = "Sending task '{0}'".format(name)
    elif state == TASK_STARTED:
        message = "Task started '{0}'".format(name)
    elif state == TASK_SUCCEEDED:
        result = str(result)
        suffix = ' ({0})'.format(result) if result not in ("'None'",
                                                           'None') else ''
        message = "Task succeeded '{0}{1}'".format(name, suffix)
    elif state == TASK_RESCHEDULED:
        message = "Task rescheduled '{0}'".format(name)
        if exception_str:
            message = '{0} -> {1}'.format(message, exception_str)
    elif state == TASK_FAILED:
        message = "Task failed '{0}'".format(name)
        if exception_str:
            message = "{0} -> {1}".format(message, exception_str)
    else:
        raise RuntimeError('unhandled event type: {0}'.format(state))

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
        task.name, state,
        event.get('result'), event.get('exception'),
        task.current_retries, task.total_retries
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
