########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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

import os
import sys
import logging.config
import logging.handlers
import json
import datetime
from functools import wraps

from cloudify import constants
from cloudify import amqp_client_utils
from cloudify import event as _event
from cloudify.utils import (get_execution_creator_username,
                            is_management_environment,
                            ENV_AGENT_LOG_LEVEL,
                            ENV_AGENT_LOG_DIR,
                            ENV_AGENT_LOG_MAX_BYTES,
                            ENV_AGENT_LOG_MAX_HISTORY)
from cloudify.exceptions import ClosedAMQPClientException
from cloudify._compat import text_type

EVENT_CLASS = _event.Event
EVENT_VERBOSITY_LEVEL = _event.NO_VERBOSE


def message_context_from_cloudify_context(ctx):
    """Build a message context from a CloudifyContext instance"""

    context = {
        'blueprint_id': ctx.blueprint.id,
        'deployment_id': ctx.deployment.id,
        'execution_id': ctx.execution_id,
        'workflow_id': ctx.workflow_id,
        'task_id': ctx.task_id,
        'task_name': ctx.task_name,
        'task_queue': ctx.task_queue,
        'task_target': ctx.task_target,
        'operation': ctx.operation.name,
        'plugin': ctx.plugin,
        'tenant': {'name': ctx.tenant_name},
    }
    if ctx.type == constants.NODE_INSTANCE:
        context['node_id'] = ctx.instance.id
        context['node_name'] = ctx.node.name
    elif ctx.type == constants.RELATIONSHIP_INSTANCE:
        context['source_id'] = ctx.source.instance.id
        context['source_name'] = ctx.source.node.name
        context['target_id'] = ctx.target.instance.id
        context['target_name'] = ctx.target.node.name

    return context


def message_context_from_workflow_context(ctx):
    """Build a message context from a CloudifyWorkflowContext instance"""
    return {
        'blueprint_id': ctx.blueprint.id,
        'deployment_id': ctx.deployment.id,
        'execution_id': ctx.execution_id,
        'workflow_id': ctx.workflow_id,
        'tenant': {'name': ctx.tenant_name},
    }


def message_context_from_sys_wide_wf_context(ctx):
    """Build a message context from a CloudifyWorkflowContext instance"""
    return {
        'blueprint_id': None,
        'deployment_id': None,
        'execution_id': ctx.execution_id,
        'workflow_id': ctx.workflow_id,
        'tenant': {'name': ctx.tenant_name},
    }


def message_context_from_workflow_node_instance_context(ctx):
    """Build a message context from a CloudifyWorkflowNode instance"""
    message_context = message_context_from_workflow_context(ctx.ctx)
    message_context.update({
        'node_name': ctx.node_id,
        'node_id': ctx.id,
    })
    return message_context


class CloudifyBaseLoggingHandler(logging.Handler):
    """A base handler class for writing log messages to RabbitMQ"""

    def __init__(self, ctx, out_func, message_context_builder):
        logging.Handler.__init__(self)
        self.context = message_context_builder(ctx)
        self.out_func = out_func or amqp_log_out

    def flush(self):
        pass

    def emit(self, record):
        message = self.format(record)
        log = {
            'context': self.context,
            'logger': record.name,
            'level': record.levelname.lower(),
            'message': {
                'text': message
            }
        }
        self.out_func(log)


class CloudifyPluginLoggingHandler(CloudifyBaseLoggingHandler):
    """A handler class for writing plugin log messages to RabbitMQ"""
    def __init__(self, ctx, out_func=None):
        CloudifyBaseLoggingHandler.__init__(
            self, ctx, out_func, message_context_from_cloudify_context)


class CloudifyWorkflowLoggingHandler(CloudifyBaseLoggingHandler):
    """A Handler class for writing workflow log messages to RabbitMQ"""
    def __init__(self, ctx, out_func=None):
        CloudifyBaseLoggingHandler.__init__(
            self, ctx, out_func, message_context_from_workflow_context)


class SystemWideWorkflowLoggingHandler(CloudifyBaseLoggingHandler):
    """Class for writing system-wide workflow log messages to RabbitMQ"""
    def __init__(self, ctx, out_func=None):
        CloudifyBaseLoggingHandler.__init__(
            self, ctx, out_func, message_context_from_sys_wide_wf_context)


class CloudifyWorkflowNodeLoggingHandler(CloudifyBaseLoggingHandler):
    """A Handler class for writing workflow nodes log messages to RabbitMQ"""
    def __init__(self, ctx, out_func=None):
        CloudifyBaseLoggingHandler.__init__(
            self, ctx, out_func,
            message_context_from_workflow_node_instance_context)


class CloudifyCtxLoggingHandler(logging.Handler):
    """
    A logging handler for Cloudify's context logger.
    A logger attached to this handler will result in logging being passed
    through to the Cloudify logger.
    This is useful for plugins that would like to have underlying APIs'
    loggers flow through to the Cloudify context logger.
    """
    def __init__(self, ctx):
        logging.Handler.__init__(self)
        self.ctx = ctx

    def emit(self, record):
        message = self.format(record)
        self.ctx.logger.log(record.levelno, message)


def init_cloudify_logger(handler, logger_name,
                         logging_level=logging.DEBUG):
    """
    Instantiate an amqp backed logger based on the provided handler
    for sending log messages to RabbitMQ

    :param handler: A logger handler based on the context
    :param logger_name: The logger name
    :param logging_level: The logging level
    :return: An amqp backed logger
    """

    # TODO: somehow inject logging level (no one currently passes
    # logging_level)
    logger = logging.getLogger('ctx.{0}'.format(logger_name))
    logger.setLevel(logging_level)
    for h in logger.handlers:
        logger.removeHandler(h)
    handler.setFormatter(logging.Formatter("%(message)s"))
    handler.setLevel(logging_level)
    logger.addHandler(handler)
    return logger


def send_workflow_event(ctx, event_type,
                        message=None,
                        args=None,
                        additional_context=None,
                        out_func=None):
    """Send a workflow event to RabbitMQ

    :param ctx: A CloudifyWorkflowContext instance
    :param event_type: The event type
    :param message: The message
    :param args: additional arguments that may be added to the message
    :param additional_context: additional context to be added to the context
    """
    _send_event(ctx, 'workflow', event_type, message, args,
                additional_context, out_func)


def send_sys_wide_wf_event(ctx, event_type, message=None, args=None,
                           additional_context=None, out_func=None):
    """Send a workflow event to RabbitMQ

    :param ctx: A CloudifySystemWideWorkflowContext instance
    :param event_type: The event type
    :param message: The message
    :param args: additional arguments that may be added to the message
    :param additional_context: additional context to be added to the context
    """
    _send_event(ctx, 'system_wide_workflow', event_type, message, args,
                additional_context, out_func)


def send_workflow_node_event(ctx, event_type,
                             message=None,
                             args=None,
                             additional_context=None,
                             out_func=None):
    """Send a workflow node event to RabbitMQ

    :param ctx: A CloudifyWorkflowNode instance
    :param event_type: The event type
    :param message: The message
    :param args: additional arguments that may be added to the message
    :param additional_context: additional context to be added to the context
    """
    _send_event(ctx, 'workflow_node', event_type, message, args,
                additional_context, out_func)


def send_plugin_event(ctx,
                      message=None,
                      args=None,
                      additional_context=None,
                      out_func=None):
    """Send a plugin event to RabbitMQ

    :param ctx: A CloudifyContext instance
    :param message: The message
    :param args: additional arguments that may be added to the message
    :param additional_context: additional context to be added to the context
    """
    _send_event(ctx, 'plugin', 'plugin_event', message, args,
                additional_context, out_func)


def send_task_event(cloudify_context,
                    event_type,
                    message=None,
                    args=None,
                    additional_context=None,
                    out_func=None):
    """Send a task event to RabbitMQ

    :param cloudify_context: a __cloudify_context struct as passed to
                             operations
    :param event_type: The event type
    :param message: The message
    :param args: additional arguments that may be added to the message
    :param additional_context: additional context to be added to the context
    """
    # import here to avoid cyclic dependencies
    from cloudify.context import CloudifyContext
    _send_event(CloudifyContext(cloudify_context),
                'task', event_type, message, args,
                additional_context,
                out_func)


def _send_event(ctx, context_type, event_type,
                message, args, additional_context,
                out_func):
    if context_type in ['plugin', 'task']:
        message_context = message_context_from_cloudify_context(
            ctx)
    elif context_type == 'workflow':
        message_context = message_context_from_workflow_context(ctx)
    elif context_type == 'workflow_node':
        message_context = message_context_from_workflow_node_instance_context(
            ctx)
    elif context_type == 'system_wide_workflow':
        message_context = message_context_from_sys_wide_wf_context(ctx)
    else:
        raise RuntimeError('Invalid context_type: {0}'.format(context_type))

    additional_context = additional_context or {}
    message_context.update(additional_context)

    if hasattr(ctx, 'execution_creator_username'):
        if ctx.execution_creator_username:
            message_context.update({'execution_creator_username':
                                    ctx.execution_creator_username})
    event = {
        'event_type': event_type,
        'context': message_context,
        'message': {
            'text': message,
            'arguments': args
        }
    }
    out_func = out_func or amqp_event_out
    out_func(event)


def populate_base_item(item, message_type):
    # Adding 'Z' to match ISO format
    timestamp = '{0}Z'.format(datetime.datetime.utcnow().isoformat()[:-3])
    item['timestamp'] = timestamp
    item['message_code'] = None
    item['type'] = message_type


def amqp_event_out(event):
    populate_base_item(event, 'cloudify_event')
    message_type = event['context'].get('message_type') or 'event'
    _publish_message(event, message_type, logging.getLogger('cloudify_events'))


def amqp_log_out(log):
    populate_base_item(log, 'cloudify_log')
    _publish_message(log, 'log', logging.getLogger('cloudify_logs'))


def stdout_event_out(event):
    populate_base_item(event, 'cloudify_event')
    output = create_event_message_prefix(event)
    if output:
        sys.stdout.write('{0}\n'.format(output))
        sys.stdout.flush()


def stdout_log_out(log):
    populate_base_item(log, 'cloudify_log')
    output = create_event_message_prefix(log)
    if output:
        sys.stdout.write('{0}\n'.format(output))
        sys.stdout.flush()


def create_event_message_prefix(event):
    event_obj = EVENT_CLASS(event, verbosity_level=EVENT_VERBOSITY_LEVEL)
    if not event_obj.has_output:
        return None
    return text_type(event_obj)


def with_amqp_client(func):
    @wraps(func)
    def wrapper(*args, **kwargs):
        """Calls the wrapped func with an AMQP client instance."""
        # call the wrapped func with the amqp client
        with amqp_client_utils.global_management_events_publisher as client:
            return func(client, *args, **kwargs)

    return wrapper


@with_amqp_client
def _publish_message(client, message, message_type, logger):
    try:
        client.publish_message(message, message_type)
    except ClosedAMQPClientException:
        raise
    except BaseException as e:
        logger.warning(
            'Error publishing {0} to RabbitMQ ({1})[message={2}]'
            .format(message_type,
                    '{0}: {1}'.format(type(e).__name__, e),
                    json.dumps(message)))


def setup_logger_base(log_level, log_dir=None):
    # for python 2.6 compat, we translate the string to the actual number
    # that is required (like logging.DEBUG etc)
    log_level = logging.getLevelName(log_level)

    console_formatter = logging.Formatter(
        '%(asctime)s:%(levelname)s: %(message)s')
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)
    console_handler.setFormatter(console_formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(console_handler)

    # the 'ctx' logger is for ctx.logger and will be handled by one
    # of the PluginHandlers
    context_logger = logging.getLogger('ctx')
    context_logger.propagate = False

    if log_dir and not os.path.exists(log_dir):
        os.mkdir(log_dir)

    # silence pika and http loggers so that even if the agent is logging on
    # DEBUG, we're not getting all the uninteresting AMQP/HTTP information
    logging.getLogger('pika').setLevel(logging.WARNING)
    logging.getLogger('cloudify.rest_client.http').setLevel(logging.INFO)


def setup_subprocess_logger():
    setup_logger_base(os.environ.get(ENV_AGENT_LOG_LEVEL) or 'DEBUG',
                      os.environ.get(ENV_AGENT_LOG_DIR))


def setup_agent_logger(log_name, log_level=None, log_dir=None,
                       max_bytes=None, max_history=None):
    if log_level is None:
        log_level = os.environ.get(ENV_AGENT_LOG_LEVEL) or 'DEBUG'

    if log_dir is None:
        log_dir = os.environ.get(ENV_AGENT_LOG_DIR)

    setup_logger_base(log_level, log_dir)

    worker_logger = logging.getLogger('worker')
    dispatch_logger = logging.getLogger('dispatch')

    for logger in [worker_logger, dispatch_logger]:
        logger.setLevel(log_level)

    if log_dir:
        log_file = os.path.join(log_dir, '{0}.log'.format(log_name))

        # On the manager, we for sure have a logrotate policy.
        if is_management_environment():
            file_handler = logging.handlers.WatchedFileHandler(log_file)
        else:
            # These are only needed on agents, as mgmtworker
            # has logrotate available.
            # We explicitly assume that AGENT_LOG_MAX_BYTES and
            # AGENT_LOG_MAX_HISTORY exist in the environment, because
            # we don't want to deal with hard-coded defaults at this point
            # in the code flow. We're OK to assume that these environment
            # variables are defined in the agent's service configuration file
            # that had been rendered during agent installation.
            if max_bytes is None:
                max_bytes = int(os.environ[ENV_AGENT_LOG_MAX_BYTES])

            if max_history is None:
                max_history = int(os.environ[ENV_AGENT_LOG_MAX_HISTORY])

            # On linux agents, we may have logrotate in place in the future.
            # On Windows agents, there's no reliable logrotate alternative.
            # So, use Python's RotatingFileHandler which works OK on both.
            file_handler = logging.handlers.RotatingFileHandler(
                filename=log_file, maxBytes=max_bytes,
                backupCount=max_history)
        file_handler.setLevel(log_level)
        file_handler.setFormatter(ExecutorNameFormatter())

        root_logger = logging.getLogger()
        root_logger.addHandler(file_handler)


class ExecutorNameFormatter(logging.Formatter):
    default_fmt = logging.Formatter(' %(asctime)-15s - %(name)s - '
                                    '%(levelname)s - %(message)s')
    user_fmt = logging.Formatter(' %(asctime)-15s - %(name)s - '
                                 '%(levelname)s - %(username)s - %(message)s')

    def format(self, record):
        username = get_execution_creator_username()
        if username:
            record.username = username
            return self.user_fmt.format(record)

        return self.default_fmt.format(record)
