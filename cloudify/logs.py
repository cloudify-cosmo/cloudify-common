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
import datetime

from cloudify import constants, manager
from cloudify import event as _event
from cloudify.utils import (
    get_execution_creator_username,
    get_execution_id,
    is_management_environment,
    ENV_AGENT_LOG_LEVEL,
    ENV_AGENT_LOG_DIR,
    ENV_AGENT_LOG_MAX_BYTES,
    ENV_AGENT_LOG_MAX_HISTORY,
    get_manager_name,
    get_daemon_name,
)

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
        self.out_func = out_func or manager_log_out

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
    for sending log messages

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
    """Send a workflow event

    :param ctx: A CloudifyWorkflowContext instance
    :param event_type: The event type
    :param message: The message
    :param args: additional arguments that may be added to the message
    :param additional_context: additional context to be added to the context
    """
    _send_event(
        message_context_from_workflow_context(ctx),
        event_type, message, args, additional_context, out_func)


def send_workflow_node_event(ctx, event_type,
                             message=None,
                             args=None,
                             additional_context=None,
                             out_func=None,
                             skip_send=False):
    """Send a workflow node event

    :param ctx: A CloudifyWorkflowNode instance
    :param event_type: The event type
    :param message: The message
    :param args: additional arguments that may be added to the message
    :param additional_context: additional context to be added to the context
    :param skip_send: Only for internal use - do not send the event to the
                      manager, but log it to the logfile
    """
    _send_event(
        message_context_from_workflow_node_instance_context(ctx),
        event_type, message, args, additional_context, out_func,
        skip_send=skip_send)


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
    _send_event(
        message_context_from_cloudify_context(ctx),
        'plugin_event', message, args, additional_context, out_func)


def send_task_event(cloudify_context,
                    event_type,
                    message=None,
                    args=None,
                    additional_context=None,
                    out_func=None):
    """Send a task event

    :param cloudify_context: a __cloudify_context struct as passed to
                             operations
    :param event_type: The event type
    :param message: The message
    :param args: additional arguments that may be added to the message
    :param additional_context: additional context to be added to the context
    """
    # import here to avoid cyclic dependencies
    from cloudify.context import CloudifyContext
    ctx = CloudifyContext(cloudify_context)
    _send_event(
        message_context_from_cloudify_context(ctx),
        event_type, message, args, additional_context, out_func)


def _send_event(message_context, event_type,
                message, args, additional_context,
                out_func, **kwargs):
    additional_context = additional_context or {}
    message_context.update(additional_context)

    event = {
        'event_type': event_type,
        'context': message_context,
        'message': {
            'text': message,
            'arguments': args
        }
    }
    out_func = out_func or manager_event_out
    out_func(event, **kwargs)


def populate_base_item(item, message_type):
    # Adding 'Z' to match ISO format
    timestamp = '{0}Z'.format(datetime.datetime.utcnow().isoformat()[:-3])
    item['timestamp'] = timestamp
    item['message_code'] = None
    item['type'] = message_type


def manager_event_out(event, **kwargs):
    populate_base_item(event, 'cloudify_event')
    message_type = event['context'].get('message_type') or 'event'
    _publish_message(
        event, message_type, logging.getLogger('cloudify_events'), **kwargs)


def manager_log_out(log, **kwargs):
    populate_base_item(log, 'cloudify_log')
    _publish_message(log, 'log', logging.getLogger('cloudify_logs'), **kwargs)


def stdout_event_out(event, **kwargs):
    populate_base_item(event, 'cloudify_event')
    output = create_event_message_prefix(event)
    if output:
        sys.stdout.write('{0}\n'.format(output))
        sys.stdout.flush()


def stdout_log_out(log, **kwargs):
    populate_base_item(log, 'cloudify_log')
    output = create_event_message_prefix(log)
    if output:
        sys.stdout.write('{0}\n'.format(output))
        sys.stdout.flush()


def create_event_message_prefix(event, with_worker_names=False):
    event_obj = EVENT_CLASS(
        event,
        verbosity_level=EVENT_VERBOSITY_LEVEL,
        with_worker_names=with_worker_names,
    )
    if not event_obj.has_output:
        return None
    return str(event_obj)


def _log_message(logger, message):
    level = message.get('level', 'info')
    log_func = getattr(logger, level, logger.info)
    exec_id = message.get('context', {}).get('execution_id')
    execution_creator_username = message.get('context', {}).get(
        'execution_creator_username')
    msg = message['message']['text']
    try:
        node_id = message['context']['node_id']
        msg = u'[{0}] {1}'.format(node_id, msg)
    except KeyError:
        pass
    if exec_id:
        msg = u'[{0}] {1}'.format(exec_id, msg)
        if execution_creator_username:
            msg = u'[{0}] '.format(execution_creator_username) + msg
    log_func(msg)


def _publish_message(message, message_type, logger, skip_send=False):
    if u'message' in message:
        _log_message(logger, message)
    if skip_send:
        return
    execution_id = get_execution_id()
    client = manager.get_rest_client()
    if message_type == 'log':
        logs = [message]
        events = None
    else:
        logs = None
        events = [message]
    client.events.create(
        events=events,
        logs=logs,
        execution_id=execution_id,
        agent_name=get_daemon_name(),
        manager_name=get_manager_name(),
    )


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
    log_dir = os.environ.get(ENV_AGENT_LOG_DIR)
    if log_dir and os.path.exists(log_dir):
        log_dir = os.path.abspath(log_dir)
    else:
        log_dir = None

    setup_logger_base(
        os.environ.get(ENV_AGENT_LOG_LEVEL) or 'DEBUG',
        log_dir,
    )


def setup_agent_logger(log_name, log_level=None, log_dir=None,
                       max_bytes=None, max_history=None):
    if log_level is None:
        log_level = os.environ.get(ENV_AGENT_LOG_LEVEL) or 'DEBUG'

    if log_dir is None:
        log_dir = os.environ.get(ENV_AGENT_LOG_DIR)
        if log_dir:
            log_dir = os.path.realpath(os.path.abspath(log_dir), strict=True)

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
