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

from collections import deque
import copy
import json
import logging
import os
import queue
import random
import ssl
import threading
import time

import pika
import pika.exceptions

from cloudify import broker_config, utils
from cloudify.constants import INSPECT_TIMEOUT

# keep compat with both pika 0.11 and pika 1.1: switch the calls based
# on this flag. We're keeping compat with 0.11 for the py2.6 agent on rhel6.
# all pika calls that are different between 0.11 and 1.1, must be under
# if statements on this flag.
OLD_PIKA = not hasattr(pika, 'SSLOptions')

logger = logging.getLogger(__name__)


# requires 2.7+
def wait_for_event(evt, poll_interval=0.5):
    """Wait for a threading.Event by polling, to allow handling of signals.
    (ie. does not block ^C)
    """
    while True:
        if evt.wait(poll_interval):
            return


class AMQPParams(object):
    def __init__(self,
                 amqp_host=None,
                 amqp_user=None,
                 amqp_pass=None,
                 amqp_port=None,
                 amqp_vhost=None,
                 ssl_enabled=None,
                 ssl_cert_path=None,
                 ssl_cert_data=None,
                 socket_timeout=3,
                 heartbeat_interval=None):
        super(AMQPParams, self).__init__()
        username = amqp_user or broker_config.broker_username
        password = amqp_pass or broker_config.broker_password
        heartbeat = heartbeat_interval or broker_config.broker_heartbeat
        ssl_enabled = ssl_enabled or broker_config.broker_ssl_enabled
        ssl_cert_path = ssl_cert_path or broker_config.broker_cert_path
        credentials = pika.credentials.PlainCredentials(
            username=username,
            password=password,
        )

        self.raw_host = amqp_host or broker_config.broker_hostname
        self._amqp_params = {
            'port': amqp_port or broker_config.broker_port,
            'virtual_host': amqp_vhost or broker_config.broker_vhost,
            'credentials': credentials,
            'heartbeat': heartbeat,
            'socket_timeout': socket_timeout
        }
        if OLD_PIKA:
            if ssl_cert_data:
                raise RuntimeError(
                    'Passing in cert content is incompatible with old pika')
            self._amqp_params['ssl'] = ssl_enabled
            if ssl_enabled:
                self._amqp_params['ssl_options'] = {
                    'cert_reqs': ssl.CERT_REQUIRED,
                    'ca_certs': ssl_cert_path,
                }
            else:
                self._amqp_params['ssl_options'] = {}
        else:
            if ssl_enabled:
                if ssl_cert_data:
                    ssl_context = ssl.create_default_context(
                        cadata=ssl_cert_data)
                elif ssl_cert_path:
                    ssl_context = ssl.create_default_context(
                        cafile=ssl_cert_path)
                else:
                    raise RuntimeError(
                        'When ssl is enabled, ssl_cert_path or ssl_cert_data '
                        'must be provided')
                self._amqp_params['ssl_options'] = pika.SSLOptions(ssl_context)

    def as_pika_params(self):
        return pika.ConnectionParameters(**self._amqp_params)


class ConnectionTimeoutError(Exception):
    """Timeout trying to connect"""


def _get_daemon_factory():
    """
    We need the factory to dynamically load daemon config, to support
    HA failovers
    """
    # Dealing with circular dependency
    try:
        from cloudify_agent.api.factory import DaemonFactory
    except ImportError:
        # Might not exist in e.g. the REST service
        DaemonFactory = None
    return DaemonFactory


class AMQPConnection(object):
    MAX_BACKOFF = 30

    def __init__(self, handlers, name=None, amqp_params=None,
                 connect_timeout=10):
        self._handlers = handlers
        self.name = name
        self._connection_params = self._get_connection_params()
        self._reconnect_backoff = 1
        self._closed = False
        self._amqp_params = amqp_params or AMQPParams()
        self._pika_connection = None
        self._consumer_thread = None
        self.connect_wait = threading.Event()
        self._connect_timeout = connect_timeout
        self._error = None
        self._daemon_factory = _get_daemon_factory()

        # use this queue to schedule methods to be called on the pika channel
        # from the connection thread - for sending data to rabbitmq, eg.
        # publishing messages or sending ACKs, which needs to be done from
        # the connection thread
        self._connection_tasks_queue = queue.Queue()

    def _get_connection_params(self):
        params = self._amqp_params.as_pika_params()
        hosts = copy.copy(self._amqp_params.raw_host)
        if not isinstance(hosts, list):
            hosts = [hosts]
        random.shuffle(hosts)
        while True:
            for host in hosts:
                params.host = host
                logger.debug('Current connection params: {0}'.format(params))
                yield params

    def _get_reconnect_backoff(self):
        backoff = self._reconnect_backoff
        self._reconnect_backoff = min(backoff * 2, self.MAX_BACKOFF)
        return backoff

    def _reset_reconnect_backoff(self):
        self._reconnect_backoff = 1

    def connect(self):
        self._error = None
        deadline = None
        self._pika_connection = None
        if self._connect_timeout is not None:
            deadline = time.time() + self._connect_timeout

        try:
            while self._pika_connection is None:
                params = next(self._connection_params)
                self._pika_connection = self._get_pika_connection(
                    params, deadline)
        # unfortunately DaemonNotFoundError is a BaseException subclass :(
        except BaseException as e:
            self._error = e
            self.connect_wait.set()
            raise e

        out_channel = self._pika_connection.channel()
        out_channel.confirm_delivery()
        for handler in self._handlers:
            handler.register(self, out_channel)
        self.connect_wait.set()
        return out_channel

    def _get_pika_connection(self, params, deadline=None):
        try:
            connection = pika.BlockingConnection(params)
        except pika.exceptions.AMQPConnectionError as e:
            time.sleep(self._get_reconnect_backoff())
            if deadline and time.time() > deadline:
                raise e
        else:
            self._reset_reconnect_backoff()
            self._closed = False
            return connection

    def consume(self):
        out_channel = self.connect()
        while not self._closed:
            try:
                self._pika_connection.process_data_events(0.05)
                self._process_publish(out_channel)
            except pika.exceptions.ChannelClosed as e:
                # happens when we attempt to use an exchange/queue that is not
                # declared - nothing we can do to help it, just exit
                logger.error('Channel closed: {0}'.format(e))
                break
            except pika.exceptions.ConnectionClosed:
                self.connect_wait.clear()
                out_channel = self.connect()
                continue
        self._process_publish(out_channel)
        self._pika_connection.close()

    def consume_in_thread(self):
        """Spawn a thread to run consume"""
        if self._consumer_thread:
            return
        self._consumer_thread = threading.Thread(target=self.consume)
        self._consumer_thread.daemon = True
        self._consumer_thread.start()
        wait_for_event(self.connect_wait)

        if self._error is not None:
            raise self._error
        return self._consumer_thread

    def __enter__(self):
        self.consume_in_thread()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _process_publish(self, channel):
        while True:
            try:
                envelope = self._connection_tasks_queue.get_nowait()
            except queue.Empty:
                return

            target_channel = envelope['channel'] or channel
            method = envelope['method']
            # we use a separate queue to send any possible exceptions back
            # to the calling thread - see the publish method
            message = envelope['message']
            err_queue = envelope.get('err_queue')

            try:
                if callable(method):
                    method(self, channel, **message)
                else:
                    getattr(target_channel, method)(**message)
            except pika.exceptions.ConnectionClosed:
                if self._closed:
                    return
                # if we couldn't send the message because the connection
                # was down, requeue it to be sent again later
                self._connection_tasks_queue.put(envelope)
                raise
            except Exception as e:
                if err_queue:
                    err_queue.put(e)
                raise
            else:
                if err_queue:
                    err_queue.put(None)

    def close(self, wait=True):
        self._closed = True
        if self._consumer_thread and wait:
            self._consumer_thread.join()
            self._consumer_thread = None

    def add_handler(self, handler):
        self._handlers.append(handler)
        if self._pika_connection:
            self.channel_method(handler.register)

    def channel(self):
        if self._closed or not self._pika_connection:
            raise RuntimeError(
                'Attempted to open a channel on a closed connection')
        return self._pika_connection.channel()

    def channel_method(self, method, channel=None, wait=True,
                       timeout=None, **kwargs):
        """Schedule a channel method to be called from the connection thread.

        Use this to schedule a channel method such as .publish or .basic_ack
        to be called from the connection thread.
        """
        if wait and self._consumer_thread \
                and self._consumer_thread is threading.current_thread():
            # when sending from the connection thread, we can't wait because
            # then we wouldn't allow the actual send loop (._process_publish)
            # to run, because we'd block on the err_queue here
            raise RuntimeError(
                'Cannot wait when sending from the connection thread')

        # the message is going to be sent from another thread (the .consume
        # thread). If an error happens there, we must have a way to get it
        # back out, so we pass a Queue together with the message, that will
        # contain either an exception instance, or None
        err_queue = queue.Queue() if wait else None
        envelope = {
            'method': method,
            'message': kwargs,
            'err_queue': err_queue,
            'channel': channel
        }
        self._connection_tasks_queue.put(envelope)
        if err_queue:
            err = err_queue.get(timeout=timeout)
            if isinstance(err, Exception):
                raise err

    def publish(self, message, wait=True, timeout=None):
        """Schedule a message to be sent.

        :param message: Kwargs for the pika basic_publish call. Should at
                        least contain the "body" and "exchange" keys, and
                        it might contain other keys such as "routing_key"
                        or "properties"
        :param wait: Whether to wait for the message to actually be sent.
                     If true, an exception will be raised if the message
                     cannot be sent.
        """
        properties = message.get('properties') or pika.BasicProperties()
        if properties.delivery_mode is None:
            # Unless the sender has decided that the message should have a
            # specific delivery mode, we'll make sure it's persistent so that
            # it isn't lost in the event of broker/cluster outage (as long as
            # it's on a durable queue).
            properties.delivery_mode = 2
        message['properties'] = properties
        if OLD_PIKA:
            self.channel_method(
                'publish', wait=wait, timeout=timeout, **message)
        else:
            self.channel_method(
                'basic_publish', wait=wait, timeout=timeout, **message)

    def ack(self, channel, delivery_tag, wait=True, timeout=None):
        self.channel_method('basic_ack', wait=wait, timeout=timeout,
                            channel=channel, delivery_tag=delivery_tag)


# return this result from .handle_task to not send a response.
# If a response is not sent, the reply queue will also be deleted
NO_RESPONSE = object()
STOP_AGENT = object()


class TaskConsumer(object):
    routing_key = ''
    late_ack = False

    def __init__(self, queue, threadpool_size=5, exchange_type='direct'):
        self.threadpool_size = threadpool_size
        self.exchange = queue
        self.queue = '{0}_{1}'.format(queue, self.routing_key)
        self._sem = threading.Semaphore(threadpool_size)
        self._connection = None
        self.exchange_type = exchange_type
        self._tasks_buffer = deque()

    def register(self, connection, channel):
        self._connection = connection
        channel.basic_qos(prefetch_count=self.threadpool_size)
        channel.confirm_delivery()
        channel.exchange_declare(exchange=self.exchange,
                                 auto_delete=False,
                                 durable=True,
                                 exchange_type=self.exchange_type)
        channel.queue_declare(queue=self.queue,
                              durable=True,
                              auto_delete=False)
        channel.queue_bind(queue=self.queue,
                           exchange=self.exchange,
                           routing_key=self.routing_key)
        if OLD_PIKA:
            channel.basic_consume(self.process, self.queue)
        else:
            channel.basic_consume(self.queue, self.process)

    def process(self, channel, method, properties, body):
        try:
            full_task = json.loads(body.decode('utf-8'))
        except ValueError:
            logger.error('Error parsing task: {0}'.format(body))
            return

        task_args = (channel, properties, full_task, method.delivery_tag)
        if self._sem.acquire(blocking=False):
            self._run_task(task_args)
        else:
            self._tasks_buffer.append(task_args)

    def _process_message(self, channel, properties, full_task, delivery_tag):
        if not self.late_ack:
            self._connection.ack(channel, delivery_tag)
        try:
            result = self.handle_task(full_task)
        except Exception as e:
            result = {'ok': False, 'error': repr(e)}
            logger.exception(
                'ERROR - failed message processing: '
                '{0!r}\nbody: {1}'.format(e, full_task)
            )
        if self.late_ack:
            self._connection.ack(channel, delivery_tag)
        if properties.reply_to:
            if result is NO_RESPONSE:
                self.delete_queue(properties.reply_to)
            else:
                if result is STOP_AGENT:
                    body = json.dumps({'ok': True})
                else:
                    body = json.dumps(result)
                self._connection.publish({
                    'exchange': self.exchange,
                    'routing_key': properties.reply_to,
                    'properties': pika.BasicProperties(
                        correlation_id=properties.correlation_id),
                    'body': body
                })
        if result is STOP_AGENT:
            # the operation asked us to exit, so drop everything and exit
            os._exit(0)
        if not self._maybe_run_next_task():
            self._sem.release()

    def _run_task(self, task_args):
        new_thread = threading.Thread(
            target=self._process_message,
            args=task_args
        )
        new_thread.daemon = True
        new_thread.start()

    def _maybe_run_next_task(self):
        try:
            task_args = self._tasks_buffer.popleft()
        except IndexError:
            return False
        else:
            self._run_task(task_args)
            return True

    def handle_task(self, full_task):
        raise NotImplementedError()

    def delete_queue(self, queue, if_empty=True, wait=True):
        self._connection.channel_method(
            'queue_delete', queue=queue, if_empty=if_empty, wait=wait)

    def delete_exchange(self, exchange):
        self._connection.channel_method('exchange_delete', exchange=exchange)


class SendHandler(object):
    exchange_settings = {
        'auto_delete': False,
        'durable': True,
    }
    wait_for_publish = True

    def __init__(self, exchange, exchange_type='direct', routing_key=''):
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key
        self.logger = logging.getLogger('dispatch.{0}'.format(self.exchange))
        self._connection = None

    def register(self, connection, channel):
        self._connection = connection
        channel.exchange_declare(exchange=self.exchange,
                                 exchange_type=self.exchange_type,
                                 **self.exchange_settings)

    def publish(self, message, **kwargs):
        self._connection.publish({
            'exchange': self.exchange,
            'body': json.dumps(message),
            'routing_key': self.routing_key
        }, wait=self.wait_for_publish)


class BlockingRequestResponseHandler(TaskConsumer):
    def __init__(self, *args, **kwargs):
        super(BlockingRequestResponseHandler, self).__init__(*args, **kwargs)
        self._response = queue.Queue()

    def register(self, connection, channel):
        self._connection = connection
        channel.exchange_declare(exchange=self.exchange,
                                 auto_delete=False,
                                 durable=True,
                                 exchange_type=self.exchange_type)

    def _declare_queue(self, queue_name):
        self._connection.channel_method(
            'queue_declare', queue=queue_name, durable=True,
            exclusive=True)
        self._connection.channel_method(
            'queue_bind', queue=queue_name, exchange=self.exchange)
        if OLD_PIKA:
            self._connection.channel_method(
                'basic_consume', queue=queue_name,
                consumer_callback=self.process)
        else:
            self._connection.channel_method(
                'basic_consume', queue=queue_name,
                on_message_callback=self.process)

    def _queue_name(self, correlation_id):
        """Make the queue name for this handler based on the correlation id"""
        return '{0}_response_{1}'.format(self.exchange, correlation_id)

    def make_response_queue(self, correlation_id):
        self._declare_queue(self._queue_name(correlation_id))

    def publish(self, message, correlation_id=None, routing_key='',
                expiration=None, timeout=None):
        if correlation_id is None:
            correlation_id = utils.uuid4()
        self.make_response_queue(correlation_id)

        if expiration is not None:
            # rabbitmq wants it to be a string
            expiration = '{0}'.format(expiration)
        self._connection.publish({
            'exchange': self.exchange,
            'body': json.dumps(message),
            'properties': pika.BasicProperties(
                reply_to=self._queue_name(correlation_id),
                correlation_id=correlation_id,
                expiration=expiration),
            'routing_key': routing_key
        })

        try:
            return json.loads(
                self._response.get(timeout=timeout).decode('utf-8')
            )
        except queue.Empty:
            raise RuntimeError('No response received for task {0}'
                               .format(correlation_id))
        except ValueError:
            logger.error('Error parsing response for task {0}'
                         .format(correlation_id))

    def process(self, channel, method, properties, body):
        channel.basic_ack(method.delivery_tag)
        self.delete_queue(
            self._queue_name(properties.correlation_id),
            wait=False, if_empty=False)
        self._response.put(body)


def get_client(amqp_host=None,
               amqp_user=None,
               amqp_pass=None,
               amqp_port=None,
               amqp_vhost=None,
               ssl_enabled=None,
               ssl_cert_path=None,
               ssl_cert_data=None,
               name=None,
               connect_timeout=10,
               cls=AMQPConnection):
    """
    Create a client without any handlers in it. Use the `add_handler` method
    to add handlers to this client
    :return: CloudifyConnectionAMQPConnection
    """

    amqp_params = AMQPParams(
        amqp_host,
        amqp_user,
        amqp_pass,
        amqp_port,
        amqp_vhost,
        ssl_enabled,
        ssl_cert_path,
        ssl_cert_data,
    )

    return cls(handlers=[], amqp_params=amqp_params, name=name,
               connect_timeout=connect_timeout)


def is_agent_alive(name,
                   client,
                   timeout=INSPECT_TIMEOUT,
                   connect=True):
    """
    Send a `ping` service task to an agent, and validate that a correct
    response is received

    :param name: the agent's amqp exchange name
    :param client: an AMQPClient for the agent's vhost
    :param timeout: how long to wait for the response
    :param connect: whether to connect the client (should be False if it is
                    already connected)
    """
    handler = BlockingRequestResponseHandler(name)
    client.add_handler(handler)
    if connect:
        with client:
            response = _send_ping_task(name, handler, timeout)
    else:
        response = _send_ping_task(name, handler, timeout)
    return 'time' in response


def _send_ping_task(name, handler, timeout=INSPECT_TIMEOUT):
    task = {
        'service_task': {
            'task_name': 'ping',
            'kwargs': {}
        }
    }
    # messages expire shortly before we hit the timeout - if they haven't
    # been handled by then, they won't make the timeout
    expiration = (timeout * 1000) - 200  # milliseconds
    try:
        return handler.publish(task, routing_key='service',
                               timeout=timeout, expiration=expiration)
    except pika.exceptions.AMQPError as e:
        logger.warning('Could not send a ping task to {0}: {1}'
                       .format(name, e))
        return {}
    except RuntimeError as e:
        logger.info('No ping response from {0}: {1}'.format(name, e))
        return {}
