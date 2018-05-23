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

import ssl
import json
import time
import uuid
import Queue
import logging
import threading

import pika
import pika.exceptions

from cloudify import broker_config
from cloudify import exceptions

try:
    from cloudify_agent.api.factory import DaemonFactory
except ImportError:
    DaemonFactory = None


logger = logging.getLogger(__name__)

HEARTBEAT_INTERVAL = 30


class AMQPParams(object):
    def __init__(self,
                 amqp_host=None,
                 amqp_user=None,
                 amqp_pass=None,
                 amqp_port=None,
                 amqp_vhost=None,
                 ssl_enabled=None,
                 ssl_cert_path=None,
                 socket_timeout=3):
        super(AMQPParams, self).__init__()
        username = amqp_user or broker_config.broker_username
        password = amqp_pass or broker_config.broker_password
        credentials = pika.credentials.PlainCredentials(
            username=username,
            password=password,
        )

        broker_ssl_options = {}
        if ssl_enabled:
            broker_ssl_options = {
                'ca_certs': ssl_cert_path,
                'cert_reqs': ssl.CERT_REQUIRED,
            }
        if not broker_ssl_options:
            broker_ssl_options = broker_config.broker_ssl_options

        self._amqp_params = {
            'host': amqp_host or broker_config.broker_hostname,
            'port': amqp_port or broker_config.broker_port,
            'virtual_host': amqp_vhost or broker_config.broker_vhost,
            'credentials': credentials,
            'ssl': ssl_enabled or broker_config.broker_ssl_enabled,
            'ssl_options': broker_ssl_options,
            'heartbeat': HEARTBEAT_INTERVAL,
            'socket_timeout': socket_timeout
        }

    def as_pika_params(self):
        return pika.ConnectionParameters(**self._amqp_params)


class ConnectionTimeoutError(Exception):
    """Timeout trying to connect"""


class AMQPConnection(object):
    MAX_BACKOFF = 30

    def __init__(self, handlers, name=None, amqp_params=None,
                 connect_timeout=10):
        self._handlers = handlers
        self._publish_queue = Queue.Queue()
        self.name = name
        self._connection_params = self._get_connection_params()
        self._reconnect_backoff = 1
        self._closed = False
        self._amqp_params = amqp_params or AMQPParams()
        self.connection = None
        self._consumer_thread = None
        self.connect_wait = threading.Event()
        self._connect_timeout = connect_timeout
        self._error = None

    def _get_connection_params(self):
        while True:
            params = self._amqp_params.as_pika_params()
            if self.name and DaemonFactory is not None:
                daemon = DaemonFactory().load(self.name)
                if daemon.cluster:
                    for node_ip in daemon.cluster:
                        params.host = node_ip
                        yield params
                    continue
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
        if self._connect_timeout is not None:
            deadline = time.time() + self._connect_timeout

        for params in self._connection_params:
            try:
                self.connection = pika.BlockingConnection(params)
            except pika.exceptions.AMQPConnectionError:
                time.sleep(self._get_reconnect_backoff())
                if deadline and time.time() > deadline:
                    self._error = ConnectionTimeoutError()
                    self.connect_wait.set()
                    raise self._error
            else:
                self._reset_reconnect_backoff()
                self._closed = False
                break

        out_channel = self.connection.channel()
        for handler in self._handlers:
            handler.register(self.connection, self._publish_queue)
            logger.info('Registered handler for {0} [{1}]'
                        .format(handler.__class__.__name__,
                                handler.routing_key))
        self.connect_wait.set()
        return out_channel

    def consume(self):
        out_channel = self.connect()
        while not self._closed:
            try:
                self.connection.process_data_events(0.2)
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
        self.connection.close()

    def consume_in_thread(self):
        """Spawn a thread to run consume"""
        if self._consumer_thread:
            return
        self._consumer_thread = threading.Thread(target=self.consume)
        self._consumer_thread.daemon = True
        self._consumer_thread.start()
        self.connect_wait.wait()
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
                msg = self._publish_queue.get_nowait()
            except Queue.Empty:
                return
            try:
                channel.basic_publish(**msg)
            except pika.exceptions.ConnectionClosed:
                if self._closed:
                    return
                # if we couldn't send the message because the connection
                # was down, requeue it to be sent again later
                self._publish_queue.put(msg)
                raise

    def close(self):
        self._closed = True
        if self._consumer_thread:
            self._consumer_thread.join()
            self._consumer_thread = None

    def add_handler(self, handler):
        self._handlers.append(handler)
        if self.connection:
            handler.register(self.connection, self._publish_queue)


class TaskConsumer(object):
    routing_key = ''

    def __init__(self, queue, threadpool_size=5):
        self.threadpool_size = threadpool_size
        self.exchange = queue
        self.queue = '{0}_{1}'.format(queue, self.routing_key)
        self._sem = threading.Semaphore(threadpool_size)
        self._output_queue = None
        self.in_channel = None

    def register(self, connection, output_queue):
        self._output_queue = output_queue
        self.in_channel = connection.channel()
        self._register_queue(self.in_channel)

    def _register_queue(self, channel):
        channel.basic_qos(prefetch_count=self.threadpool_size)
        channel.confirm_delivery()
        channel.exchange_declare(
            exchange=self.exchange, auto_delete=False, durable=True)
        channel.queue_declare(queue=self.queue,
                              durable=True,
                              auto_delete=False)
        channel.queue_bind(queue=self.queue,
                           exchange=self.exchange,
                           routing_key=self.routing_key)
        channel.basic_consume(self.process, self.queue)

    def process(self, channel, method, properties, body):
        try:
            full_task = json.loads(body)
        except ValueError:
            logger.error('Error parsing task: {0}'.format(body))
            return

        self._sem.acquire()
        new_thread = threading.Thread(
            target=self._process_message,
            args=(properties, full_task)
        )
        new_thread.daemon = True
        new_thread.start()
        channel.basic_ack(method.delivery_tag)

    def _process_message(self, properties, full_task):
        try:
            result = self.handle_task(full_task)
        except Exception as e:
            result = {'ok': False, 'error': repr(e)}
            logger.error(
                'ERROR - failed message processing: '
                '{0!r}\nbody: {1}'.format(e, full_task)
            )

        if properties.reply_to:
            self._output_queue.put({
                'exchange': '',
                'routing_key': properties.reply_to,
                'properties': pika.BasicProperties(
                    correlation_id=properties.correlation_id),
                'body': json.dumps(result)
            })
        self._sem.release()

    def handle_task(self, full_task):
        raise NotImplementedError()


class SendHandler(object):
    exchange_settings = {
        'auto_delete': False,
        'durable': True,
    }

    def __init__(self, exchange, exchange_type='direct', routing_key=''):
        self.exchange = exchange
        self.exchange_type = exchange_type
        self.routing_key = routing_key
        self.logger = logging.getLogger('dispatch.{0}'.format(self.exchange))

    def register(self, connection, output_queue):
        self._output_queue = output_queue

        out_channel = connection.channel()
        out_channel.exchange_declare(exchange=self.exchange,
                                     exchange_type=self.exchange_type,
                                     **self.exchange_settings)

    def _log_message(self, message):
        level = message.get('level', 'info')
        log_func = getattr(self.logger, level, self.logger.info)
        exec_id = message.get('context', {}).get('execution_id')
        text = message['message']['text']
        msg = '[{0}] {1}'.format(exec_id, text) if exec_id else text
        log_func(msg)

    def publish(self, message, **kwargs):
        if 'message' in message:
            # message is textual, let's log it
            self._log_message(message)
        self._output_queue.put({
            'exchange': self.exchange,
            'body': json.dumps(message),
            'routing_key': self.routing_key
        })


class _RequestResponseHandlerBase(TaskConsumer):
    def __init__(self, exchange):
        super(_RequestResponseHandlerBase, self).__init__(exchange)
        self.queue = None

    def _register_queue(self, channel):
        self.queue = uuid.uuid4().hex
        self.in_channel.queue_declare(queue=self.queue, exclusive=True,
                                      durable=True)
        channel.basic_consume(self.process, self.queue)

    def publish(self, message, correlation_id=None, routing_key=''):
        if correlation_id is None:
            correlation_id = uuid.uuid4().hex

        self._output_queue.put({
            'exchange': self.exchange,
            'body': json.dumps(message),
            'properties': pika.BasicProperties(
                reply_to=self.queue,
                correlation_id=correlation_id),
            'routing_key': routing_key
        })
        return correlation_id

    def process(self, channel, method, properties, body):
        raise NotImplementedError()


class BlockingRequestResponseHandler(_RequestResponseHandlerBase):
    def __init__(self, *args, **kwargs):
        super(BlockingRequestResponseHandler, self).__init__(*args, **kwargs)
        self._response_queues = {}

    def publish(self, *args, **kwargs):
        timeout = kwargs.pop('timeout', None)
        correlation_id = super(BlockingRequestResponseHandler, self)\
            .publish(*args, **kwargs)
        self._response_queues[correlation_id] = Queue.Queue()
        try:
            resp = self._response_queues[correlation_id].get(timeout=timeout)
            return resp
        except Queue.Empty:
            raise RuntimeError('No response received for task {0}'
                               .format(correlation_id))
        finally:
            del self._response_queues[correlation_id]

    def process(self, channel, method, properties, body):
        channel.basic_ack(method.delivery_tag)
        try:
            response = json.loads(body)
        except ValueError:
            logger.error('Error parsing response: {0}'.format(body))
            return
        if properties.correlation_id in self._response_queues:
            self._response_queues[properties.correlation_id].put(response)


class CallbackRequestResponseHandler(_RequestResponseHandlerBase):
    def __init__(self, *args, **kwargs):
        super(CallbackRequestResponseHandler, self).__init__(*args, **kwargs)
        self._callbacks = {}

    def publish(self, *args, **kwargs):
        callback = kwargs.pop('callback')
        correlation_id = super(CallbackRequestResponseHandler, self)\
            .publish(*args, **kwargs)
        self._callbacks[correlation_id] = callback

    def process(self, channel, method, properties, body):
        channel.basic_ack(method.delivery_tag)
        try:
            response = json.loads(body)
        except ValueError:
            logger.error('Error parsing response: {0}'.format(body))
            return
        if properties.correlation_id in self._callbacks:
            self._callbacks[properties.correlation_id](response)


def get_client(amqp_host=None,
               amqp_user=None,
               amqp_pass=None,
               amqp_port=None,
               amqp_vhost=None,
               ssl_enabled=None,
               ssl_cert_path=None,
               connect_timeout=10):
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
        ssl_cert_path
    )

    return AMQPConnection(handlers=[], amqp_params=amqp_params,
                          connect_timeout=connect_timeout)


class CloudifyEventsPublisher(object):
    EVENTS_EXCHANGE_NAME = 'cloudify-events'
    SOCKET_TIMEOUT = 5
    CONNECTION_ATTEMPTS = 3
    LOGS_EXCHANGE_NAME = 'cloudify-logs'
    channel_settings = {
        'auto_delete': False,
        'durable': True,
    }

    def __init__(self, amqp_params):
        self.events_handler = SendHandler(self.EVENTS_EXCHANGE_NAME,
                                          exchange_type='fanout')
        self.logs_handler = SendHandler(self.LOGS_EXCHANGE_NAME,
                                        exchange_type='fanout')
        self._connection = AMQPConnection(
            handlers=[self.events_handler, self.logs_handler],
            amqp_params=amqp_params
        )
        self._is_closed = False

    def connect(self):
        self._connection.consume_in_thread()

    def publish_message(self, message, message_type):
        if self._is_closed:
            raise exceptions.ClosedAMQPClientException(
                'Publish failed, AMQP client already closed')
        if message_type == 'event':
            handler = self.events_handler
        else:
            handler = self.logs_handler
        handler.publish(message)

    def close(self):
        if self._is_closed:
            return
        self._is_closed = True
        thread = threading.current_thread()
        if self._connection:
            logger.debug('Closing amqp client of thread {0}'.format(thread))
            try:
                self._connection.close()
            except Exception as e:
                logger.debug('Failed to close amqp client of thread {0}, '
                             'reported error: {1}'.format(thread, repr(e)))


def create_events_publisher(amqp_host=None,
                            amqp_user=None,
                            amqp_pass=None,
                            amqp_port=None,
                            amqp_vhost=None,
                            ssl_enabled=None,
                            ssl_cert_path=None):
    thread = threading.current_thread()

    amqp_params = AMQPParams(
        amqp_host,
        amqp_user,
        amqp_pass,
        amqp_port,
        amqp_vhost,
        ssl_enabled,
        ssl_cert_path
    )

    try:
        client = CloudifyEventsPublisher(amqp_params)
        client.connect()
        logger.debug('AMQP client created for thread {0}'.format(thread))
    except Exception as e:
        logger.warning(
            'Failed to create AMQP client for thread: {0} ({1}: {2})'
            .format(thread, type(e).__name__, e))
        raise
    return client
