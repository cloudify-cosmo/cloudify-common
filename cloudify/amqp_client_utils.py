########
# Copyright (c) 2016 GigaSpaces Technologies Ltd. All rights reserved
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

from threading import Thread, RLock, Event

from cloudify import amqp_client
from cloudify._compat import queue
from cloudify.exceptions import ClosedAMQPClientException


class AMQPWrappedThread(Thread):
    """
    creates an amqp client before calling the target method.
    This thread is always set as a daemon.
    """

    def __init__(self, target, *args, **kwargs):

        def wrapped_target(*inner_args, **inner_kwargs):
            with global_events_publisher:
                self.target_method(*inner_args, **inner_kwargs)

        self.target_method = target
        super(AMQPWrappedThread, self).__init__(target=wrapped_target, *args,
                                                **kwargs)
        self.started_amqp_client = global_events_publisher.client_started
        self.daemon = True


_STOP = object()


class _GlobalEventsPublisher(object):
    def __init__(self, *client_args, **client_kwargs):
        self.client_started = Event()
        self._connect_lock = RLock()
        self._callers = 0
        self._thread = None
        self._queue = queue.Queue()
        self._client_args = client_args
        self._client_kwargs = client_kwargs

    def register_caller(self):
        with self._connect_lock:
            if not self.client_started.is_set():
                self._connect()
            self._callers += 1

    def unregister_caller(self):
        with self._connect_lock:
            self._callers -= 1
            # Can theoretically be less than zero, if this function is called
            # by dispatch.py as part of cleanup after the init_events_publisher
            # call failed.
            if self._callers <= 0:
                self._disconnect()
                self._thread.join()

    def __enter__(self):
        self.register_caller()
        return self

    def __exit__(self, exc, val, tb):
        self.unregister_caller()

    def publish_message(self, message, message_type):
        self._queue.put((message, message_type))

    def close(self):
        self.unregister_caller()
        self._client.close()

    def _make_client(self):
        return amqp_client.create_events_publisher(
            *self._client_args, **self._client_kwargs)

    def _connect(self):
        self._client = self._make_client()
        self.client_started.set()
        self._thread = Thread(target=self._handle_publish_message)
        self._thread.start()

    def _disconnect(self):
        self._queue.put(_STOP)

    def _handle_publish_message(self):
        while True:
            request = self._queue.get()
            if request is _STOP:
                break
            try:
                self._client.publish_message(*request)
            except ClosedAMQPClientException:
                with self._connect_lock:
                    self._client = self._make_client()
                self._client.publish_message(*request)
        self._client.close()
        self.client_started.clear()


def init_events_publisher():
    global_events_publisher.register_caller()
    global_management_events_publisher.register_caller()


def close_amqp_client():
    # This function MUST:
    # 1. Be idempotent
    # 2. Never raise any exception
    global_events_publisher.unregister_caller()
    global_management_events_publisher.unregister_caller()


global_events_publisher = _GlobalEventsPublisher()
global_management_events_publisher = _GlobalEventsPublisher(amqp_vhost='/')
