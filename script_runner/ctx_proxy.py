#########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.


import traceback
import argparse
import sys
import os
import tempfile
import re
import collections
import json
import threading
from StringIO import StringIO
from wsgiref.simple_server import WSGIServer, WSGIRequestHandler
from wsgiref.simple_server import make_server as make_wsgi_server


import bottle
import requests

# Environment variable for the socket url
# (used by clients to locate the socket [http, zmq(unix, tcp)])
CTX_SOCKET_URL = 'CTX_SOCKET_URL'


class RequestError(RuntimeError):

    def __init__(self, ex_message, ex_type, ex_traceback):
        super(RequestError, self).__init__(
            self,
            '{}: {}'.format(ex_type, ex_message))
        self.ex_type = ex_type
        self.ex_message = ex_message
        self.ex_traceback = ex_traceback


class CtxProxy(object):

    def __init__(self, ctx, socket_url):
        self.ctx = ctx
        self.socket_url = socket_url

    def process(self, request):
        try:
            typed_request = json.loads(request)
            args = typed_request['args']
            payload = process_ctx_request(self.ctx, args)
            result = json.dumps({
                'type': 'result',
                'payload': payload
            })
        except Exception, e:
            tb = StringIO()
            traceback.print_exc(file=tb)
            payload = {
                'type': type(e).__name__,
                'message': str(e),
                'traceback': tb.getvalue()
            }
            result = json.dumps({
                'type': 'error',
                'payload': payload
            })
        return result

    def close(self):
        pass


class HTTPCtxProxy(CtxProxy):

    def __init__(self, ctx, port=29635):
        socket_url = 'http://localhost:{}'.format(port)
        super(HTTPCtxProxy, self).__init__(ctx, socket_url)
        self.port = port
        self.thread = self._start_server()

    def _start_server(self):

        proxy = self

        class BottleServerAdapter(bottle.ServerAdapter):

            def run(self, app):

                class Server(WSGIServer):
                    allow_reuse_address = True

                    def handle_error(self, request, client_address):
                        pass

                class Handler(WSGIRequestHandler):
                    def address_string(self):
                        return self.client_address[0]

                    def log_request(*args, **kwargs):
                        if not self.quiet:
                            return WSGIRequestHandler.log_request(
                                *args, **kwargs)

                self.srv = make_wsgi_server(
                    self.host,
                    self.port,
                    app,
                    Server,
                    Handler)
                self.port = self.srv.server_port
                proxy.server = self.srv
                self.srv.serve_forever(poll_interval=0.1)

        bottle.post('/', callback=self._request_handler)

        def serve():
            bottle.run(
                host='localhost',
                port=self.port,
                quiet=True,
                server=BottleServerAdapter)
        thread = threading.Thread(target=serve)
        thread.daemon = True
        thread.start()
        return thread

    def close(self):
        self.server.shutdown()
        self.server.server_close()

    def _request_handler(self):
        request = bottle.request.body.read()
        response = self.process(request)
        return bottle.LocalResponse(
            body=response,
            status=200,
            headers={'content-type': 'application/json'})


class ZMQCtxProxy(CtxProxy):

    def __init__(self, ctx, socket_url):
        super(ZMQCtxProxy, self).__init__(ctx, socket_url)
        import zmq
        self.z_context = zmq.Context(io_threads=1)
        self.sock = self.z_context.socket(zmq.REP)
        self.sock.bind(self.socket_url)
        self.poller = zmq.Poller()
        self.poller.register(self.sock, zmq.POLLIN)

    def poll_and_process(self, timeout=1):
        import zmq
        state = dict(self.poller.poll(1000*timeout)).get(self.sock)
        if not state == zmq.POLLIN:
            return False
        request = self.sock.recv()
        response = self.process(request)
        self.sock.send(response)
        return True

    def close(self):
        self.sock.close()
        self.z_context.term()


class UnixCtxProxy(ZMQCtxProxy):

    def __init__(self, ctx, socket_path=None):
        if not socket_path:
            socket_path = tempfile.mktemp(prefix='ctx-', suffix='.socket')
        socket_url = 'ipc://{}'.format(socket_path)
        super(UnixCtxProxy, self).__init__(ctx, socket_url)


class TCPCtxProxy(ZMQCtxProxy):

    def __init__(self, ctx, ip='127.0.0.1', port=29635):
        socket_url = 'tcp://{}:{}'.format(ip, port)
        super(TCPCtxProxy, self).__init__(ctx, socket_url)


def process_ctx_request(ctx, args):
    current = ctx
    num_args = len(args)
    index = 0
    while index < num_args:
        arg = args[index]
        desugared_attr = _desugar_attr(current, arg)
        if desugared_attr:
            current = getattr(current, desugared_attr)
        elif isinstance(current, collections.MutableMapping):
            key = arg
            path_dict = PathDictAccess(current)
            if index + 1 == num_args:
                # read dict prop by path
                value = path_dict.get(key)
                current = value
            elif index + 2 == num_args:
                # set dict prop by path
                value = args[index+1]
                current = path_dict.set(key, value)
            else:
                raise RuntimeError('Illegal argument while accessing dict')
            break
        elif callable(current):
            kwargs = {}
            remaining_args = args[index:]
            if isinstance(remaining_args[-1], collections.MutableMapping):
                kwargs = remaining_args[-1]
                remaining_args = remaining_args[:-1]
            current = current(*remaining_args, **kwargs)
            break
        else:
            raise RuntimeError('{} cannot be processed in {}'
                               .format(arg, args))
        index += 1

    if callable(current):
        current = current()

    return current


def _desugar_attr(obj, attr):
    if not isinstance(attr, basestring):
        return None
    if hasattr(obj, attr):
        return attr
    attr = attr.replace('-', '_')
    if hasattr(obj, attr):
        return attr
    return None


class PathDictAccess(object):

    pattern = re.compile("(.+)\[(\d+)\]")

    def __init__(self, obj):
        self.obj = obj

    def set(self, prop_path, value):
        obj, prop_name = self._get_parent_obj_prop_name_by_path(prop_path)
        obj[prop_name] = value

    def get(self, prop_path):
        value = self._get_object_by_path(prop_path)
        return value

    def _get_object_by_path(self, prop_path):
        current = self.obj
        for prop_segment in prop_path.split('.'):
            match = self.pattern.match(prop_segment)
            if match:
                index = int(match.group(2))
                property_name = match.group(1)
                if property_name not in current:
                    self._raise_illegal(prop_path)
                if type(current[property_name]) != list:
                    self._raise_illegal(prop_path)
                current = current[property_name][index]
            else:
                if prop_segment not in current:
                    current[prop_segment] = {}
                current = current[prop_segment]
        return current

    def _get_parent_obj_prop_name_by_path(self, prop_path):
        split = prop_path.split('.')
        if len(split) == 1:
            return self.obj, prop_path
        parent_path = '.'.join(split[:-1])
        parent_obj = self._get_object_by_path(parent_path)
        prop_name = split[-1]
        return parent_obj, prop_name

    @staticmethod
    def _raise_illegal(prop_path):
        raise RuntimeError('illegal path: {0}'.format(prop_path))


def zmq_client_req(socket_url, request, timeout):
    import zmq
    context = zmq.Context()
    sock = context.socket(zmq.REQ)
    try:
        sock.connect(socket_url)
        sock.send_json(request)
        if sock.poll(1000*timeout):
            return sock.recv_json()
        else:
            raise RuntimeError('Timed out while waiting for response')
    finally:
        sock.close()
        context.term()


def http_client_req(socket_url, request, timeout):
    response = requests.post(
        socket_url,
        data=json.dumps(request),
        headers={'content-type': 'application/json'},
        timeout=timeout)
    if response.status_code != 200:
        raise RuntimeError('Requeste failed: {}'.format(response))
    return response.json()


def client_req(socket_url, args, timeout=5):
    request = {
        'args': args
    }

    schema, _ = socket_url.split('://')
    if schema in ['ipc', 'tcp']:
        request_method = zmq_client_req
    elif schema in ['http']:
        request_method = http_client_req
    else:
        raise RuntimeError('Unsupported protocol: {}'.format(schema))

    response = request_method(socket_url, request, timeout)
    payload = response['payload']
    if response.get('type') == 'error':
        ex_type = payload['type']
        ex_message = payload['message']
        ex_traceback = payload['traceback']
        raise RequestError(ex_message,
                           ex_type,
                           ex_traceback)
    else:
        return payload


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--timeout', type=int, default=5)
    parser.add_argument('--socket-url', default=os.environ.get(CTX_SOCKET_URL))
    parser.add_argument('--json-arg-prefix', default='@')
    parser.add_argument('-j', '--json-output', action='store_true')
    parser.add_argument('args', nargs='*')
    args = parser.parse_args(args)
    if not args.socket_url:
        raise RuntimeError('Missing CTX_SOCKET_URL environment variable'
                           ' or socket_url command line argument')
    return args


def process_args(json_prefix, args):
    processed_args = []
    for arg in args:
        if arg.startswith(json_prefix):
            arg = json.loads(arg[1:])
        processed_args.append(arg)
    return processed_args


def main(args=None):
    args = parse_args(args)
    response = client_req(args.socket_url,
                          process_args(args.json_arg_prefix,
                                       args.args),
                          args.timeout)
    if args.json_output:
        response = json.dumps(response)
    else:
        if not response:
            response = ''
        response = str(response)
    sys.stdout.write(response)


if __name__ == '__main__':
    main()
