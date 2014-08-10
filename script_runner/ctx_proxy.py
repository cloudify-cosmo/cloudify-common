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
from StringIO import StringIO

import zmq

# Environment variable for the socket url
# (used by zmq clients to create the socket)
CTX_SOCKET_URL = 'CTX_SOCKET_URL'


class CtxProxy(object):

    def __init__(self, ctx, socket_url):
        self.ctx = ctx
        self.socket_url = socket_url
        self.z_context = zmq.Context(io_threads=1)
        self.sock = self.z_context.socket(zmq.REP)
        self.sock.bind(self.socket_url)
        self.poller = zmq.Poller()
        self.poller.register(self.sock, zmq.POLLIN)

    def poll_and_process(self, timeout=1):
        state = dict(self.poller.poll(1000*timeout)).get(self.sock)
        if state != zmq.POLLIN:
            return False
        request = self.sock.recv_json()
        try:
            payload = self._process(request['args'])
            result = json.dumps({
                'type': 'result',
                'payload': payload
            })
        except Exception, e:
            tb = StringIO()
            traceback.print_exc(file=tb)
            response_type = 'error'
            payload = {
                'type': type(e).__name__,
                'message': str(e),
                'traceback': tb.getvalue()
            }
            result = json.dumps({
                'type': 'error',
                'payload': payload
            })
        self.sock.send(result)
        return True

    def _process(self, args):
        return process_ctx_request(self.ctx, args)

    def close(self):
        self.sock.close()
        self.z_context.term()


class UnixCtxProxy(CtxProxy):

    def __init__(self, ctx, socket_path=None):
        if not socket_path:
            socket_path = tempfile.mktemp(prefix='ctx-', suffix='.socket')
        socket_url = 'ipc://{}'.format(socket_path)
        super(UnixCtxProxy, self).__init__(ctx, socket_url)


class TCPCtxProxy(CtxProxy):

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


def client_req(socket_url, args, timeout=5):
    context = zmq.Context()
    sock = context.socket(zmq.REQ)
    try:
        sock.connect(socket_url)
        sock.send_json({
                'args': args
            })
        if sock.poll(1000*timeout):
            response = sock.recv_json()
            payload = response['payload']
            if response.get('type') == 'error':
                ex_type = payload['type']
                ex_message = payload['message']
                ex_traceback = payload['traceback']
                raise RequestError('{}: {}'.format(ex_type, ex_message))
            else:
                return payload
        else:
            raise RuntimeError('Timed out while waiting for response')
    finally:
        sock.close()
        context.term()


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


class CtxProxyServer(object):

    def __init__(self, ctx, socket_path=None):
        self.ctx = ctx
        self.proxy = UnixCtxProxy(ctx, socket_path)

    def close(self):
        self.proxy.close()

    def serve(self):
        while True:
            try:
                self.proxy.poll_and_process(timeout=1)
            except RuntimeError, e:
                print 'ignoring: {}'.format(e)


def server():
    import importlib
    socket_path = sys.argv[1]
    ctx_module_path = sys.argv[2]
    sys.path.append(os.path.dirname(ctx_module_path))
    ctx_module = __import__(os.path.basename(os.path.splitext(ctx_module_path)[0]))
    ctx = getattr(ctx_module, 'ctx')
    server = CtxProxyServer(ctx, socket_path)
    print server.proxy.socket_url
    server.serve()


class RequestError(RuntimeError):
    pass
