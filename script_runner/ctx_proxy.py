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


import argparse
import sys
import os
import tempfile
import re
import collections

import zmq

# Environment variable for the socket url
# (used by zmq clients to create the socket)
CTX_SOCKET_URL = 'CTX_SOCKET_URL'


class CtxProxyServer(object):

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
        args = self.sock.recv_json()
        result = self._process(args)
        self.sock.send_json(result)
        return True

    def _process(self, args):
        return process_ctx_request(self.ctx, args)

    def close(self):
        self.sock.close()
        self.z_context.term()


class UnixCtxProxyServer(CtxProxyServer):

    def __init__(self, ctx):
        socket_path = tempfile.mktemp(prefix='ctx-', suffix='.socket')
        socket_url = 'ipc://{}'.format(socket_path)
        super(UnixCtxProxyServer, self).__init__(ctx, socket_url)


class TCPCtxProxyServer(CtxProxyServer):

    def __init__(self, ctx, ip='127.0.0.1', port=29635):
        socket_url = 'tcp://{}:{}'.format(ip, port)
        super(TCPCtxProxyServer, self).__init__(ctx, socket_url)


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
            remaining_args = args[index:]
            current = current(*remaining_args)
            break
        else:
            raise RuntimeError('{} cannot be process in {}'
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


# TODO make request timeout configurable in command line
def client_req(socket_url, args, timeout=5):
    context = zmq.Context()
    sock = context.socket(zmq.REQ)
    try:
        sock.connect(socket_url)
        sock.send_json(args)
        if sock.poll(1000*timeout):
            return sock.recv_json()
        else:
            raise RuntimeError('Timed out while waiting for response')
    finally:
        sock.close()
        context.term()

def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', '--timeout', type=int, default=5)
    parser.add_argument('--socket_url', default=os.environ.get(CTX_SOCKET_URL))
    parser.add_argument('args', nargs='*')
    args = parser.parse_args(args)
    if not args.socket_url:
        raise RuntimeError('Missing CTX_SOCKET_URL environment variable'
                           ' or socket_url command line argument')
    return args


def main(args=None):
    args = parse_args(args)
    response = client_req(args.socket_url,
                          args.args,
                          args.timeout)
    if not response:
        response = ''
    sys.stdout.write(response)


if __name__ == '__main__':
    main()
