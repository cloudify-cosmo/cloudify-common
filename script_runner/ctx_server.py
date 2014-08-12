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
import os
import sys
import importlib
import re

from ctx_proxy import UnixCtxProxy


class CtxProxyServer(object):

    def __init__(self, ctx, socket_path=None):
        self.ctx = ctx
        self.proxy = UnixCtxProxy(ctx, socket_path)
        self.stopped = False

    def close(self):
        self.proxy.close()

    def stop(self):
        self.stopped = True

    def serve(self):
        while not self.stopped:
            try:
                self.proxy.poll_and_process(timeout=0.1)
            except RuntimeError, e:
                print 'ignoring: {}'.format(e)


def parse_args(args=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--socket-path', default=None)
    parser.add_argument('-e', '--expression', default=None)
    parser.add_argument('-p', '--module-path', default=None)
    return parser.parse_args(args)


def load_ctx_from_module_path(module_path):
    module_dir = os.path.dirname(module_path)
    if module_dir not in sys.path:
        sys.path.append(module_dir)
    ctx_module = importlib.import_module(
        os.path.basename(os.path.splitext(module_path)[0]))
    ctx_module = reload(ctx_module)
    return getattr(ctx_module, 'ctx')


# impl taken from 'pythonpy' package
def load_ctx_from_expression(expression, prefix=''):
    regex = r"({}[a-zA-Z_][a-zA-Z0-9_]*)\.?".format(prefix)
    matches = set(re.findall(regex, expression))
    for module_name in matches:
        try:
            module = importlib.import_module(module_name)
            globals()[module_name] = module
            load_ctx_from_expression(expression,
                                     prefix='{}.'.format(module_name))
        except ImportError:
            pass
    if not prefix:
        return eval(expression)


def load_ctx(load_ctx_function, **kwargs):
    ctx = load_ctx_function()
    if callable(ctx):
        ctx = ctx(**kwargs)
    return ctx


def admin_function(ctx_server, load_ctx_function):
    def admin(action, **kwargs):
        if action == 'load':
            ctx = load_ctx(load_ctx_function, **kwargs)
            ctx._admin_ = admin_function(ctx_server, load_ctx_function)
            ctx_server.proxy.ctx = ctx
        elif action == 'stop':
            ctx_server.stop()
        else:
            raise RuntimeError('unknown action: {}'.format(action))
    return admin


def main():
    args = parse_args()
    if (args.module_path and args.expression) or not \
            (args.module_path or args.expression):
        sys.exit('ctx-server: error: use either --module-path or --expression')
    if args.module_path:
        def load_ctx_function():
            return load_ctx_from_module_path(args.module_path)
    else:
        def load_ctx_function():
            return load_ctx_from_expression(args.expression)

    ctx = load_ctx(load_ctx_function)
    server = CtxProxyServer(ctx,
                            args.socket_path)
    ctx._admin_ = admin_function(server,
                                 load_ctx_function)
    print server.proxy.socket_url
    server.serve()


if __name__ == '__main__':
    main()
