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


import subprocess
import fcntl
import select
import os
import errno

from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from ctx_proxy import CtxProxyServer, CTX_SOCKET_PATH


@operation
def run(ctx, **kwargs):
    script_path = get_script_to_run(ctx)
    if not script_path:
        return
    execute(script_path, ctx)
    return '[{0}] succeeded. return code 0' \
           .format(os.path.basename(script_path))


def get_script_to_run(ctx):
    script_path = ctx.properties.get('script_path')
    if script_path:
        script_path = ctx.download_resource(script_path)
    if 'scripts' in ctx.properties:
        operation_simple_name = ctx.operation.split('.')[-1:].pop()
        scripts = ctx.properties['scripts']
        if operation_simple_name not in scripts:
            ctx.logger.info("No script mapping found for operation {0}. "
                            "Nothing to do.".format(operation_simple_name))
            return None
        script_path = ctx.download_resource(scripts[operation_simple_name])
    if not script_path:
        raise NonRecoverableError('No script to run was defined either in '
                                  '"script_path" node property on under '
                                  '"scripts/{operation}" node property')
    os.chmod(script_path, 0755)
    return script_path


def execute(command, ctx):
    ctx.logger.info('Running command: {}'.format(command))

    ctx_proxy_server = CtxProxyServer(ctx)
    env = os.environ.copy()
    env[CTX_SOCKET_PATH] = ctx_proxy_server.socket_path

    process = subprocess.Popen(command,
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               env=env)
    make_async(process.stdout)
    make_async(process.stderr)

    stdout = ''
    stderr = ''
    return_code = None

    while True:
        # Wait for data to become available
        select.select([process.stdout, process.stderr], [], [], 0.1)

        ctx_proxy_server.poll_and_process(timeout=0.1)

        return_code = process.poll()

        # Try reading some data from each
        stdout_chunk = read_async(process.stdout)
        stderr_chunk = read_async(process.stderr)

        stdout += stdout_chunk
        stderr += stderr_chunk

        if return_code is not None and not (stdout_chunk or stderr_chunk):
            break

    ctx_proxy_server.close()

    ctx.logger.info('Done running command (return_code={}): {}'
                    .format(return_code, command))

    if return_code == 0:
        return stdout
    else:
        raise ProcessException(command, return_code, stdout, stderr)


# Helper function to add the O_NONBLOCK flag to a file descriptor
def make_async(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, flags)


# Helper function to read some data from a file descriptor,
# ignoring EAGAIN errors
def read_async(fd):
    try:
        return fd.readline()
    except IOError, e:
        if e.errno != errno.EAGAIN:
            raise e
        else:
            return ''


class ProcessException(Exception):
    def __init__(self, command, exit_code, stdout, stderr):
        super(ProcessException, self).__init__(stderr)
        self.command = command
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
