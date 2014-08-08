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
from StringIO import StringIO

from cloudify.decorators import operation
from cloudify.exceptions import NonRecoverableError

from ctx_proxy import UnixCtxProxyServer, CTX_SOCKET_URL


@operation
def run(ctx, **kwargs):
    script_path = get_script_to_run(ctx)
    if script_path:
        execute(script_path, ctx)


def get_script_to_run(ctx):
    script_path = ctx.properties.get('script_path')
    if script_path:
        script_path = ctx.download_resource(script_path)
    elif 'scripts' in ctx.properties:
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


def execute(script_path, ctx):
    ctx.logger.info('Executing: {}'.format(script_path))

    ctx_proxy_server = UnixCtxProxyServer(ctx)
    env = os.environ.copy()
    env[CTX_SOCKET_URL] = ctx_proxy_server.socket_url

    process = subprocess.Popen(script_path,
                               shell=True,
                               stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE,
                               env=env)

    make_async(process.stdout, process.stderr)

    stdout = StringIO()
    stderr = StringIO()
    return_code = None

    while True:
        # Wait for process output (stdout, stderr)
        select.select([process.stdout, process.stderr], [], [], 0.1)

        # Check if a context request is pending and process it
        ctx_proxy_server.poll_and_process(timeout=0.1)

        return_code = process.poll()

        # Try reading some stdout/stderr from process
        stdout_chunk = read_async(process.stdout)
        stderr_chunk = read_async(process.stderr)

        stdout.write(stdout_chunk)
        stderr.write(stderr_chunk)

        if return_code is not None and not (stdout_chunk or stderr_chunk):
            break

    ctx_proxy_server.close()

    ctx.logger.info('Execution done (return_code={}): {}'
                    .format(return_code, script_path))

    if return_code != 0:
        raise ProcessException(script_path,
                               return_code,
                               stdout.getvalue(),
                               stderr.getvalue())


def make_async(*fds):
    """
    Helper function to add the O_NONBLOCK flag to a file descriptor
    list.
    """
    for fd in fds:
        flags = fcntl.fcntl(fd, fcntl.F_GETFL) | os.O_NONBLOCK
        fcntl.fcntl(fd, fcntl.F_SETFL, flags)


def read_async(fd):
    """
    Helper function to read some data from a file descriptor,
    ignoring EAGAIN errors.
    """
    try:
        return fd.readline()
    except IOError, e:
        if e.errno != errno.EAGAIN:
            raise
        else:
            return ''


class ProcessException(Exception):

    def __init__(self, command, exit_code, stdout, stderr):
        super(ProcessException, self).__init__(stderr)
        self.command = command
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr
