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

import ast
import codecs
import os
import re
import shutil
import sys
import time
import json
import tempfile
import textwrap
import subprocess
from contextlib import contextmanager

import requests

from cloudify import ctx as operation_ctx
from cloudify.utils import (create_temp_folder,
                            get_exec_tempdir,
                            OutputConsumer)
from cloudify.workflows import ctx as workflows_ctx
from cloudify.decorators import operation, workflow
from cloudify.exceptions import NonRecoverableError
from cloudify.proxy.client import CTX_SOCKET_URL
from cloudify.proxy.server import HTTPCtxProxy, StubCtxProxy

from script_runner import eval_env
from script_runner import constants

try:
    from cloudify.proxy.client import ScriptException
except ImportError:
    ScriptException = None


ILLEGAL_CTX_OPERATION_ERROR = RuntimeError('ctx may only abort or return once')
UNSUPPORTED_SCRIPT_FEATURE_ERROR = \
    RuntimeError('ctx abort & retry commands are only supported in Cloudify '
                 '3.4 or later')

IS_WINDOWS = os.name == 'nt'

POLL_LOOP_INTERVAL = 0.1
POLL_LOOP_LOG_ITERATIONS = 200

DEFAULT_TASK_LOG_DIR = os.path.join(tempfile.gettempdir(), 'cloudify')


@operation
def run(
    script_path=None,
    script_source=None,
    process=None,
    ssl_cert_content=None,
    **kwargs,
):
    ctx = operation_ctx._get_current_object()
    if script_path is None and script_source is None:
        raise NonRecoverableError(
            'Either script_path or script_source must be provided')
    if script_source and script_path:
        raise NonRecoverableError(
            'Only provide script_path or script_source, not both')
    process = create_process_config(process or {}, kwargs)
    if script_source:
        script_path = _script_source_to_file(script_source, process)
    else:
        script_path = download_resource(
            ctx.download_resource,
            script_path,
            ssl_cert_content
        )
    os.chmod(script_path, 0o755)
    script_func = get_run_script_func(script_path, process)
    try:
        script_result = process_execution(script_func, script_path, ctx,
                                          process)
    finally:
        os.remove(script_path)
    return script_result


@workflow
def execute_workflow(
    script_path=None,
    script_source=None,
    ssl_cert_content=None,
    **kwargs,
):
    if script_path is None and script_source is None:
        raise NonRecoverableError(
            'Either script_path or script_source must be provided')
    if script_source and script_path:
        raise NonRecoverableError(
            'Only provide script_path or script_source, not both')
    ctx = workflows_ctx._get_current_object()
    if script_source:
        script_path = _script_source_to_file(
            script_source,
            {'eval_python': True},
        )
    else:
        script_path = download_resource(
            ctx.internal.handler.download_deployment_resource,
            script_path,
            ssl_cert_content,
        )
    try:
        script_result = process_execution(eval_script, script_path, ctx)
    finally:
        os.remove(script_path)
    return script_result


def create_process_config(process, operation_kwargs):
    env_vars = operation_kwargs.copy()
    if 'ctx' in env_vars:
        del env_vars['ctx']
    env_vars.update(process.get('env', {}))
    output_env_vars = {}
    for k, v in env_vars.items():
        k = str(k)
        if isinstance(v, (dict, list, set, bool)):
            envvar_value = json.dumps(v)
            if IS_WINDOWS:
                # the windows shell removes all double quotes - escape them
                # to still be able to pass JSON in env vars to the shell
                envvar_value = envvar_value.replace('"', '\\"')

            output_env_vars[k] = envvar_value
        else:
            output_env_vars[k] = str(v)
    process['env'] = output_env_vars
    return process


def process_execution(script_func, script_path, ctx, process=None):
    ctx.is_script_exception_defined = ScriptException is not None

    def abort_operation(message=None):
        if ctx._return_value is not None:
            ctx._return_value = ILLEGAL_CTX_OPERATION_ERROR
            raise ctx._return_value
        if ctx.is_script_exception_defined:
            ctx._return_value = ScriptException(message)
        else:
            ctx._return_value = UNSUPPORTED_SCRIPT_FEATURE_ERROR
            raise ctx._return_value
        return ctx._return_value

    def retry_operation(message=None, retry_after=None):
        if ctx._return_value is not None:
            ctx._return_value = ILLEGAL_CTX_OPERATION_ERROR
            raise ctx._return_value
        if ctx.is_script_exception_defined:
            ctx._return_value = ScriptException(message, retry=True)
            ctx.operation.retry(message=message, retry_after=retry_after)
        else:
            ctx._return_value = UNSUPPORTED_SCRIPT_FEATURE_ERROR
            raise ctx._return_value
        return ctx._return_value

    ctx.abort_operation = abort_operation
    ctx.retry_operation = retry_operation

    def returns(value):
        if ctx._return_value is not None:
            ctx._return_value = ILLEGAL_CTX_OPERATION_ERROR
            raise ctx._return_value
        ctx._return_value = value
    ctx.returns = returns

    ctx._return_value = None

    script_func(script_path, ctx, process)
    script_result = ctx._return_value
    if (ctx.is_script_exception_defined and
       isinstance(script_result, ScriptException)):
        if script_result.retry:
            return script_result
        else:
            raise NonRecoverableError(str(script_result))
    else:
        return script_result


def _script_source_to_file(script_source, process):
    """Write script_source to a file, with an auto-detected file extension.

    If the script is valid python syntax, we'll consider it a python
    script, otherwise it's a shell script.

    eval_python still overrides this auto-detection.

    Return a filename in the tempdir.
    """
    is_python = False
    script_source = textwrap.dedent(script_source)
    if process.get('eval_python'):
        is_python = True
    else:
        try:
            ast.parse(script_source)
        except SyntaxError:
            pass
        else:
            is_python = True

    if is_python:
        filename = 'script.py'
    else:
        filename = 'script.sh'

    script_path = os.path.join(create_temp_folder(), filename)
    with open(script_path, 'w') as f:
        f.write(script_source)
    return script_path


def treat_script_as_python_script(script_path, process):
    eval_python = process.get('eval_python')
    script_extension = os.path.splitext(script_path)[1].lower()
    return eval_python is True or \
        (script_extension == constants.PYTHON_SCRIPT_FILE_EXTENSION and
         eval_python is not False)


def treat_script_as_powershell_script(script_path):
    script_extension = os.path.splitext(script_path)[1].lower()
    return script_extension == constants.POWERSHELL_SCRIPT_FILE_EXTENSION


def get_run_script_func(script_path, process):
    if treat_script_as_python_script(script_path, process):
        return eval_script
    else:
        if treat_script_as_powershell_script(script_path):
            process.setdefault('command_prefix',
                               constants.DEFAULT_POWERSHELL_EXECUTABLE)
        return execute


def execute(script_path, ctx, process):
    on_posix = 'posix' in sys.builtin_module_names

    proxy = start_ctx_proxy(ctx, process)
    env = _get_process_environment(process, proxy)
    cwd = process.get('cwd')

    command_prefix = process.get('command_prefix')
    if command_prefix:
        command = '{0} {1}'.format(command_prefix, script_path)
    else:
        command = script_path

    args = process.get('args')
    if args:
        command = ' '.join([command] + args)

    # Figure out logging.

    log_stdout = process.get('log_stdout', True)
    log_stderr = process.get('log_stderr', True)
    stderr_to_stdout = process.get('stderr_to_stdout', False)

    ctx.logger.debug('log_stdout=%r, log_stderr=%r, stderr_to_stdout=%r',
                     log_stdout, log_stderr, stderr_to_stdout)

    if log_stderr:
        stderr_value = subprocess.STDOUT if stderr_to_stdout \
            else subprocess.PIPE
    else:
        stderr_value = None

    consume_stderr = stderr_value == subprocess.PIPE

    process = subprocess.Popen(
        args=command,
        shell=True,
        stdout=subprocess.PIPE if log_stdout else None,
        stderr=stderr_value,
        env=env,
        cwd=cwd,
        bufsize=1,
        close_fds=on_posix)

    pid = process.pid
    ctx.logger.info('Process created, PID: {0}'.format(pid))

    stdout_consumer = stderr_consumer = None

    if log_stdout:
        stdout_consumer = OutputConsumer(
            process.stdout, ctx.logger, '<out> ',
            ctx=operation_ctx._get_current_object())
        ctx.logger.debug('Started consumer thread for stdout')
    if consume_stderr:
        stderr_consumer = OutputConsumer(
            process.stderr, ctx.logger, '<err> ',
            ctx=operation_ctx._get_current_object())
        ctx.logger.debug('Started consumer thread for stderr')

    log_counter = 0
    while True:
        process_ctx_request(proxy)
        return_code = process.poll()
        if return_code is not None:
            break
        time.sleep(POLL_LOOP_INTERVAL)

        log_counter += 1
        if log_counter == POLL_LOOP_LOG_ITERATIONS:
            log_counter = 0
            ctx.logger.info('Waiting for process {0} to end...'.format(pid))

    ctx.logger.info('Execution done (PID={0}, return_code={1}): {2}'
                    .format(pid, return_code, command))

    try:
        proxy.close()
    except Exception:
        ctx.logger.warning('Failed closing context proxy', exc_info=True)
    else:
        ctx.logger.debug("Context proxy closed")

    for consumer, name in [(stdout_consumer, 'stdout'),
                           (stderr_consumer, 'stderr')]:
        if consumer:
            ctx.logger.debug('Joining consumer thread for %s', name)
            consumer.join()
            ctx.logger.debug('Consumer thread for %s ended', name)
        else:
            ctx.logger.debug('Consumer thread for %s not created; not joining',
                             name)

    # happens when more than 1 ctx result command is used
    if isinstance(ctx._return_value, RuntimeError):
        raise NonRecoverableError(str(ctx._return_value))
    elif return_code != 0:
        if not (ctx.is_script_exception_defined and
                isinstance(ctx._return_value, ScriptException)):
            if log_stdout:
                stdout = ''.join(stdout_consumer.output)
            else:
                stdout = process.stdout.read().decode('utf-8', 'replace')
            if stderr_to_stdout:
                stderr = None
            elif log_stderr:
                stderr = ''.join(stderr_consumer.output)
            else:
                stderr = process.stderr.read().decode('utf-8', 'replace')
            raise ProcessException(command, return_code, stdout, stderr)


def start_ctx_proxy(ctx, process):
    ctx_proxy_type = process.get('ctx_proxy_type')
    if not ctx_proxy_type or ctx_proxy_type in ('auto', 'http'):
        return HTTPCtxProxy(ctx)
    elif ctx_proxy_type == 'none':
        return StubCtxProxy()
    else:
        raise NonRecoverableError('Unsupported proxy type: {0}'
                                  .format(ctx_proxy_type))


def process_ctx_request(proxy):
    if isinstance(proxy, StubCtxProxy):
        return
    if isinstance(proxy, HTTPCtxProxy):
        return
    proxy.poll_and_process(timeout=0)


def eval_script(script_path, ctx, process=None):
    eval_globals = eval_env.setup_env_and_globals(script_path)
    with codecs.open(script_path, mode='r',
                     encoding='utf-8', errors='replace') as f:
        script_source = f.read()
    exec(compile(script_source, script_path, 'exec'), eval_globals)


def _get_target_path(source):
    # to extract a human-readable suffix from the source path, split by
    # both backslash (to handle windows filesystem) and forward slash
    # (to handle URLs and unix filesystem)
    suffix = re.split(r'\\|/', source)[-1]
    return os.path.join(create_temp_folder(), suffix)


def _get_process_environment(process, proxy):
    """Get env to be used by the script process.

    This env must at the very least contain the proxy url, and a PATH
    allowing bash scripts to use `ctx`, which is expected to live next to
    the current executable.
    """
    env = os.environ.copy()
    env.setdefault('TMPDIR', get_exec_tempdir())
    process_env = process.get('env', {})
    env.update(process_env)

    env[CTX_SOCKET_URL] = proxy.socket_url

    env_path = env.get('PATH')
    bin_dir = os.path.dirname(sys.executable)
    if env_path:
        if bin_dir not in env_path.split(os.pathsep):
            env['PATH'] = os.pathsep.join([env_path, bin_dir])
    else:
        env['PATH'] = bin_dir

    return env


def download_resource(
    download_resource_func,
    script_path,
    ssl_cert_content=None,
):
    split = script_path.split('://')
    schema = split[0]
    target_path = _get_target_path(script_path)
    if schema in ['http', 'https']:
        with _prepare_ssl_cert(ssl_cert_content) as cert_file:
            response = requests.get(script_path, verify=cert_file)
        # We only accept HTTP 200. Any other code (including other 2xx codes)
        # would imply that something unexpected happened.
        if response.status_code != requests.codes.ok:
            raise NonRecoverableError('Failed downloading script: {0} ('
                                      'status code: {1})'
                                      .format(script_path,
                                              response.status_code))
        content = response.text
        with open(target_path, 'w') as f:
            f.write(content)
        return target_path
    else:
        # Specifically check for "file://" url. We can't do that in the
        # condition above ('if schema in...') because the "requests"
        # package doesn't ship with built-in support for "file://"
        # URL's.
        if schema == 'file' and len(split) > 1:
            shutil.copyfile(split[1], target_path)
            result = target_path
        else:
            result = download_resource_func(script_path, target_path)
        return result


@contextmanager
def _prepare_ssl_cert(ssl_cert_content):
    cert_file = None
    if ssl_cert_content:
        with tempfile.NamedTemporaryFile(delete=False, mode='w') as f:
            f.write(ssl_cert_content)
            cert_file = f.name
    try:
        yield cert_file
    finally:
        if cert_file:
            os.unlink(cert_file)


class ProcessException(Exception):
    def __init__(self, command, exit_code, stdout=None, stderr=None):
        self.command = command
        self.exit_code = exit_code
        self.stdout = stdout
        self.stderr = stderr

        message = 'command: {0}, exit_code: {1}'.format(
            command, exit_code)

        if stdout:
            message += ', stdout: {0}'.format(stdout)
        if stderr:
            message += ', stderr: {0}'.format(stderr)

        super(ProcessException, self).__init__(message)
