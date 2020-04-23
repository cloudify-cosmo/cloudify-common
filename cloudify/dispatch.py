########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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


import copy
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
import threading
from time import sleep
import traceback

from contextlib import contextmanager

from cloudify_rest_client.executions import Execution
from cloudify_rest_client.constants import VisibilityState
from cloudify_rest_client.exceptions import (
    InvalidExecutionUpdateStatus,
    CloudifyClientError
)

from cloudify import logs
from cloudify import exceptions
from cloudify import state
from cloudify import context
from cloudify import utils
from cloudify import amqp_client_utils
from cloudify import constants
from cloudify._compat import queue, StringIO
from cloudify.amqp_client_utils import AMQPWrappedThread
from cloudify.manager import update_execution_status, get_rest_client
from cloudify.constants import LOGGING_CONFIG_FILE
from cloudify.error_handling import (
    serialize_known_exception,
    deserialize_known_exception
)
try:
    from cloudify.workflows import api
    from cloudify.workflows import workflow_context
except ImportError:
    workflow_context = None
    api = None


ENV_ENCODING = 'utf-8'  # encoding for env variables
CLOUDIFY_DISPATCH = 'CLOUDIFY_DISPATCH'

# This is relevant in integration tests when cloudify-agent is installed in
# editable mode. Adding this directory using PYTHONPATH will make it appear
# after the editable projects appear so it is not applicable in this case.
if os.environ.get('PREPEND_CWD_TO_PYTHONPATH'):
    if os.getcwd() in sys.path:
        sys.path.remove(os.getcwd())
    sys.path.insert(0, os.getcwd())

# Remove different variations in which cloudify may be added to the sys
# path
if os.environ.get(CLOUDIFY_DISPATCH):
    file_dir = os.path.dirname(__file__)
    site_packages_cloudify = os.path.join('site-packages', 'cloudify')
    for entry in copy.copy(sys.path):
        if entry == file_dir or entry.endswith(site_packages_cloudify):
            sys.path.remove(entry)

try:
    from cloudify_agent import VIRTUALENV
except ImportError:
    VIRTUALENV = sys.prefix


SYSTEM_DEPLOYMENT = '__system__'
PLUGINS_DIR = os.path.join(VIRTUALENV, 'plugins')
DISPATCH_LOGGER_FORMATTER = logging.Formatter(
    '%(asctime)s [%(name)s] %(levelname)s: %(message)s')


class LockedFile(object):
    """Like a writable file object, but writes are under a lock.

    Used for logging, so that multiple threads can write to the same logfile
    safely (deployment.log).

    We keep track of the number of users, so that we can close the file
    only when the last one stops writing.
    """
    SETUP_LOGGER_LOCK = threading.Lock()
    LOGFILES = {}

    @classmethod
    def open(cls, fn):
        """Create a new LockedFile, or get a cached one if one for this
        filename already exists.
        """
        with cls.SETUP_LOGGER_LOCK:
            if fn not in cls.LOGFILES:
                if not os.path.exists(os.path.dirname(fn)):
                    os.mkdir(os.path.dirname(fn))
                cls.LOGFILES[fn] = cls(fn)
            rv = cls.LOGFILES[fn]
            rv.users += 1
        return rv

    def __init__(self, filename):
        self._filename = filename
        self._f = None
        self.users = 0
        self._lock = threading.Lock()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def write(self, data):
        with self._lock:
            if self._f is None:
                self._f = open(self._filename, 'ab')
            self._f.write(data)
            self._f.flush()

    def close(self):
        with self.SETUP_LOGGER_LOCK:
            self.users -= 1
            if self.users == 0:
                if self._f:
                    self._f.close()
                self.LOGFILES.pop(self._filename)


class TimeoutWrapper(object):
    def __init__(self, cloudify_context, process):
        self.timeout = cloudify_context.get('timeout')
        self.timeout_recoverable = cloudify_context.get(
            'timeout_recoverable', False)
        self.timeout_encountered = False
        self.process = process
        self.timer = None
        self.logger = logging.getLogger(__name__)

    def _timer_func(self):
        self.timeout_encountered = True
        self.logger.warning("Terminating subprocess; PID=%d...",
                            self.process.pid)
        self.process.terminate()
        for i in range(10):
            if self.process.poll() is not None:
                return
            self.logger.warning("Subprocess still alive; waiting...")
            sleep(0.5)
        self.logger.warning("Subprocess still alive; sending KILL signal")
        self.process.kill()
        self.logger.warning("Subprocess killed")

    def __enter__(self):
        if self.timeout:
            self.timer = threading.Timer(self.timeout, self._timer_func)
            self.timer.start()
        return self

    def __exit__(self, *args):
        if self.timer:
            self.timer.cancel()


class TaskHandler(object):
    NOTSET = object()

    def __init__(self, cloudify_context, args, kwargs, process_registry=None):
        self.cloudify_context = cloudify_context
        self.args = args
        self.kwargs = kwargs
        self._ctx = None
        self._func = self.NOTSET
        self._logfiles = {}
        self._process_registry = process_registry

    def handle_or_dispatch_to_subprocess_if_remote(self):
        if self.cloudify_context.get('task_target'):
            return self.dispatch_to_subprocess()
        else:
            return self.handle()

    def handle(self):
        raise NotImplementedError('Implemented by subclasses')

    def run_subprocess(self, *subprocess_args, **subprocess_kwargs):
        subprocess_kwargs.setdefault('stderr', subprocess.STDOUT)
        subprocess_kwargs.setdefault('stdout', subprocess.PIPE)
        p = subprocess.Popen(*subprocess_args, **subprocess_kwargs)
        if self._process_registry:
            self._process_registry.register(self, p)

        with TimeoutWrapper(self.cloudify_context, p) as timeout_wrapper:
            with self.logfile() as f:
                while True:
                    line = p.stdout.readline()
                    if line:
                        f.write(line)
                    if p.poll() is not None:
                        break

        cancelled = False
        if self._process_registry:
            cancelled = self._process_registry.is_cancelled(self)
            self._process_registry.unregister(self, p)

        if timeout_wrapper.timeout_encountered:
            message = 'Process killed due to timeout of %d seconds' % \
                      timeout_wrapper.timeout
            if p.poll() is None:
                message += ', however it has not stopped yet; please check ' \
                           'process ID {0} manually'.format(p.pid)
            exception_class = exceptions.RecoverableError if \
                timeout_wrapper.timeout_recoverable else \
                exceptions.NonRecoverableError
            raise exception_class(message)

        if p.returncode in (-15, -9):  # SIGTERM, SIGKILL
            if cancelled:
                raise exceptions.ProcessKillCancelled()
            raise exceptions.NonRecoverableError('Process terminated (rc={0})'
                                                 .format(p.returncode))
        if p.returncode != 0:
            raise exceptions.NonRecoverableError(
                'Unhandled exception occurred in operation dispatch (rc={0})'
                .format(p.returncode))

    def logfile(self):
        try:
            handler_context = self.ctx.deployment.id
        except AttributeError:
            handler_context = SYSTEM_DEPLOYMENT
        else:
            # an operation may originate from a system wide workflow.
            # in that case, the deployment id will be None
            handler_context = handler_context or SYSTEM_DEPLOYMENT

        log_name = os.path.join(os.environ.get('AGENT_LOG_DIR', ''), 'logs',
                                '{0}.log'.format(handler_context))

        return LockedFile.open(log_name)

    def dispatch_to_subprocess(self):
        # inputs.json, output.json and output are written to a temporary
        # directory that only lives during the lifetime of the subprocess
        split = self.cloudify_context['task_name'].split('.')
        dispatch_dir = tempfile.mkdtemp(prefix='task-{0}.{1}-'.format(
            split[0], split[-1]))

        try:
            with open(os.path.join(dispatch_dir, 'input.json'), 'w') as f:
                json.dump({
                    'cloudify_context': self.cloudify_context,
                    'args': self.args,
                    'kwargs': self.kwargs
                }, f)
            if self.cloudify_context.get('bypass_maintenance'):
                os.environ[constants.BYPASS_MAINTENANCE] = 'True'
            env = self._build_subprocess_env()
            executable = self._get_executable()
            env['PATH'] = '{0}:{1}'.format(
                os.path.dirname(executable), env['PATH'])
            command_args = [executable, '-u', '-m', 'cloudify.dispatch',
                            dispatch_dir]
            self.run_subprocess(command_args,
                                env=env,
                                bufsize=1,
                                close_fds=os.name != 'nt')
            with open(os.path.join(dispatch_dir, 'output.json')) as f:
                dispatch_output = json.load(f)
            if dispatch_output['type'] == 'result':
                return dispatch_output['payload']
            elif dispatch_output['type'] == 'error':
                e = dispatch_output['payload']
                error = deserialize_known_exception(e)
                error.causes.append({
                    'message': e['message'],
                    'type': e['exception_type'],
                    'traceback': e['traceback']
                })
                raise error
            else:
                raise exceptions.NonRecoverableError(
                    'Unexpected output type: {0}'
                    .format(dispatch_output['type']))
        finally:
            shutil.rmtree(dispatch_dir, ignore_errors=True)

    def _get_executable(self):
        plugin_dir = self._extract_plugin_dir()
        if plugin_dir:
            if os.name == 'nt':
                return os.path.join(plugin_dir, 'Scripts', 'python.exe')
            else:
                return os.path.join(plugin_dir, 'bin', 'python')
        return sys.executable

    def _build_subprocess_env(self):
        env = os.environ.copy()

        # marker for code that only gets executed when inside the dispatched
        # subprocess, see usage in the imports section of this module
        env[CLOUDIFY_DISPATCH] = 'true'

        # This is used to support environment variables configurations for
        # central deployment based operations. See workflow_context to
        # understand where this value gets set initially
        # Note that this is received via json, so it is unicode. It must
        # be encoded, because environment variables must be bytes.
        execution_env = self.cloudify_context.get('execution_env') or {}
        execution_env = dict((k.encode(ENV_ENCODING), v.encode(ENV_ENCODING))
                             for k, v in execution_env.items())
        env.update(execution_env)

        if self.cloudify_context.get('bypass_maintenance'):
            env[constants.BYPASS_MAINTENANCE] = 'True'

        return env

    def _extract_plugin_dir(self):
        plugin = self.cloudify_context.get('plugin', {})
        plugin_name = plugin.get('name')
        package_name = plugin.get('package_name')
        package_version = plugin.get('package_version')
        deployment_id = self.cloudify_context.get('deployment_id',
                                                  SYSTEM_DEPLOYMENT)
        tenant = self.cloudify_context.get('tenant', {})
        tenant_name = tenant.get('name')
        plugin_visibility = plugin.get('visibility')

        if plugin_visibility == VisibilityState.GLOBAL:
            tenant_name = plugin.get('tenant_name')

        return utils.internal.plugin_prefix(
            package_name=package_name,
            package_version=package_version,
            deployment_id=deployment_id,
            plugin_name=plugin_name,
            tenant_name=tenant_name,
            sys_prefix_fallback=False)

    def setup_logging(self):
        logs.setup_subprocess_logger()
        self._update_logging_level()

    @staticmethod
    def _update_logging_level():
        if not os.path.isfile(LOGGING_CONFIG_FILE):
            return
        with open(LOGGING_CONFIG_FILE, 'r') as config_file:
            config_lines = config_file.readlines()
        for line in config_lines:
            if not line.strip() or line.startswith('#'):
                continue
            level_name, logger_name = line.split()
            level_id = logging.getLevelName(level_name.upper())
            if not isinstance(level_id, int):
                continue
            logging.getLogger(logger_name).setLevel(level_id)

    @property
    def ctx_cls(self):
        raise NotImplementedError('implemented by subclasses')

    @property
    def ctx(self):
        if not self._ctx:
            self._ctx = self.ctx_cls(self.cloudify_context)
        return self._ctx

    @property
    def func(self):
        if self._func is self.NOTSET:
            try:
                self._func = self.get_func()
            except Exception:
                self._func = None

        return self._func

    def get_func(self):
        task_name = self.cloudify_context['task_name']
        return utils.get_func(task_name)


class OperationHandler(TaskHandler):

    @property
    def ctx_cls(self):
        return context.CloudifyContext

    def _validate_operation_func(self):
        if not self.func:
            # if there is a problem importing/getting the operation function,
            # this will raise and bubble up
            self.get_func()

    def _validate_operation_resumable(self):
        if self.ctx.resume and not getattr(self._func, 'resumable', False):
            raise exceptions.NonRecoverableError(
                'Cannot resume - operation not resumable: {0}'
                .format(self._func))

    def handle(self):
        self._validate_operation_func()

        ctx = self.ctx
        kwargs = self.kwargs
        if not ctx.task_target:
            # task is local (not through AMQP) so we need to clone kwargs
            kwargs = copy.deepcopy(kwargs)

        with state.current_ctx.push(ctx, kwargs):
            if self.cloudify_context.get('has_intrinsic_functions'):
                kwargs = ctx._endpoint.evaluate_functions(payload=kwargs)

        if not self.cloudify_context.get('no_ctx_kwarg'):
            kwargs['ctx'] = ctx

        with state.current_ctx.push(ctx, kwargs):
            # should be single `with` and comma-separate ctxmanagers,
            # but has to be nested for python 2.6 compat
            with self._amqp_client():
                with self._update_operation_state():
                    self._validate_operation_resumable()
                    result = self._run_operation_func(ctx, kwargs)

            if ctx.operation._operation_retry:
                raise ctx.operation._operation_retry
        return result

    @contextmanager
    def _update_operation_state(self):
        ctx = self.ctx
        store = True
        try:
            op = ctx.get_operation()
        except CloudifyClientError as e:
            if e.status_code == 404:
                op = None
                store = False
            else:
                raise
        if op and op.state == constants.TASK_STARTED:
            # this operation has been started before? that means we're
            # resuming a re-delivered operation
            ctx.resume = True
        if store:
            ctx.update_operation(constants.TASK_STARTED)

        try:
            yield
            error = False
        except Exception:
            error = True
            raise
        finally:
            if store:
                if ctx.operation._operation_retry:
                    state = constants.TASK_RESCHEDULED
                elif error:
                    state = constants.TASK_FAILED
                else:
                    state = constants.TASK_SUCCEEDED
                ctx.update_operation(state)

    @contextmanager
    def _amqp_client(self):
        # initialize an amqp client only when needed, ie. if the task is
        # not local
        with_amqp = bool(self.ctx.task_target)
        if with_amqp:
            try:
                amqp_client_utils.init_events_publisher()
            except Exception:
                _, ex, tb = sys.exc_info()
                # This one should never (!) raise an exception.
                amqp_client_utils.close_amqp_client()
                raise exceptions.RecoverableError(
                    'Failed initializing AMQP connection',
                    causes=[utils.exception_to_error_cause(ex, tb)])
        try:
            yield
        finally:
            if with_amqp:
                amqp_client_utils.close_amqp_client()

    def _run_operation_func(self, ctx, kwargs):
        try:
            return self.func(*self.args, **kwargs)
        finally:
            if ctx.type == constants.NODE_INSTANCE:
                ctx.instance.update()
            elif ctx.type == constants.RELATIONSHIP_INSTANCE:
                ctx.source.instance.update()
                ctx.target.instance.update()


class WorkflowHandler(TaskHandler):

    def __init__(self, *args, **kwargs):
        if workflow_context is None or api is None:
            raise RuntimeError('Dispatcher not installed')
        super(WorkflowHandler, self).__init__(*args, **kwargs)
        self.execution_parameters = copy.deepcopy(self.kwargs)

    @property
    def ctx_cls(self):
        if getattr(self.func, 'workflow_system_wide', False):
            return workflow_context.CloudifySystemWideWorkflowContext
        return workflow_context.CloudifyWorkflowContext

    def handle(self):
        self.kwargs['ctx'] = self.ctx
        with state.current_workflow_ctx.push(self.ctx, self.kwargs):
            self._validate_workflow_func()
            if self.ctx.local or self.ctx.dry_run:
                return self._handle_local_workflow()
            return self._handle_remote_workflow()

    def _validate_workflow_func(self):
        try:
            if not self.func:
                self.get_func()
            if self.ctx.resume and not getattr(self._func, 'resumable', False):
                raise exceptions.NonRecoverableError(
                    'Cannot resume - workflow not resumable: {0}'
                    .format(self._func))
        except Exception as e:
            self._workflow_failed(e, traceback.format_exc())
            raise

    @property
    def update_execution_status(self):
        return self.cloudify_context.get('update_execution_status', True)

    def _handle_remote_workflow(self):
        tenant = self.ctx._context['tenant'].get('original_name',
                                                 self.ctx.tenant_name)
        rest = get_rest_client(tenant=tenant)
        execution = rest.executions.get(self.ctx.execution_id,
                                        _include=['status'])
        if execution.status == Execution.STARTED:
            self.ctx.resume = True

        try:
            amqp_client_utils.init_events_publisher()
            try:
                self._workflow_started()
            except InvalidExecutionUpdateStatus:
                self._workflow_cancelled()
                return api.EXECUTION_CANCELLED_RESULT

            result_queue = queue.Queue()
            t = AMQPWrappedThread(target=self._remote_workflow_child_thread,
                                  args=(result_queue,),
                                  name='Workflow-Child')
            t.start()

            # while the child thread is executing the workflow, the parent
            # thread is polling for 'cancel' requests while also waiting for
            # messages from the child thread
            result = None
            while True:
                # check if child thread sent a message
                try:
                    data = result_queue.get(timeout=5)
                    if 'result' in data:
                        # child thread has terminated
                        result = data['result']
                        break
                    else:
                        # error occurred in child thread
                        error = data['error']
                        raise exceptions.ProcessExecutionError(
                            error['message'],
                            error['type'],
                            error['traceback'])
                except queue.Empty:
                    pass

                # A very hacky way to solve an edge case when trying to poll
                # for the execution status while the DB is downgraded during
                # a snapshot restore
                if self.cloudify_context['workflow_id'] == 'restore_snapshot':
                    continue

                # check for 'cancel' requests
                execution = rest.executions.get(self.ctx.execution_id,
                                                _include=['status'])
                if execution.status in [
                        Execution.CANCELLING,
                        Execution.FORCE_CANCELLING,
                        Execution.KILL_CANCELLING]:
                    # send a 'cancel' message to the child thread. It is up to
                    # the workflow implementation to check for this message
                    # and act accordingly (by stopping and raising an
                    # api.ExecutionCancelled error, or by returning the
                    # deprecated api.EXECUTION_CANCELLED_RESULT as result).
                    # parent thread then goes back to polling for messages from
                    # child thread or possibly 'force-cancelling' requests
                    api.cancel_request = True

                if execution.status == Execution.KILL_CANCELLING:
                    # if a custom workflow function must attempt some cleanup,
                    # it might attempt to catch SIGTERM, and confirm using this
                    # flag that it is being kill-cancelled
                    api.kill_request = True

                if execution.status in [
                        Execution.FORCE_CANCELLING,
                        Execution.KILL_CANCELLING]:
                    # force-cancel additionally stops this loop immediately
                    result = api.EXECUTION_CANCELLED_RESULT
                    break

            if result == api.EXECUTION_CANCELLED_RESULT:
                self._workflow_cancelled()
            else:
                self._workflow_succeeded()
            return result
        except exceptions.ProcessExecutionError as e:
            self._workflow_failed(e, e.traceback)
            raise
        except BaseException as e:
            self._workflow_failed(e, traceback.format_exc())
            raise
        finally:
            amqp_client_utils.close_amqp_client()

    def _remote_workflow_child_thread(self, queue):
        # the actual execution of the workflow will run in another thread.
        # this method is the entry point for that thread, and takes care of
        # forwarding the result or error back to the parent thread
        with state.current_workflow_ctx.push(self.ctx, self.kwargs):
            try:
                workflow_result = self._execute_workflow_function()
                queue.put({'result': workflow_result})
            except api.ExecutionCancelled:
                queue.put({'result': api.EXECUTION_CANCELLED_RESULT})
            except BaseException as workflow_ex:
                tb = StringIO()
                traceback.print_exc(file=tb)
                err = {
                    'type': type(workflow_ex).__name__,
                    'message': str(workflow_ex),
                    'traceback': tb.getvalue()
                }
                queue.put({'error': err})

    def _handle_local_workflow(self):
        try:
            self._workflow_started()
            result = self._execute_workflow_function()
            self._workflow_succeeded()
            return result
        except Exception as e:
            error = StringIO()
            traceback.print_exc(file=error)
            self._workflow_failed(e, error.getvalue())
            raise

    def _execute_workflow_function(self):
        try:
            self.ctx.internal.start_local_tasks_processing()
            result = self.func(*self.args, **self.kwargs)
            if not self.ctx.internal.graph_mode:
                tasks = list(self.ctx.internal.task_graph.tasks_iter())
                for workflow_task in tasks:
                    workflow_task.async_result.get()
            return result
        finally:
            self.ctx.internal.stop_local_tasks_processing()

    def _workflow_started(self):
        self._update_execution_status(Execution.STARTED)
        dry_run = ' (dry run)' if self.ctx.dry_run else ''
        start_resume = 'Resuming' if self.ctx.resume else 'Starting'
        self.ctx.internal.send_workflow_event(
            event_type='workflow_started',
            message="{0} '{1}' workflow execution{2}".format(
                start_resume, self.ctx.workflow_id, dry_run),
            additional_context=self._get_hook_params()
        )

    def _workflow_succeeded(self):
        self._update_execution_status(Execution.TERMINATED)
        dry_run = ' (dry run)' if self.ctx.dry_run else ''
        self.ctx.internal.send_workflow_event(
            event_type='workflow_succeeded',
            message="'{0}' workflow execution succeeded{1}".format(
                self.ctx.workflow_id, dry_run),
            additional_context=self._get_hook_params()
        )

    def _workflow_failed(self, exception, error_traceback):
        try:
            self.ctx.internal.send_workflow_event(
                event_type='workflow_failed',
                message="'{0}' workflow execution failed: {1}".format(
                    self.ctx.workflow_id, str(exception)),
                args={'error': error_traceback},
                additional_context=self._get_hook_params()
            )
            self._update_execution_status(Execution.FAILED, error_traceback)
        except Exception:
            logger = logging.getLogger(__name__)
            logger.exception('Exception raised when attempting to update '
                             'execution state')
            raise exception

    def _workflow_cancelled(self):
        self._update_execution_status(Execution.CANCELLED)
        self.ctx.internal.send_workflow_event(
            event_type='workflow_cancelled',
            message="'{0}' workflow execution cancelled".format(
                self.ctx.workflow_id),
            additional_context=self._get_hook_params()
        )

    def _get_hook_params(self):
        is_system_workflow = self.cloudify_context.get('is_system_workflow')
        hook_params = {
            'message_type': 'hook',
            'is_system_workflow': is_system_workflow,
            'rest_token': self.cloudify_context.get('rest_token')
        }

        if not is_system_workflow:
            hook_params['execution_parameters'] = self.execution_parameters
        return hook_params

    def _update_execution_status(self, status, error=None):
        if self.ctx.local or not self.update_execution_status:
            return

        caught_error = None
        for _ in range(3):
            try:
                return update_execution_status(
                    self.ctx.execution_id, status, error)
            except Exception as e:
                self.ctx.logger.exception(
                    'Update execution status got unexpected rest error: %s', e)
                caught_error = e
                sleep(5)
        else:
            raise caught_error


TASK_HANDLERS = {
    'operation': OperationHandler,
    'hook': OperationHandler,
    'workflow': WorkflowHandler
}


def dispatch(__cloudify_context, *args, **kwargs):
    dispatch_type = __cloudify_context['type']
    dispatch_handler_cls = TASK_HANDLERS.get(dispatch_type)
    if not dispatch_handler_cls:
        raise exceptions.NonRecoverableError('No handler for task type: {0}'
                                             .format(dispatch_type))
    handler = dispatch_handler_cls(cloudify_context=__cloudify_context,
                                   args=args,
                                   kwargs=kwargs)
    return handler.handle_or_dispatch_to_subprocess_if_remote()


def main():
    dispatch_dir = sys.argv[1]
    with open(os.path.join(dispatch_dir, 'input.json')) as f:
        dispatch_inputs = json.load(f)
    cloudify_context = dispatch_inputs['cloudify_context']
    args = dispatch_inputs['args']
    kwargs = dispatch_inputs['kwargs']
    dispatch_type = cloudify_context['type']
    threading.current_thread().setName('Dispatch-{0}'.format(dispatch_type))
    handler_cls = TASK_HANDLERS[dispatch_type]
    handler = None
    try:
        handler = handler_cls(cloudify_context=cloudify_context,
                              args=args,
                              kwargs=kwargs)
        handler.setup_logging()
        payload = handler.handle()
        payload_type = 'result'
    except BaseException as e:
        payload_type = 'error'
        payload = serialize_known_exception(e)

        logger = logging.getLogger(__name__)
        logger.error('Task {0}[{1}] raised:\n{2}'.format(
            handler.cloudify_context['task_name'],
            handler.cloudify_context.get('task_id', '<no-id>'),
            payload['traceback']))

    with open(os.path.join(dispatch_dir, 'output.json'), 'w') as f:
        json.dump({
            'type': payload_type,
            'payload': payload
        }, f)


if __name__ == '__main__':
    main()
