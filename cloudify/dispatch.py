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
import sys
import threading
import traceback

from cloudify_rest_client.executions import Execution
from cloudify_rest_client.exceptions import InvalidExecutionUpdateStatus

from cloudify import logs
from cloudify import exceptions
from cloudify import state
from cloudify import context
from cloudify import utils
from cloudify import constants
from cloudify._compat import queue, StringIO
from cloudify.manager import update_execution_status, get_rest_client
from cloudify.constants import LOGGING_CONFIG_FILE
from cloudify.error_handling import serialize_known_exception

try:
    from cloudify.workflows import api
    from cloudify.workflows import workflow_context
except ImportError:
    workflow_context = None
    api = None


DISPATCH_LOGGER_FORMATTER = logging.Formatter(
    '%(asctime)s [%(name)s] %(levelname)s: %(message)s')


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

    def handle(self):
        raise NotImplementedError('Implemented by subclasses')

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
            self._validate_operation_resumable()
            result = self._run_operation_func(ctx, kwargs)

            if ctx.operation._operation_retry:
                raise ctx.operation._operation_retry
        return result

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
            try:
                self._workflow_started()
            except InvalidExecutionUpdateStatus:
                self._workflow_cancelled()
                return api.EXECUTION_CANCELLED_RESULT

            result_queue = queue.Queue()
            t = threading.Thread(target=self._remote_workflow_child_thread,
                                 args=(result_queue,),
                                 name='Workflow-Child')
            t.daemon = True
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
                        raise data['error']
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
                    api.set_cancel_request()

                if execution.status == Execution.KILL_CANCELLING:
                    # if a custom workflow function must attempt some cleanup,
                    # it might attempt to catch SIGTERM, and confirm using this
                    # flag that it is being kill-cancelled
                    api.set_kill_request()

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
        except exceptions.WorkflowFailed as e:
            self._workflow_failed(e, traceback.format_exc())
            raise
        except BaseException as e:
            self._workflow_failed(e, traceback.format_exc())
            raise

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
                queue.put({'error': workflow_ex})

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
                for workflow_task in self.ctx.internal.task_graph.tasks:
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
        )

    def _workflow_succeeded(self):
        self.ctx.cleanup(finished=True)
        dry_run = ' (dry run)' if self.ctx.dry_run else ''
        self.ctx.internal.send_workflow_event(
            event_type='workflow_succeeded',
            message="'{0}' workflow execution succeeded{1}".format(
                self.ctx.workflow_id, dry_run),
        )
        self._update_execution_status(Execution.TERMINATED)

    def _workflow_failed(self, exception, error_traceback=None):
        self.ctx.cleanup(finished=True)
        try:
            self.ctx.internal.send_workflow_event(
                event_type='workflow_failed',
                message="'{0}' workflow execution failed: {1}".format(
                    self.ctx.workflow_id, exception),
                args={'error': error_traceback},
            )
            self._update_execution_status(Execution.FAILED, error_traceback)
        except Exception:
            logger = logging.getLogger(__name__)
            logger.exception('Exception raised when attempting to update '
                             'execution state')
            raise exception

    def _workflow_cancelled(self):
        self.ctx.cleanup(finished=False)
        self.ctx.internal.send_workflow_event(
            event_type='workflow_cancelled',
            message="'{0}' workflow execution cancelled".format(
                self.ctx.workflow_id),
        )
        self._update_execution_status(Execution.CANCELLED)

    def _update_execution_status(self, status, error=None):
        if self.ctx.local or not self.update_execution_status:
            return
        return update_execution_status(self.ctx.execution_id, status, error)


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
    return handler.handle()


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
            payload.get('traceback')))

    with open(os.path.join(dispatch_dir, 'output.json'), 'w') as f:
        json.dump({
            'type': payload_type,
            'payload': payload
        }, f)


if __name__ == '__main__':
    main()
