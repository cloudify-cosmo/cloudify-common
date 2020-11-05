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

import sys
import os
import shutil
import logging
import tempfile

from mock import patch, MagicMock, Mock
import testtools

from cloudify import amqp_client, amqp_client_utils
from cloudify import dispatch
from cloudify import exceptions
from cloudify import utils
from cloudify.workflows import tasks
from cloudify_rest_client.exceptions import InvalidExecutionUpdateStatus


class TestDispatchTaskHandler(testtools.TestCase):
    def setUp(self):
        super(TestDispatchTaskHandler, self).setUp()
        self.temp_log_dir = tempfile.mkdtemp()
        self.addCleanup(shutil.rmtree, self.temp_log_dir)

    def test_handle_or_dispatch_to_subprocess(self):
        expected_result = 'the result'
        local_op_handler = self._operation(
            func1, args=[expected_result])
        subprocess_op_handler = self._operation(
            func1, task_target='stub', args=[expected_result])
        for handler in [local_op_handler, subprocess_op_handler]:
            result = handler.handle_or_dispatch_to_subprocess_if_remote()
            self.assertEqual(expected_result, result)

    def test_register_subprocess(self):
        process_registry = Mock()
        op_handler = self._operation(func2, task_target='stub',
                                     process_registry=process_registry)
        op_handler.dispatch_to_subprocess()
        register_calls = process_registry.register.mock_calls
        self.assertEqual(len(register_calls), 1)

    @patch('cloudify.dispatch.amqp_client_utils')
    def test_dispatch_update_operation_resume(self, _):
        """When the operation was already started, we set resume=true

        (and the function fails because it's not resumable)
        """
        args = [1]
        op_handler = self._operation(
            func1, task_target='stub', args=args)
        with patch('cloudify.context.CloudifyContext.get_operation',
                   return_value=Mock(state=tasks.TASK_STARTED)):
            exc = self.assertRaises(exceptions.NonRecoverableError,
                                    op_handler.handle)
        self.assertIn('not resumable', str(exc))

    def test_dispatch_resume(self):
        args = [1, 2]
        kwargs = {'one': 1, 'two': 2}
        op_handler = self._operation(
            func2, task_target='stub', args=args, kwargs=kwargs)
        result = op_handler.dispatch_to_subprocess()
        self.assertEqual([args, kwargs], result)

    def test_dispatch_to_subprocess_env(self):
        existing_env_var_key = 'EXISTING_ENV_VAR'
        existing_env_var_value = 'existing_value'
        custom_env_var_key = 'CUSTOM_ENV_VAR'
        custom_env_var_value = 'custom_value'
        env_vars_keys = [existing_env_var_key, custom_env_var_key]
        env_vars_values = [existing_env_var_value, custom_env_var_value]
        op_handler = self._operation(
            func3,
            task_target='stub',
            execution_env={custom_env_var_key: custom_env_var_value},
            args=[env_vars_keys])
        with patch.dict(os.environ, {
                existing_env_var_key: existing_env_var_value}):
            result = op_handler.dispatch_to_subprocess()
        self.assertEqual(env_vars_values, result)

    def test_dispatch_to_subprocess_exception(self):
        exception_types = [
            (exceptions.NonRecoverableError, ('message',)),
            (exceptions.RecoverableError, ('message', 'retry_after')),
            (exceptions.OperationRetry, ('message', 'retry_after')),
            (exceptions.HttpException, ('url', 'code', 'message')),
            ((UserException, exceptions.RecoverableError), ('message',)),
            ((RecoverableUserException, exceptions.RecoverableError),
             ('message', 'retry_after')),
            ((NonRecoverableUserException, exceptions.NonRecoverableError),
             ('message',))
        ]
        for raised_exception_type, args in exception_types:
            kwargs = {'args': args}
            if isinstance(raised_exception_type, tuple):
                raised_exception_type, known_ex_type = raised_exception_type
                kwargs['user_exception'] = raised_exception_type.__name__
            else:
                known_ex_type = raised_exception_type
                kwargs['known_exception'] = known_ex_type.__name__
            op_handler = self._operation(
                func6, task_target='stub', kwargs=kwargs)
            e = self.assertRaises(
                known_ex_type, op_handler.dispatch_to_subprocess)
            self.assertEqual(1, len(e.causes))
            cause = e.causes[0]
            self.assertIn('message', cause['message'])
            self.assertEqual(raised_exception_type.__name__,
                             cause['type'])

            if raised_exception_type is not exceptions.OperationRetry:
                # retries have no tracebacks
                self.assertIsNotNone(cause.get('traceback'))

            for arg in args:
                if arg == 'message':
                    self.assertIn('message', str(e))
                else:
                    self.assertEqual(arg, getattr(e, arg))

    def test_dispatch_no_such_handler(self):
        context = {'type': 'unknown_type'}
        self.assertRaises(exceptions.NonRecoverableError,
                          dispatch.dispatch, context)

    def test_user_exception_causese(self):
        message = 'TEST_MESSAGE'
        op_handler = self._operation(func7, task_target='stub', args=[message])
        try:
            op_handler.dispatch_to_subprocess()
            self.fail()
        except exceptions.NonRecoverableError as e:
            self.assertEqual(2, len(e.causes))
            initial_cause = e.causes[0]
            self.assertEqual(initial_cause['message'], message)
            self.assertEqual(initial_cause['type'], 'RuntimeError')

    @patch('cloudify.dispatch.sleep')
    @patch('cloudify.dispatch.amqp_client_utils')
    @patch('cloudify.dispatch.get_rest_client')
    @patch('cloudify.dispatch.WorkflowHandler._workflow_cancelled')
    @patch('cloudify.dispatch.update_execution_status',
           side_effect=[Exception('first loop'), Exception('second loop'),
                        InvalidExecutionUpdateStatus('test invalid update')])
    def test_workflow_starting_with_execution_cancelled(
            self, mock_update_execution_status, mock_workflow_cancelled,
            *args, **kwargs):
        workflow_handler = dispatch.WorkflowHandler(
            cloudify_context={'task_name': 'test'},
            args=(), kwargs={})
        _normal_func = workflow_handler._func
        _normal_ctx = workflow_handler._ctx
        workflow_handler._func = type('MockFunc', (object,), {
            'workflow_system_wide': True,
            '__call__': func2})
        workflow_handler._ctx = type('MockWorkflowContext', (object,), {
            'local': False,
            'logger': MagicMock(),
            'internal': MagicMock(),
            'execution_id': 'test_execution_id',
            'workflow_id': 'test_workflow_id',
            'dry_run': False,
            'resume': False
        })
        workflow_handler._ctx._context = {'tenant': {'name': 'yes'}}
        workflow_handler._ctx.tenant_name = 'yes'

        try:
            workflow_handler.handle()
            mock_update_execution_status.assert_called_with(
                'test_execution_id', 'started', None)
            mock_workflow_cancelled.assert_called_with()
            self.assertEqual(3, mock_update_execution_status.call_count)
        finally:
            workflow_handler._func = _normal_func
            workflow_handler._ctx = _normal_ctx

    @patch('cloudify.dispatch.sleep')
    @patch('cloudify.dispatch.amqp_client_utils')
    @patch('cloudify.dispatch.get_rest_client')
    @patch('cloudify.dispatch.WorkflowHandler._workflow_cancelled')
    @patch('cloudify.dispatch.update_execution_status',
           side_effect=[Exception('first loop'), Exception('second loop'),
                        InvalidExecutionUpdateStatus('test invalid update')])
    def test_workflow_starting_without_masked_tenant(
            self, mock_update_execution_status, mock_workflow_cancelled,
            mock_rest_client, *args, **kwargs):
        workflow_handler = dispatch.WorkflowHandler(
            cloudify_context={'task_name': 'test'},
            args=(), kwargs={})
        _normal_func = workflow_handler._func
        _normal_ctx = workflow_handler._ctx
        workflow_handler._func = type('MockFunc', (object,), {
            'workflow_system_wide': True,
            '__call__': func2})
        workflow_handler._ctx = type('MockWorkflowContext', (object,), {
            'local': False,
            'logger': MagicMock(),
            'internal': MagicMock(),
            'execution_id': 'test_execution_id',
            'workflow_id': 'test_workflow_id',
            'dry_run': False,
            'resume': False
        })
        workflow_handler._ctx._context = {'tenant': {'name': 'yes'}}
        workflow_handler._ctx.tenant_name = 'yes'

        try:
            workflow_handler.handle()
            mock_rest_client.assert_called_once_with(tenant='yes')
        finally:
            workflow_handler._func = _normal_func
            workflow_handler._ctx = _normal_ctx

    @patch('cloudify.dispatch.sleep')
    @patch('cloudify.dispatch.amqp_client_utils')
    @patch('cloudify.dispatch.get_rest_client')
    @patch('cloudify.dispatch.WorkflowHandler._workflow_cancelled')
    @patch('cloudify.dispatch.update_execution_status',
           side_effect=[Exception('first loop'), Exception('second loop'),
                        InvalidExecutionUpdateStatus('test invalid update')])
    def test_workflow_starting_with_masked_tenant(
            self, mock_update_execution_status, mock_workflow_cancelled,
            mock_rest_client, *args, **kwargs):
        workflow_handler = dispatch.WorkflowHandler(
            cloudify_context={'task_name': 'test'},
            args=(), kwargs={})
        _normal_func = workflow_handler._func
        _normal_ctx = workflow_handler._ctx
        workflow_handler._func = type('MockFunc', (object,), {
            'workflow_system_wide': True,
            '__call__': func2})
        workflow_handler._ctx = type('MockWorkflowContext', (object,), {
            'local': False,
            'logger': MagicMock(),
            'internal': MagicMock(),
            'execution_id': 'test_execution_id',
            'workflow_id': 'test_workflow_id',
            'dry_run': False,
            'resume': False
        })
        workflow_handler._ctx._context = {'tenant': {
            'name': 'yes',
            'original_name': 'masquerade'}}
        workflow_handler._ctx.tenant_name = 'yes'

        try:
            workflow_handler.handle()
            mock_rest_client.assert_called_once_with(tenant='masquerade')
        finally:
            workflow_handler._func = _normal_func
            workflow_handler._ctx = _normal_ctx

    @patch('cloudify.dispatch.sleep')
    @patch('cloudify.dispatch.amqp_client_utils')
    @patch('cloudify.dispatch.get_rest_client')
    @patch('cloudify.dispatch.update_execution_status')
    def test_workflow_update_execution_status_set_to_false(
            self, mock_update_execution_status, *args, **kwargs):
        workflow_handler = dispatch.WorkflowHandler(
            cloudify_context={
                'task_name': 'test',
                'update_execution_status': False
            },
            args=(), kwargs={})
        _normal_func = workflow_handler._func
        _normal_ctx = workflow_handler._ctx
        workflow_handler._func = func2
        workflow_handler._ctx = type('MockWorkflowContext', (object,), {
            'local': False,
            'logger': MagicMock(),
            'internal': MagicMock(),
            'execution_id': 'test_execution_id',
            'workflow_id': 'test_workflow_id',
            'dry_run': False,
            'resume': False
        })

        # this is for making the implementation go the "local" way despite
        # ctx.local == false as execution status is only updated in remote
        # mode.
        workflow_handler._handle_remote_workflow = \
            workflow_handler._handle_local_workflow

        try:
            workflow_handler.handle()
            self.assertEqual(0, mock_update_execution_status.call_count)
        finally:
            workflow_handler._func = _normal_func
            workflow_handler._ctx = _normal_ctx

    def _operation(
            self,
            func,
            task_target=None,
            args=None,
            kwargs=None,
            execution_env=None,
            socket_url=None,
            deployment_id=None,
            local=True,
            process_registry=None):
        module = __name__
        if not local:
            module = module.split('.')[-1]
        os.environ['AGENT_LOG_DIR'] = self.temp_log_dir
        execution_env = execution_env or {}
        execution_env['PYTHONPATH'] = os.path.dirname(__file__)
        return dispatch.OperationHandler(cloudify_context={
            'no_ctx_kwarg': True,
            'local': local,
            'task_id': 'test',
            'task_name': '{0}.{1}'.format(module, func.__name__),
            'task_target': task_target,
            'type': 'operation',
            'execution_env': execution_env,
            'socket_url': socket_url,
            'deployment_id': deployment_id,
            'tenant': {'name': 'default_tenant'}
        }, args=args or [], kwargs=kwargs or {},
            process_registry=process_registry)


if os.environ.get('CLOUDIFY_DISPATCH'):
    amqp_client.create_client = Mock()
    amqp_client_utils.init_events_publisher = Mock()
    amqp_client_utils.close_amqp_client = Mock()


def func1(result):
    return result


def func2(*args, **kwargs):
    return args, kwargs


def func3(keys):
    return [os.environ.get(key) for key in keys]


def func4(message):
    logger = logging.getLogger(__name__)
    logger.info(message)


def func5(message):
    non_json_serializable_thingy = object()
    logger = logging.getLogger()
    handler = logger.handlers[0]
    handler._context = non_json_serializable_thingy
    logger.info(message)


def func6(args, known_exception=None, user_exception=None):
    if user_exception:
        raise globals()[user_exception](*args)
    else:
        raise getattr(exceptions, known_exception)(*args)


def func7(message):
    try:
        raise RuntimeError(message)
    except RuntimeError:
        _, ex, tb = sys.exc_info()
        raise NonRecoverableUserException(causes=[
            utils.exception_to_error_cause(ex, tb)
        ])


def func8(message):
    raise RuntimeError(message)


class UserException(Exception):
    pass


class RecoverableUserException(exceptions.RecoverableError):
    pass


class NonRecoverableUserException(exceptions.NonRecoverableError):
    pass
