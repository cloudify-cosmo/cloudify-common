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

from mock import patch, MagicMock, Mock
import testtools

from cloudify import dispatch
from cloudify import exceptions
from cloudify_rest_client.exceptions import InvalidExecutionUpdateStatus


class TestDispatchTaskHandler(testtools.TestCase):
    def test_dispatch_no_such_handler(self):
        context = {'type': 'unknown_type'}
        self.assertRaises(exceptions.NonRecoverableError,
                          dispatch.dispatch, context)

    @patch('cloudify.dispatch.get_rest_client')
    @patch('cloudify.dispatch.WorkflowHandler._workflow_cancelled')
    @patch('cloudify.dispatch.update_execution_status',
           side_effect=[InvalidExecutionUpdateStatus('started')])
    def test_workflow_starting_with_execution_cancelled(
            self, mock_update_execution_status, mock_workflow_cancelled,
            *args, **kwargs):
        """If the set-status-to-started call fails, execution is cancelled"""
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
        finally:
            workflow_handler._func = _normal_func
            workflow_handler._ctx = _normal_ctx

    @patch('cloudify.dispatch.get_rest_client')
    @patch('cloudify.dispatch.WorkflowHandler._workflow_cancelled')
    @patch('cloudify.dispatch.update_execution_status',
           side_effect=[InvalidExecutionUpdateStatus('started')])
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

    @patch('cloudify.dispatch.get_rest_client')
    @patch('cloudify.dispatch.WorkflowHandler._workflow_cancelled')
    @patch('cloudify.dispatch.update_execution_status',
           side_effect=[InvalidExecutionUpdateStatus('test invalid update')])
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
            'logger': Mock(),
            'cleanup': Mock(),
            'internal': Mock(),
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
        return dispatch.OperationHandler(cloudify_context={
            'no_ctx_kwarg': True,
            'local': local,
            'task_id': 'test',
            'task_name': '{0}.{1}'.format(module, func.__name__),
            'task_target': task_target,
            'type': 'operation',
            'socket_url': socket_url,
            'deployment_id': deployment_id,
            'tenant': {'name': 'default_tenant'}
        }, args=args or [], kwargs=kwargs or {},
            process_registry=process_registry)


def func1(result):
    return result


def func2(*args, **kwargs):
    return args, kwargs
