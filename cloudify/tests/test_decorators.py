########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

import sys
import warnings

import testtools

from mock import patch, Mock

from cloudify import ctx as ctx_proxy
from cloudify import context
from cloudify.state import current_ctx
from cloudify.decorators import (
    operation, workflow, serial_operation)
from cloudify.mocks import MockCloudifyContext
from cloudify.exceptions import (
    NonRecoverableError, CloudifySerializationRetry)

from cloudify.test_utils.dispatch_helper import run
import cloudify.tests.mocks.mock_rest_client as rest_client_mock


class MockNotPicklableException(Exception):
    """Non-picklable exception"""
    def __init__(self, custom_error):
        self.message = custom_error

    def __str__(self):
        return self.message


class MockPicklableException(Exception):
    """Non-picklable exception"""
    def __init__(self, custom_error):
        super(Exception, self).__init__(custom_error)


@operation
def acquire_context(*args, **kwargs):
    return run(acquire_context_impl, *args, **kwargs)


def acquire_context_impl(a, b, ctx, **kwargs):
    return ctx


@operation
def some_operation(**kwargs):
    return run(some_operation_impl, **kwargs)


def some_operation_impl(**kwargs):
    from cloudify import ctx
    return ctx


@operation
def run_op(**kwargs):
    run(assert_op_impl, **kwargs)


def assert_op_impl(ctx, test_case, **kwargs):
    test_case.assertEqual(ctx, ctx_proxy)


@patch('cloudify.manager.get_rest_client', rest_client_mock.MockRestclient)
class OperationTest(testtools.TestCase):
    def test_empty_ctx(self):
        ctx = acquire_context(0, 0)
        self.assertIsInstance(ctx, context.CloudifyContext)

    def test_provided_ctx(self):
        ctx = {'node_id': '1234'}
        kwargs = {'__cloudify_context': ctx}
        ctx = acquire_context(0, 0, **kwargs)
        self.assertIsInstance(ctx, context.CloudifyContext)
        self.assertEquals('1234', ctx.instance.id)

    def test_proxied_ctx(self):

        self.assertRaises(RuntimeError,
                          lambda: ctx_proxy.instance.id)

        run_op(test_case=self)

        self.assertRaises(RuntimeError,
                          lambda: ctx_proxy.instance.id)

    def test_provided_capabilities(self):
        ctx = {
            'node_id': '5678',
        }

        rest_client_mock.put_node_instance(
            '5678',
            relationships=[{'target_id': 'some_node',
                            'target_name': 'some_node'}])
        rest_client_mock.put_node_instance('some_node',
                                           runtime_properties={'k': 'v'})

        kwargs = {'__cloudify_context': ctx}
        ctx = acquire_context(0, 0, **kwargs)
        with warnings.catch_warnings(record=True) as warns:
            self.assertIn('k', ctx.capabilities)
            self.assertEquals('v', ctx.capabilities['k'])
        if sys.version_info < (2, 7):
            # i was unable to make this work on py2.6
            return
        self.assertEqual(len(warns), 2)
        for w in warns:
            self.assertIn('capabilities is deprecated', str(w))

    def test_capabilities_clash(self):
        ctx = {
            'node_id': '5678',
        }

        rest_client_mock.put_node_instance(
            '5678',
            relationships=[{'target_id': 'node1',
                            'target_name': 'node1'},
                           {'target_id': 'node2',
                            'target_name': 'node2'}])

        rest_client_mock.put_node_instance('node1',
                                           runtime_properties={'k': 'v1'})
        rest_client_mock.put_node_instance('node2',
                                           runtime_properties={'k': 'v2'})

        kwargs = {'__cloudify_context': ctx}
        ctx = acquire_context(0, 0, **kwargs)
        with warnings.catch_warnings(record=True) as warns:
            self.assertRaises(
                NonRecoverableError, lambda: 'k' in ctx.capabilities)
        if sys.version_info < (2, 7):
            # i was unable to make this work on py2.6
            return
        self.assertEqual(len(warns), 1)
        self.assertIn('capabilities is deprecated', str(warns[0]))

    def test_instance_update(self):
        with patch.object(context.NodeInstanceContext,
                          'update') as mock_update:
            kwargs = {'__cloudify_context': {
                'node_id': '5678'
            }}
            some_operation(**kwargs)
            mock_update.assert_called_once_with()

    def test_source_target_update_in_relationship(self):
        with patch.object(context.NodeInstanceContext,
                          'update') as mock_update:
            kwargs = {'__cloudify_context': {
                'node_id': '5678',
                'relationships': ['1111'],
                'related': {
                    'node_id': '1111',
                    'is_target': True
                }
            }}
            some_operation(**kwargs)
            self.assertEqual(2, mock_update.call_count)

    def test_backwards(self):
        @operation
        def o1():
            return 'o1'

        @operation(some_unused_kwargs='value')
        def o2():
            return 'o2'

        @workflow
        def w1():
            return 'w1'

        @workflow(system_wide=True)
        def w2():
            return 'w2'

        self.assertEqual(o1(), 'o1')
        self.assertEqual(o2(), 'o2')
        self.assertEqual(w1(), 'w1')
        self.assertEqual(w2(), 'w2')

    @patch('cloudify.tests.mocks.mock_rest_client.'
           'MockNodeInstancesClient.list')
    def test_serial_operation_install(self, list_fn):

        _ctx = MockCloudifyContext(
            node_id='test_node',
            deployment_id='test_deployment',
            index=7
        )

        # Check that we raise if the preceder is not 'started'.
        _ctx._context['workflow_id'] = 'install'
        current_ctx.set(_ctx)
        _caller = Mock()

        @serial_operation()
        def run_op_install_only(ctx=_ctx, **kwargs):
            _caller()

        list_fn.return_value = [
            Mock(id='test_node{x}'.format(x=n),
                 index=n,
                 state='uninitialized') for n in range(0, 6)
        ]
        self.assertRaises(CloudifySerializationRetry,
                          run_op_install_only)
        self.assertFalse(_caller.called)
        list_fn.return_value = [
            Mock(id='test_node{x}'.format(x=n),
                 index=n,
                 state='started' if n < 7 else 'uninitialized')
            for n in range(3, 10) if n != 7
        ]
        run_op_install_only()
        self.assertTrue(_caller.called)
        current_ctx.clear()

    @patch('cloudify.tests.mocks.mock_rest_client.'
           'MockNodeInstancesClient.list')
    def test_serial_operation_install_wait_for_1(self, list_fn):

        _ctx = MockCloudifyContext(
            node_id='test_node',
            deployment_id='test_deployment',
            index=7
        )

        # Check that we raise if the preceder is not 'started'.
        _ctx._context['workflow_id'] = 'install'
        current_ctx.set(_ctx)
        _caller = Mock()

        @serial_operation(threshold=1)
        def run_op_wait_for_1(ctx=_ctx, **kwargs):
            _caller()

        list_fn.return_value = [
            Mock(id='test_node{x}'.format(x=n),
                 index=n,
                 state='started' if n == 6 else 'uninitialized')
            for n in range(0, 6)]
        run_op_wait_for_1()
        self.assertTrue(_caller.called)
        current_ctx.clear()

    @patch('cloudify.tests.mocks.mock_rest_client.'
           'MockNodeInstancesClient.list')
    def test_serial_operation_install_states(self, list_fn):

        _ctx = MockCloudifyContext(
            node_id='test_node',
            deployment_id='test_deployment',
            index=7
        )

        # Check that we raise if the preceder is not 'started'.
        _ctx._context['workflow_id'] = 'install'
        current_ctx.set(_ctx)
        _caller = Mock()

        @serial_operation(states=['uninitialized'])
        def run_op_uninitialized_states(ctx=_ctx, **kwargs):
            _caller()

        list_fn.return_value = [
            Mock(id='test_node{x}'.format(x=n),
                 index=n,
                 state='started' if n == 6 else 'uninitialized')
            for n in range(0, 6)]
        run_op_uninitialized_states()
        self.assertTrue(_caller.called)
        current_ctx.clear()

    @patch('cloudify.tests.mocks.mock_rest_client.'
           'MockNodeInstancesClient.list')
    def test_serial_operation_install_workflow(self, list_fn):

        _ctx = MockCloudifyContext(
            node_id='test_node',
            deployment_id='test_deployment',
            index=7
        )

        # Check that we raise if the preceder is not 'started'.
        _ctx._context['workflow_id'] = 'install'
        current_ctx.set(_ctx)
        _caller = Mock()

        @serial_operation(workflows=['scale'])
        def run_op_neither(ctx=_ctx, **kwargs):
            _caller()

        list_fn.return_value = [
            Mock(id='test_node{x}'.format(x=n),
                 index=n,
                 state='started' if n == 6 else 'uninitialized')
            for n in range(0, 6)]
        run_op_neither()
        self.assertTrue(_caller.called)
        current_ctx.clear()

    @patch('cloudify.tests.mocks.mock_rest_client.'
           'MockNodeInstancesClient.list')
    def test_serial_operation_uninstall(self, list_fn):

        _ctx = MockCloudifyContext(
            node_id='test_node',
            deployment_id='test_deployment',
            index=7
        )

        # Check that we raise if the preceder is not 'started'.
        _ctx._context['workflow_id'] = 'uninstall'
        current_ctx.set(_ctx)
        _caller = Mock()

        @serial_operation(workflows=['uninstall'])
        def run_op_uninstall_only(ctx=_ctx, **kwargs):
            _caller()

        list_fn.return_value = [
            Mock(id='test_node{x}'.format(x=n),
                 index=n,
                 state='uninitialized') for n in range(1, 8) if n != 7
        ]
        self.assertRaises(CloudifySerializationRetry,
                          run_op_uninstall_only)
        self.assertFalse(_caller.called)
        current_ctx.clear()

    @patch('cloudify.tests.mocks.mock_rest_client.'
           'MockNodeInstancesClient.list')
    def test_serial_operation_uninstall_wait_3(self, list_fn):

        _ctx = MockCloudifyContext(
            node_id='test_node',
            deployment_id='test_deployment',
            index=7
        )

        # Check that we raise if the preceder is not 'started'.
        _ctx._context['workflow_id'] = 'uninstall'
        current_ctx.set(_ctx)
        _caller = Mock()

        @serial_operation(threshold=3,
                          workflows=['uninstall'])
        def run_op_wait_for_3(ctx=_ctx, **kwargs):
            _caller()

        finished3 = []
        for n in range(1, 8):
            if n == 7:
                continue
            elif n in [1, 2, 3]:
                state = 'started'
            else:
                state = 'uninitialized'
            finished3.append(
                Mock(id='test_node{x}'.format(x=n),
                     index=n,
                     state=state))
        list_fn.return_value = finished3
        self.assertRaises(CloudifySerializationRetry,
                          run_op_wait_for_3)
        self.assertFalse(_caller.called)

        # Check if we cross the serialization_type threshold,
        # then we do not trigger the retry.
        list_fn.return_value = [
            Mock(id='test_node{x}'.format(x=n),
                 index=n,
                 state='started' if n < 7 else 'uninitialized')
            for n in range(3, 10) if n != 7
        ]
        run_op_wait_for_3()
        self.assertTrue(_caller.called)
        current_ctx.clear()
