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

import unittest
import threading
import time
import logging
import tempfile
import os
import sys
from StringIO import StringIO

from nose.tools import nottest, istest

from cloudify.context import CloudifyContext
from cloudify.exceptions import NonRecoverableError

from script_runner import tasks, ctx_proxy
from script_runner.ctx_proxy import (UnixCtxProxy,
                                     TCPCtxProxy,
                                     client_req)


def base_ctx():
    return {
        '__cloudify_context': '0.3',
        'task_id': '@task_id',
        'task_name': '@task_name',
        'task_target': '@task_target',
        'blueprint_id': '@blueprint_id',
        'deployment_id': '@deployment_id',
        'execution_id': '@execution_id',
        'workflow_id': '@workflow_id',
        'node_id': '@node_id',
        'node_name': '@node_name',
        'node_properties': {
            'prop1': 'value1',
            'prop2': {
                'nested_prop1': 'nested_value1'
            },
            'prop3': [
                {'index': 0, 'value': 'value_0'},
                {'index': 1, 'value': 'value_1'},
                {'index': 2, 'value': 'value_2'}
            ],
            'prop4': {
                # place holder because properties is read only and we can't
                # use runtime_properties here
                'key': 'value'
            }
        },
        'plugin': '@plugin',
        'operation': '@operation',
        'relationships': ['@rel1', '@rel2'],
        'related': {
            'node_id': '@rel_node_id',
            'node_properties': {

            }
        }
    }


@nottest
class TestCtxProxy(unittest.TestCase):

    @staticmethod
    def stub_method(*args):
        return args

    @staticmethod
    def stub_sleep(seconds):
        time.sleep(float(seconds))

    @staticmethod
    def stub_args(arg1, arg2, arg3='arg3', arg4='arg4', *args, **kwargs):
        return dict(
            arg1=arg1,
            arg2=arg2,
            arg3=arg3,
            arg4=arg4,
            args=args,
            kwargs=kwargs)

    def setUp(self):
        self.raw_ctx = base_ctx()
        self.ctx = CloudifyContext(self.raw_ctx)
        self.ctx.stub_method = self.stub_method
        self.ctx.stub_sleep = self.stub_sleep
        self.ctx.stub_args = self.stub_args
        self.server = self.proxy_server_class(self.ctx)
        self._start_server()

    def _start_server(self):
        self.stop_server = False
        self.server_stopped = False

        def serve():
            while not self.stop_server:
                self.server.poll_and_process(timeout=0.1)
            self.server.close()
            self.server_stopped = True
        self.server_thread = threading.Thread(target=serve)
        self.server_thread.daemon = True
        self.server_thread.start()

    def _stop_server(self):
        self.stop_server = True
        while not self.server_stopped:
            time.sleep(0.1)

    def tearDown(self):
        self._stop_server()

    def request(self, *args):
        return client_req(self.server.socket_url, args)

    def test_attribute_access(self):
        response = self.request('related', 'node_id')
        self.assertEqual(response, '@rel_node_id')

    def test_sugared_attribute_access(self):
        response = self.request('related', 'node-id')
        self.assertEqual(response, '@rel_node_id')

    def test_dict_prop_access_get_key(self):
        response = self.request('properties', 'prop1')
        self.assertEqual(response, 'value1')

    def test_dict_prop_access_get_key_nested(self):
        response = self.request('properties', 'prop2.nested_prop1')
        self.assertEqual(response, 'nested_value1')

    def test_dict_prop_access_get_with_list_index(self):
        response = self.request('properties', 'prop3[2].value')
        self.assertEqual(response, 'value_2')

    def test_dict_prop_access_set(self):
        self.request('properties', 'prop4.key', 'new_value')
        self.request('properties', 'prop3[2].value', 'new_value_2')
        self.request('properties', 'prop4.some.new.path', 'some_new_value')
        self.assertEqual(self.ctx.properties['prop4']['key'], 'new_value')
        self.assertEqual(
            self.ctx.properties['prop3'][2]['value'],
            'new_value_2')
        self.assertEqual(
            self.ctx.properties['prop4']['some']['new']['path'],
            'some_new_value')

    def test_method_invocation(self):
        args = ['arg1', 'arg2', 'arg3']
        response_args = self.request('stub-method', *args)
        self.assertEqual(args, response_args)

    def test_method_invocation_no_args(self):
        response = self.request('stub-method')
        self.assertEqual([], response)

    def test_method_invocation_kwargs(self):
        arg1 = 'arg1'
        arg2 = 'arg2'
        arg4 = 'arg4_override'
        arg5 = 'arg5'
        kwargs = dict(
            arg4=arg4,
            arg5=arg5)
        response = self.request('stub_args', arg1, arg2, kwargs)
        self.assertDictEqual(response, dict(
            arg1=arg1,
            arg2=arg2,
            arg3='arg3',
            arg4=arg4,
            args=[],
            kwargs=dict(
                arg5=arg5)))

    def test_empty_return_value(self):
        response = self.request('related', 'properties')
        self.assertEqual(response, {})

    def test_client_request_timeout(self):
        self.assertRaises(RuntimeError,
                          client_req,
                          self.server.socket_url,
                          ['stub-sleep', '0.5'],
                          0.1)

    def test_processing_exception(self):
        self.assertRaises(ctx_proxy.RequestError,
                          self.request, 'property_that_does_not_exist')

    def test_not_json_serializable(self):
        self.assertRaises(ctx_proxy.RequestError,
                          self.request, 'logger')

    def test_no_string_arg(self):
        args = ['stub_method', 1, 2]
        response = self.request(*args)
        self.assertEqual(args[1:], response)



@istest
class TestUnixCtxProxy(TestCtxProxy):

    def setUp(self):
        self.proxy_server_class = UnixCtxProxy
        super(TestUnixCtxProxy, self).setUp()


@istest
class TestTCPCtxProxy(TestCtxProxy):

    def setUp(self):
        self.proxy_server_class = TCPCtxProxy
        super(TestTCPCtxProxy, self).setUp()


class TestScriptRunner(unittest.TestCase):

    def _create_script(self, script):
        script_path = tempfile.mktemp()
        with open(script_path, 'w') as f:
            f.write(script)
        return script_path

    def _run(self, updated, expected_script_path, actual_script_path):
        def mock_download_resource(script_path):
            self.assertEqual(script_path, expected_script_path)
            return actual_script_path
        raw_ctx = base_ctx()
        raw_ctx.update(updated)
        CloudifyContext.logger = logging.getLogger()
        ctx = CloudifyContext(raw_ctx)
        ctx.download_resource = mock_download_resource
        tasks.run(ctx)
        return ctx

    def test_script_path(self):
        actual_script_path = self._create_script(
            '''#! /bin/bash -e
            ctx properties map.key value
            ''')
        expected_script_path = 'expected_script_path'
        ctx = self._run(
            updated={
                'node_properties': {
                    'map': {},
                    'script_path': expected_script_path
                }
            },
            expected_script_path=expected_script_path,
            actual_script_path=actual_script_path
        )
        self.assertEqual(ctx.properties['map']['key'], 'value')

    def test_operation_scripts(self):
        self._operation_scripts_impl('start', 'start')

    def test_operation_scripts_no_mapping(self):
        # This is the same as the test above only now we map start
        # while the current operation is actually not_start
        # so nothing should be executed, so no map.key should exist
        # thus KeyError (small workaround)
        self.assertRaises(KeyError,
                          self._operation_scripts_impl,
                          'not_start', 'start')

    def _operation_scripts_impl(self, operation, scripts_operation):
        actual_script_path = self._create_script(
            '''#! /bin/bash -e
            ctx properties map.key value
            ctx properties map.key2 '@{"inner_key": 100}'
            ''')
        expected_script_path = 'expected_script_path'
        ctx = self._run(
            updated={
                'operation': operation,
                'node_properties': {
                    'map': {},
                    'scripts': {scripts_operation: expected_script_path}
                }
            },
            expected_script_path=expected_script_path,
            actual_script_path=actual_script_path
        )
        self.assertEqual(ctx.properties['map']['key'], 'value')
        self.assertDictEqual(ctx.properties['map']['key2'],
                             {'inner_key': 100})

    def test_no_script_path(self):
        self.assertRaises(NonRecoverableError,
                          self._run,
                          updated={},
                          expected_script_path=None,
                          actual_script_path=None)

    def test_script_error(self):
        actual_script_path = self._create_script(
            '''#! /bin/bash -e
            echo 123123
            command_that_does_not_exist
            ''')
        expected_script_path = 'expected_script_path'
        try:
            self._run(
                updated={
                    'node_properties': {
                        'script_path': expected_script_path
                    }
                },
                expected_script_path=expected_script_path,
                actual_script_path=actual_script_path
            )
            self.fail()
        except tasks.ProcessException, e:
            self.assertEqual(e.command, actual_script_path)
            self.assertEqual(e.exit_code, 127)
            self.assertEqual(e.stdout.strip(), '123123')
            expected_error = '{}: line 3: ' \
                             'command_that_does_not_exist: command not found' \
                             .format(actual_script_path)
            self.assertEqual(e.stderr.strip(), expected_error)

    def test_script_error_from_bad_ctx_request(self):
        actual_script_path = self._create_script(
            '''#! /bin/bash -e
            ctx property_that_does_not_exist
            ''')
        expected_script_path = 'expected_script_path'
        try:
            self._run(
                updated={
                    'node_properties': {
                        'script_path': expected_script_path
                    }
                },
                expected_script_path=expected_script_path,
                actual_script_path=actual_script_path
            )
            self.fail()
        except tasks.ProcessException, e:
            self.assertEqual(e.command, actual_script_path)
            self.assertEqual(e.exit_code, 1)
            self.assertIn('RequestError', e.stderr)
            self.assertIn('property_that_does_not_exist', e.stderr)

    def test_ruby_ctx(self):
        actual_script_path = self._create_script(
            '''#! /bin/ruby
            load '/home/dan/work/ruby-ctx/ctx.rb'
            ''')
        ctx = self._run(
            updated={
                'node_properties': {
                    'map': {'list': [1, 2, 3, 4]},
                    'script_path': 'expected_script_path'
                }
            },
            expected_script_path='expected_script_path',
            actual_script_path=actual_script_path
        )
        print ctx.properties['map']['new_key']


class TestArgumentParsing(unittest.TestCase):

    def mock_client_req(self, socket_url, args, timeout):
        self.assertEqual(socket_url, self.expected.get('socket_url'))
        self.assertEqual(args, self.expected.get('args'))
        self.assertEqual(timeout, int(self.expected.get('timeout')))
        return self.mock_response

    def setUp(self):
        self.original_client_req = ctx_proxy.client_req
        ctx_proxy.client_req = self.mock_client_req
        self.addCleanup(self.restore)
        self.expected = dict(
            args=[],
            timeout=5,
            socket_url='stub')
        self.mock_response = None
        os.environ['CTX_SOCKET_URL'] = 'stub'

    def restore(self):
        ctx_proxy.client_req = self.original_client_req
        if 'CTX_SOCKET_URL' in os.environ:
            del os.environ['CTX_SOCKET_URL']

    def test_socket_url_arg(self):
        self.expected.update(dict(
            socket_url='sock_url'))
        ctx_proxy.main(['--socket-url', self.expected.get('socket_url')])

    def test_socket_url_env(self):
        expected_socket_url = 'env_sock_url'
        os.environ['CTX_SOCKET_URL'] = expected_socket_url
        self.expected.update(dict(
            socket_url=expected_socket_url))
        ctx_proxy.main([])

    def test_socket_url_missing(self):
        del os.environ['CTX_SOCKET_URL']
        self.assertRaises(RuntimeError,
                          ctx_proxy.main, [])

    def test_args(self):
        self.expected.update(dict(
            args=['1', '2', '3']))
        ctx_proxy.main(self.expected.get('args'))

    def test_timeout(self):
        self.expected.update(dict(
            timeout='10'))
        ctx_proxy.main(['--timeout', self.expected.get('timeout')])
        self.expected.update(dict(
            timeout='15'))
        ctx_proxy.main(['-t', self.expected.get('timeout')])

    def test_mixed_order(self):
        self.expected.update(dict(
            args=['1', '2', '3'],
            timeout='20',
            socket_url='mixed_socket_url'))
        ctx_proxy.main(
            ['-t', self.expected.get('timeout')] +
            ['--socket-url', self.expected.get('socket_url')] +
            self.expected.get('args'))
        ctx_proxy.main(
            ['-t', self.expected.get('timeout')] +
            self.expected.get('args') +
            ['--socket-url', self.expected.get('socket_url')])
        ctx_proxy.main(
            self.expected.get('args') +
            ['-t', self.expected.get('timeout')] +
            ['--socket-url', self.expected.get('socket_url')])

    def test_json_args(self):
        args = ['@1', '@[1,2,3]', '@{"key":"value"}']
        expected_args = [1, [1, 2, 3], {'key': 'value'}]
        self.expected.update(dict(
            args=expected_args))
        ctx_proxy.main(args)

    def test_json_arg_prefix(self):
        args = ['_1', '@1']
        expected_args = [1, '@1']
        self.expected.update(dict(
            args=expected_args))
        ctx_proxy.main(args + ['--json-arg-prefix', '_'])

    def test_json_output(self):
        self.assert_valid_output('string', 'string', '"string"')
        self.assert_valid_output(1, '1', '1')
        self.assert_valid_output([1, '2'], "[1, '2']", '[1, "2"]')
        self.assert_valid_output({'key': 1},
                                 "{'key': 1}",
                                 '{"key": 1}')
        self.assert_valid_output(False, '', 'false')
        self.assert_valid_output(True, 'True', 'true')
        self.assert_valid_output([], '', '[]')
        self.assert_valid_output({}, '', '{}')

    def assert_valid_output(self, response, ex_typed_output, ex_json_output):
        self.mock_response = response
        current_stdout = sys.stdout

        def run(args, expected):
            output = StringIO()
            sys.stdout = output
            ctx_proxy.main(args)
            self.assertEqual(output.getvalue(), expected)

        try:
            run([], ex_typed_output)
            run(['-j'], ex_json_output)
            run(['--json-output'], ex_json_output)
        finally:
            sys.stdout = current_stdout
