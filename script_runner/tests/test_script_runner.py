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
import tempfile
import os
import sys
from StringIO import StringIO

import requests
from nose.tools import nottest, istest

from cloudify.decorators import workflow
from cloudify.workflows import local
from cloudify.workflows import ctx as workflow_ctx
from cloudify.mocks import MockCloudifyContext
from cloudify.exceptions import NonRecoverableError

from script_runner import tasks, ctx_proxy
from script_runner.ctx_proxy import (UnixCtxProxy,
                                     TCPCtxProxy,
                                     HTTPCtxProxy,
                                     StubCtxProxy,
                                     client_req)

IS_WINDOWS = os.name == 'nt'


@nottest
class TestCtxProxy(unittest.TestCase):

    class StubAttribute(object):
        some_property = 'some_value'

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
        self.ctx = MockCloudifyContext(node_id='instance_id', properties={
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
                'key': 'value'
            }
        })
        self.ctx.stub_method = self.stub_method
        self.ctx.stub_sleep = self.stub_sleep
        self.ctx.stub_args = self.stub_args
        self.ctx.stub_attr = self.StubAttribute()
        self.server = self.proxy_server_class(self.ctx)
        self.start_server()

    def start_server(self):
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

    def stop_server_now(self):
        self.stop_server = True
        while not self.server_stopped:
            time.sleep(0.1)

    def tearDown(self):
        self.stop_server_now()

    def request(self, *args):
        return client_req(self.server.socket_url, args)

    def test_attribute_access(self):
        response = self.request('stub_attr', 'some_property')
        self.assertEqual(response, 'some_value')

    def test_sugared_attribute_access(self):
        response = self.request('stub-attr', 'some-property')
        self.assertEqual(response, 'some_value')

    def test_dict_prop_access_get_key(self):
        response = self.request('node', 'properties', 'prop1')
        self.assertEqual(response, 'value1')

    def test_dict_prop_access_get_key_nested(self):
        response = self.request('node', 'properties', 'prop2.nested_prop1')
        self.assertEqual(response, 'nested_value1')

    def test_dict_prop_access_get_with_list_index(self):
        response = self.request('node', 'properties', 'prop3[2].value')
        self.assertEqual(response, 'value_2')

    def test_dict_prop_access_set(self):
        self.request('node', 'properties', 'prop4.key', 'new_value')
        self.request('node', 'properties', 'prop3[2].value', 'new_value_2')
        self.request('node', 'properties', 'prop4.some.new.path',
                     'some_new_value')
        self.assertEqual(self.ctx.node.properties['prop4']['key'], 'new_value')
        self.assertEqual(
            self.ctx.node.properties['prop3'][2]['value'],
            'new_value_2')
        self.assertEqual(
            self.ctx.node.properties['prop4']['some']['new']['path'],
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
        response = self.request('blueprint', 'id')
        self.assertIsNone(response)

    def test_client_request_timeout(self):
        if hasattr(self, 'expected_exception'):
            expected_exception = self.expected_exception
        else:
            expected_exception = RuntimeError
        self.assertRaises(expected_exception,
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
        if IS_WINDOWS:
            raise unittest.SkipTest('Test skipped on windows')
        self.proxy_server_class = UnixCtxProxy
        super(TestUnixCtxProxy, self).setUp()


@istest
class TestTCPCtxProxy(TestCtxProxy):

    def setUp(self):
        self.proxy_server_class = TCPCtxProxy
        super(TestTCPCtxProxy, self).setUp()


@istest
class TestHTTPCtxProxy(TestCtxProxy):

    def setUp(self):
        self.proxy_server_class = HTTPCtxProxy
        super(TestHTTPCtxProxy, self).setUp()

    def start_server(self):
        pass

    def stop_server_now(self):
        self.server.close()

    def test_client_request_timeout(self):
        self.expected_exception = requests.Timeout
        super(TestHTTPCtxProxy, self).test_client_request_timeout()


@nottest
class TestScriptRunner(unittest.TestCase):

    def _create_script(self, linux_script, windows_script,
                       windows_suffix='.bat', linux_suffix=''):
        suffix = windows_suffix if IS_WINDOWS else linux_suffix
        script = windows_script if IS_WINDOWS else linux_script
        script_path = tempfile.mktemp(suffix=suffix)
        with open(script_path, 'w') as f:
            f.write(script)
        return script_path

    def _run(self, script_path,
             process=None,
             workflow_name='execute_operation',
             parameters=None):

        process = process or {}
        process.update({
            'ctx_proxy_type': self.ctx_proxy_type
        })

        inputs = {
            'script_path': script_path,
            'process': process
        }
        blueprint_path = os.path.join(os.path.dirname(__file__),
                                      'blueprint', 'blueprint.yaml')
        self.env = local.init_env(blueprint_path,
                                  name=self._testMethodName,
                                  inputs=inputs)
        result = self.env.execute(workflow_name,
                                  parameters=parameters,
                                  task_retries=0)
        if not result:
            result = self.env.storage.get_node_instances()[0][
                'runtime_properties']
        return result

    def test_script_path_parameter(self):
        script_path = self._create_script(
            linux_script='''#! /bin/bash -e
            ctx instance runtime-properties map.key value
            ''',
            windows_script='''
            ctx instance runtime-properties map.key value
            ''')
        props = self._run(script_path=script_path)
        self.assertEqual(props['map']['key'], 'value')

    def test_return_value(self):
        script_path = self._create_script(
            linux_script='''#! /bin/bash -e
            ctx returns '@["1", 2, true]'
            ''',
            windows_script='''
            ctx returns "@[""1"", 2, true]"
            ''')
        result = self._run(script_path=script_path)
        self.assertEqual(result, ['1', 2, True])

    def test_process_env(self):
        script_path = self._create_script(
            linux_script='''#! /bin/bash -e
            ctx instance runtime-properties map.key1 $key1
            ctx instance runtime-properties map.key2 $key2
            ''',
            windows_script='''
            ctx instance runtime-properties map.key1 %key1%
            ctx instance runtime-properties map.key2 %key2%
            ''')
        props = self._run(
            script_path=script_path,
            process={
                'env': {
                    'key1': 'value1',
                    'key2': 'value2'
                }
            })
        p_map = props['map']
        self.assertEqual(p_map['key1'], 'value1')
        self.assertEqual(p_map['key2'], 'value2')

    def test_process_cwd(self):
        script_path = self._create_script(
            linux_script='''#! /bin/bash -e
            ctx instance runtime-properties map.cwd $PWD
            ''',
            windows_script='''
            ctx instance runtime-properties map.cwd %CD%
            ''')
        tmpdir = tempfile.gettempdir()
        props = self._run(
            script_path=script_path,
            process={
                'cwd': tmpdir
            })
        p_map = props['map']
        self.assertEqual(p_map['cwd'], tmpdir)

    def test_process_command_prefix(self):
        script_path = self._create_script(
            linux_script='''
import subprocess
subprocess.check_output(
    'ctx instance runtime-properties map.key value'.split(' '))
            ''',
            windows_script='''
            ctx instance runtime-properties map.key $env:TEST_KEY
            ''',
            windows_suffix='.ps1')
        if IS_WINDOWS:
            command_prefix = 'powershell'
        else:
            command_prefix = 'python'

        props = self._run(
            script_path=script_path,
            process={
                'env': {'TEST_KEY': 'value'},
                'command_prefix': command_prefix
            })
        p_map = props['map']
        self.assertEqual(p_map['key'], 'value')

    def test_process_args(self):
        script_path = self._create_script(
            linux_script='''#! /bin/bash -e
            ctx instance runtime-properties map.arg1 "$1"
            ctx instance runtime-properties map.arg2 $2
            ''',
            windows_script='''
            ctx instance runtime-properties map.arg1 %1
            ctx instance runtime-properties map.arg2 %2
            ''')
        props = self._run(
            script_path=script_path,
            process={
                'args': ['"arg with spaces"', 'arg2']
            })
        self.assertEqual('arg with spaces', props['map']['arg1'])
        self.assertEqual('arg2', props['map']['arg2'])

    def test_no_script_path(self):
        self.assertRaises(NonRecoverableError,
                          self._run, script_path=None)

    def test_script_error(self):
        script_path = self._create_script(
            linux_script='''#! /bin/bash -e
            echo 123123
            command_that_does_not_exist
            ''',
            windows_script='''
            @echo off
            echo 123123
            command_that_does_not_exist
            ''')
        try:
            self._run(script_path=script_path)
            self.fail()
        except tasks.ProcessException, e:
            expected_exit_code = 1 if IS_WINDOWS else 127
            if IS_WINDOWS:
                expected_stderr = "'command_that_does_not_exist' is not " \
                                  "recognized as an internal or external " \
                                  "command,\r\noperable program or batch " \
                                  "file."
            else:
                expected_stderr = ': line 3: ' \
                                  'command_that_does_not_exist: command ' \
                                  'not found'

            self.assertIn(os.path.basename(script_path), e.command)
            self.assertEqual(e.exit_code, expected_exit_code)
            self.assertEqual(e.stdout.strip(), '123123')
            self.assertIn(expected_stderr, e.stderr.strip())

    def test_script_error_from_bad_ctx_request(self):
        script_path = self._create_script(
            linux_script='''#! /bin/bash -e
            ctx property_that_does_not_exist
            ''',
            windows_script='''
            ctx property_that_does_not_exist
            ''')
        try:
            self._run(script_path=script_path)
            self.fail()
        except tasks.ProcessException, e:
            self.assertIn(os.path.basename(script_path), e.command)
            self.assertEqual(e.exit_code, 1)
            self.assertIn('RequestError', e.stderr)
            self.assertIn('property_that_does_not_exist', e.stderr)

    def test_python_script(self):
        script = '''
if __name__ == '__main__':
    from cloudify import ctx
    ctx.instance.runtime_properties['key'] = 'value'
'''
        suffix = '.py'
        script_path = self._create_script(
            linux_script=script,
            windows_script=script,
            linux_suffix=suffix,
            windows_suffix=suffix)
        props = self._run(script_path=script_path)
        self.assertEqual(props['key'], 'value')

    def test_execute_workflow(self):
        result = self._run(script_path=None,  # overridden by workflow
                           workflow_name='workflow_script',
                           parameters={'key': 'value'})
        self.assertEqual(result, 'value')


@istest
class TestScriptRunnerUnixCtxProxy(TestScriptRunner):

    def setUp(self):
        if IS_WINDOWS:
            raise unittest.SkipTest('Test skipped on windows')
        self.ctx_proxy_type = 'unix'
        super(TestScriptRunner, self).setUp()


@istest
class TestScriptRunnerTCPCtxProxy(TestScriptRunner):

    def setUp(self):
        self.ctx_proxy_type = 'tcp'
        super(TestScriptRunner, self).setUp()


@istest
class TestScriptRunnerHTTPCtxProxy(TestScriptRunner):

    def setUp(self):
        self.ctx_proxy_type = 'http'
        super(TestScriptRunner, self).setUp()


class TestCtxProxyType(unittest.TestCase):

    def test_http_ctx_type(self):
        self.assert_valid_ctx_proxy('http', HTTPCtxProxy)

    def test_tcp_ctx_type(self):
        self.assert_valid_ctx_proxy('tcp', TCPCtxProxy)

    def test_unix_ctx_type(self):
        if IS_WINDOWS:
            raise unittest.SkipTest('Skipped on windows')
        self.assert_valid_ctx_proxy('unix', UnixCtxProxy)

    def test_none_ctx_type(self):
        self.assert_valid_ctx_proxy('none', StubCtxProxy)

    def test_illegal_type(self):
        self.assertRaises(
            NonRecoverableError,
            self.assert_valid_ctx_proxy, 'doesnotexist', None)

    def test_explicit_auto_type(self):
        self._test_auto_type(explicit=True)

    def test_implicit_auto_type(self):
        self._test_auto_type(explicit=False)

    def _test_auto_type(self, explicit):
        if IS_WINDOWS:
            expected_type = TCPCtxProxy
        else:
            expected_type = UnixCtxProxy
        if explicit:
            ctx_proxy_type = 'auto'
        else:
            ctx_proxy_type = None
        self.assert_valid_ctx_proxy(ctx_proxy_type, expected_type)

    def assert_valid_ctx_proxy(self, ctx_proxy_type, expected_type):
        process = {}
        if ctx_proxy_type:
            process['ctx_proxy_type'] = ctx_proxy_type
        proxy = tasks.start_ctx_proxy(None, process)
        try:
            self.assertEqual(type(proxy), expected_type)
        finally:
            proxy.close()


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


class TestEvalPythonConfiguration(unittest.TestCase):

    def setUp(self):
        self.original_eval_script = tasks.eval_script
        self.original_execute = tasks.execute
        self.original_os_chmod = os.chmod
        self.addCleanup(self.cleanup)

        def eval_script(script_path, ctx, process):
            if self.expected_call != 'eval':
                self.fail()

        def execute(script_path, ctx, process):
            if self.expected_call != 'execute':
                self.fail()

        tasks.eval_script = eval_script
        tasks.execute = execute
        os.chmod = lambda p, m: None

    def cleanup(self):
        tasks.eval_script = self.original_eval_script
        tasks.execute = self.original_execute
        os.chmod = self.original_os_chmod

    def mock_ctx(self, **kwargs):
        ctx = MockCloudifyContext(**kwargs)
        ctx.download_resource = lambda s_path: s_path
        return ctx

    def test_explicit_eval_without_py_extenstion(self):
        self.expected_call = 'eval'
        tasks.run('script_path',
                  process={'eval_python': True},
                  ctx=self.mock_ctx())

    def test_explicit_eval_with_py_extenstion(self):
        self.expected_call = 'eval'
        tasks.run('script_path.py',
                  process={'eval_python': True},
                  ctx=self.mock_ctx())

    def test_implicit_eval(self):
        self.expected_call = 'eval'
        tasks.run('script_path.py',
                  ctx=self.mock_ctx())

    def test_explicit_execute_without_py_extension(self):
        self.expected_call = 'execute'
        tasks.run('script_path',
                  process={'eval_python': False},
                  ctx=self.mock_ctx())

    def test_explicit_execute_with_py_extension(self):
        self.expected_call = 'execute'
        tasks.run('script_path.py',
                  process={'eval_python': False},
                  ctx=self.mock_ctx())

    def test_implicit_execute(self):
        self.expected_call = 'execute'
        tasks.run('script_path',
                  ctx=self.mock_ctx())


@workflow
def execute_operation(**_):
    node = next(workflow_ctx.nodes)
    instance = next(node.instances)
    return instance.execute_operation('test.run').get()
