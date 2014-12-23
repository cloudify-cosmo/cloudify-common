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
import tempfile
import os
from collections import namedtuple

import requests
import testtools
from nose.tools import nottest, istest

from cloudify.decorators import workflow
from cloudify.workflows import local
from cloudify.workflows import ctx as workflow_ctx
from cloudify.mocks import MockCloudifyContext
from cloudify.exceptions import NonRecoverableError
from cloudify.proxy.server import (UnixCtxProxy,
                                   TCPCtxProxy,
                                   HTTPCtxProxy,
                                   StubCtxProxy)

from script_runner import tasks

IS_WINDOWS = os.name == 'nt'


@nottest
class TestScriptRunner(testtools.TestCase):

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
subprocess.Popen(
    'ctx instance runtime-properties map.key value'.split(' ')).communicate()[0]  # NOQA
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


class TestCtxProxyType(testtools.TestCase):

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


class TestEvalPythonConfiguration(testtools.TestCase):

    def setUp(self):
        super(TestEvalPythonConfiguration, self).setUp()
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


class TestDownloadResource(testtools.TestCase):

    def setUp(self):
        super(TestDownloadResource, self).setUp()
        self.status_code = 200

    def _mock_requests_get(self, url):
        response = namedtuple('Response', 'text status_code')
        return response(url, self.status_code)

    def _test_url(self, url):
        script_path = url
        original_requests_get = requests.get
        try:
            requests.get = self._mock_requests_get
            result = tasks.download_resource(None, script_path)
            with open(result) as f:
                self.assertEqual(script_path, f.read())
            self.assertTrue(result.endswith('-some_script.py'))
        finally:
            requests.get = original_requests_get

    def test_http_url(self):
        self._test_url('http://localhost/some_script.py')

    def test_https_url(self):
        self._test_url('https://localhost/some_script.py')

    def test_url_status_code_404(self):
        self.status_code = 404
        try:
            self.test_http_url()
            self.fail()
        except NonRecoverableError, e:
            self.assertIn('status code: 404', str(e))

    def test_blueprint_resource(self):
        test_script_path = 'my_script.py'

        def mock_download_resource(script_path):
            self.assertEqual(script_path, test_script_path)
            return script_path
        result = tasks.download_resource(mock_download_resource,
                                         test_script_path)
        self.assertEqual(result, test_script_path)


@workflow
def execute_operation(**_):
    node = next(workflow_ctx.nodes)
    instance = next(node.instances)
    return instance.execute_operation('test.run').get()
