#########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

import os
import shutil
import tempfile
import unittest

import pytest

from . import string_in_log

import cloudify.ctx_wrappers
from cloudify.exceptions import NonRecoverableError
from cloudify.exceptions import OperationRetry
from cloudify.workflows import local
from script_runner.tasks import ProcessException, IS_WINDOWS

BLUEPRINT_DIR = os.path.join(os.path.dirname(__file__), 'wrapper_blueprint')


class PythonWrapperTests(unittest.TestCase):
    @pytest.fixture(autouse=True)
    def _inject_fixtures(self, caplog, tmpdir):
        self._caplog = caplog
        self.tempdir = str(tmpdir)

    def setUp(self):
        super(PythonWrapperTests, self).setUp()
        source = os.path.join(os.path.dirname(
            cloudify.ctx_wrappers.__file__), 'ctx-py.py')
        destination = os.path.join(self.tempdir, 'ctxwrapper.py')
        shutil.copy(source, destination)
        self.script_path = tempfile.mktemp()
        self.addCleanup(self.cleanup)

    def cleanup(self):
        try:
            os.remove(self.script_path)
        except Exception:
            pass

    def _prescript(self):
        return (
            '#!/usr/bin/env python\n'
            'from ctxwrapper import ctx\n'
        )

    def _create_script(self, script):
        script_path = tempfile.mktemp()
        with open(script_path, 'w') as f:
            f.write(self._prescript() + script)
        return script_path

    def _run(self, script,
             process=None,
             workflow_name='execute_operation',
             parameters=None,
             env_var='value',
             task_retries=0):

        self.script_path = self._create_script(script)
        process = process or {}
        env = process.setdefault('env', {})

        if 'PYTHONPATH' in env:
            env['PYTHONPATH'] += ':' + self.tempdir
        else:
            env['PYTHONPATH'] = self.tempdir

        if IS_WINDOWS:
            process['command_prefix'] = 'python'

        inputs = {
            'script_path': self.script_path,
            'process': process,
            'env_var': env_var
        }
        blueprint_path = os.path.join(BLUEPRINT_DIR, 'blueprint.yaml')
        self.env = local.init_env(blueprint_path,
                                  name=self._testMethodName,
                                  inputs=inputs)
        result = self.env.execute(workflow_name,
                                  parameters=parameters,
                                  task_retries=task_retries,
                                  task_retry_interval=0)
        if not result:
            result = self.env.storage.get_node_instances()[0][
                'runtime_properties']
        return result

    def test_direct_ctx_call(self):
        script = ('ctx("instance runtime-properties key value")')
        result = self._run(script)
        self.assertEqual(result['key'], 'value')

    def test_direct_bad_ctx_call(self):
        script = ('ctx("bad_call")')
        self.assertRaises(ProcessException, self._run, script)
        self.assertTrue(string_in_log(
            'RuntimeError: bad_call cannot be processed in',
            self._caplog))

    def test_direct_ctx_call_missing_property(self):
        script = ('ctx("node properties missing_node_property")')
        self.assertRaises(ProcessException, self._run, script)
        self.assertTrue(string_in_log(
            'illegal path: missing_node_property',
            self._caplog))

    def test_logger(self):
        script = ('ctx.logger.debug("debug_message")\n'
                  'ctx.logger.info("info_message")\n'
                  'ctx.logger.warn("warning_message")\n'
                  'ctx.logger.warning("warning_message")\n'
                  'ctx.logger.error("error_message")')
        expected_levels = ['DEBUG', 'INFO', 'WARNING', 'WARNING', 'ERROR']
        expected_msgs = [
            'debug_message',
            'info_message',
            'warning_message',
            'warning_message',
            'error_message'
        ]
        self._run(script)
        # skip messages that are unrelated to the test
        records = [r for r in self._caplog.records
                   if any(msg in r.msg for msg in expected_msgs)]
        for m in range(1, len(expected_levels)):
            self.assertEqual(
                '{0}: {1}'.format(
                    records[m].levelname,
                    records[m].msg),
                '{0}: {1}'.format(
                    expected_levels[m],
                    expected_msgs[m]))

    def test_get_node_properties(self):
        script = ('value = ctx.node.properties["key"]\n'
                  'ctx.returns(value)')
        result = self._run(script)
        self.assertEqual('value', result)

    def test_get_all_node_properties(self):
        script = ('value = ctx.node.properties.get_all()\n'
                  'ctx.returns(value)')
        res_dict = self._run(script)
        self.assertIn('ip', res_dict)
        self.assertEqual(res_dict['ip'], '1.1.1.1')
        self.assertIn('key', res_dict)
        self.assertEqual(res_dict['key'], 'value')

    def test_get_node_properties_get_function(self):
        script = ('value = ctx.node.properties.get("key", "b")\n'
                  'ctx.returns(value)')
        result = self._run(script)
        self.assertEqual('value', result)

    def test_get_node_properties_get_function_missing_key_no_default(self):
        script = ('value = ctx.node.properties.get("key1")\n'
                  'ctx.returns({"value": value})')
        result = self._run(script)
        self.assertEqual({"value": None}, result)

    def test_get_node_properties_get_function_missing_key_with_default(self):
        script = ('value = ctx.node.properties.get("key1", "b")\n'
                  'ctx.returns(value)')
        result = self._run(script)
        self.assertEqual('b', result)

    def test_get_node_id(self):
        script = ('value = ctx.node.id\n'
                  'ctx.returns(value)')
        result = self._run(script)
        self.assertEqual('test_node', result)

    def test_get_node_name(self):
        script = ('value = ctx.node.name\n'
                  'ctx.returns(value)')
        result = self._run(script)
        self.assertEqual('test_node', result)

    def test_get_node_type(self):
        script = ('value = ctx.node.type\n'
                  'ctx.returns(value)')
        result = self._run(script)
        self.assertEqual('cloudify.nodes.Compute', result)

    def test_get_instance_id(self):
        script = ('value = ctx.instance.id\n'
                  'ctx.returns(value)')
        result = self._run(script)
        self.assertIn('test_node_', result)

    def test_get_instance_relationships(self):
        script = ('value = ctx.instance.relationships\n'
                  'ctx.returns({"relationships": value})')
        result = self._run(script)
        self.assertEqual({"relationships": []}, result)

    def test_set_get_instance_runtime_properties(self):
        script = ('ctx.instance.runtime_properties["key"] = "value"\n'
                  'value = ctx.instance.runtime_properties["key"]\n'
                  'ctx.returns(value)')
        result = self._run(script)
        self.assertEqual('value', result)

    def test_get_instance_runtime_properties_non_string(self):
        script = ('value = ctx.instance.runtime_properties["key"] = 1\n'
                  'ctx.returns(value)')
        result = self._run(script)
        self.assertEqual(1, result)

    def test_get_instance_runtime_properties_missing_key_no_default(self):
        script = ('value = ctx.instance.runtime_properties.get("key1")\n'
                  'ctx.returns({"value": value})')
        result = self._run(script)
        self.assertEqual({'value': None}, result)

    def test_get_instance_runtime_properties_missing_key_with_default(self):
        script = ('value = ctx.instance.runtime_properties.get("key1", "b")\n'
                  'ctx.returns(value)')
        result = self._run(script)
        self.assertEqual('b', result)

    def test_get_instance_host_ip(self):
        script = ('value = ctx.instance.host_ip\n'
                  'ctx.returns(value)')
        result = self._run(script)
        self.assertEqual(result, '1.1.1.1')

    def _test_download_resource(self, script, expected_content):
        result = None
        try:
            result = self._run(script)
            self.assertTrue(result.startswith(tempfile.gettempdir()))
            self.assertTrue(result.endswith('-resource'))
            with open(result) as f:
                resulting_content = f.read()
            self.assertEqual(expected_content, resulting_content)
        finally:
            if result is not None:
                os.remove(result)

    def test_download_resource(self):
        script = ('path = ctx.download_resource("resource")\n'
                  'ctx.returns(path)')
        self._test_download_resource(
            script=script,
            expected_content='{{ ctx.node.name }}')

    def test_download_resource_with_destination(self):
        fd, temp_path = tempfile.mkstemp(suffix='-resource')
        os.close(fd)
        script = ('path = ctx.download_resource("resource", {0!r})\n'
                  'ctx.returns(path)'.format(temp_path))
        self._test_download_resource(
            script=script,
            expected_content='{{ ctx.node.name }}')

    def test_download_resource_and_render(self):
        script = ('path = ctx.download_resource_and_render("resource")\n'
                  'ctx.returns(path)')
        self._test_download_resource(
            script=script,
            expected_content='test_node')

    def test_download_resource_and_render_with_destination(self):
        fd, temp_path = tempfile.mkstemp(suffix='-resource')
        os.close(fd)
        script = ('path = ctx.download_resource_and_render("resource", {0!r})\n'  # NOQA
                  'ctx.returns(path)'.format(temp_path))
        self._test_download_resource(
            script=script,
            expected_content='test_node')

    def test_download_missing_resource(self):
        script = ('path = ctx.download_resource("missing_resource")\n'
                  'ctx.returns(path)')
        self.assertRaises(ProcessException, self._run, script)
        self.assertTrue(string_in_log(
            '[Errno 2] No such file or directory:',
            self._caplog))

    def test_abort_operation(self):
        script = ('ctx.abort_operation("abort_message")')
        self.assertRaisesRegex(
            NonRecoverableError, 'abort_message', self._run, script)

    def test_retry_operation(self):
        script = ('ctx.retry_operation("retry_message")')
        with self.assertRaises(OperationRetry) as cm:
            self._run(script)
        self.assertIn('retry_message', str(cm.exception))
