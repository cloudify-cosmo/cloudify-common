########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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

import os
import mock
import logging
import tempfile
from testtools import TestCase

from cloudify import utils
from cloudify.exceptions import CommandExecutionException
from cloudify.utils import (
    setup_logger,
    merge_plugins,
    get_exec_tempdir,
    LocalCommandRunner)

from dsl_parser.constants import PLUGIN_INSTALL_KEY, PLUGIN_NAME_KEY


class LocalCommandRunnerTest(TestCase):

    runner = None

    @classmethod
    def setUpClass(cls):
        cls.logger = setup_logger(cls.__name__)
        cls.logger.setLevel(logging.DEBUG)
        cls.runner = LocalCommandRunner(
            logger=cls.logger)

    def test_run_command_success(self):
        response = self.runner.run('echo Hello')
        self.assertEqual('Hello', response.std_out)
        self.assertEqual(0, response.return_code)
        self.assertEqual('', response.std_err)

    def test_run_command_error(self):
        try:
            self.runner.run('/bin/sh -c bad')
            self.fail('Expected CommandExecutionException due to Bad command')
        except CommandExecutionException as e:
            self.assertTrue(1, e.code)

    def test_run_command_with_env(self):
        response = self.runner.run('env',
                                   execution_env={'TEST_KEY': 'TEST_VALUE'})
        self.assertTrue('TEST_KEY=TEST_VALUE' in response.std_out)


class TempdirTest(TestCase):
    def test_executable_no_override(self):
        sys_default_tempdir = tempfile.gettempdir()
        self.assertEqual(sys_default_tempdir, get_exec_tempdir())

    @mock.patch.dict(os.environ, {'CFY_EXEC_TEMP': '/fake/temp'})
    def test_executable_override(self):
        self.assertEqual('/fake/temp', get_exec_tempdir())


class TestPluginFunctions(TestCase):

    def test_extract_plugins_is_correct(self):
        def true(_):
            return True

        def false(_):
            return False

        def three(p):
            return p['dummy'] == 3

        plugin_list = [{PLUGIN_INSTALL_KEY: True, 'dummy': 1}] * 2
        result = utils.extract_plugins_to_install(plugin_list, true)
        self.assertListEqual(result, plugin_list)
        result = utils.extract_plugins_to_install(plugin_list, false)
        self.assertListEqual(result, [])

        p = {PLUGIN_INSTALL_KEY: True, 'dummy': 3}
        plugin_list.append(p)
        result = utils.extract_plugins_to_install(plugin_list, three)
        self.assertListEqual(result, [p])

    def test_extract_and_merge_to_install_executes_correctly(self):
        with mock.patch('cloudify.utils.merge_plugins') as merge_plugins_mock:
            dep_plugins = [{PLUGIN_INSTALL_KEY: True, 'dummy': 1},
                           {PLUGIN_INSTALL_KEY: True, 'dummy': 2}]
            wf_plugins = [{PLUGIN_INSTALL_KEY: True, 'dummy': 2}]
            utils.extract_and_merge_plugins_to_install(
                dep_plugins, wf_plugins, lambda x: x['dummy'] == 2)
            merge_plugins_mock.assert_called_with(dep_plugins[1:], wf_plugins)

    def test_merge_is_correct(self):
        deployment_plugins = [{PLUGIN_NAME_KEY: 'dummy{0}'.format(i)}
                              for i in range(3)]
        workflow_plugins = deployment_plugins + [{PLUGIN_NAME_KEY: 'dummy3'}]
        result = merge_plugins(deployment_plugins, workflow_plugins)
        # Checks that no duplicates are made
        self.assertListEqual(result, workflow_plugins)

    def test_extract_and_merge_to_uninstall_executes_correctly(self):
        dep_plugins = [{PLUGIN_INSTALL_KEY: True, 'dummy': 1},
                       {PLUGIN_INSTALL_KEY: True, 'dummy': 2}]
        wf_plugins = [{PLUGIN_INSTALL_KEY: True, 'dummy': 2}]
        res = utils.extract_and_merge_plugins_to_uninstall(
            dep_plugins, wf_plugins, lambda x: x['dummy'] == 2)
        self.assertListEqual(dep_plugins[1:] + wf_plugins, res)

    def test_install_task_not_executed(self):
        utils.add_plugins_to_install(None, False, None)

    def test_uinstall_task_not_executed(self):
        utils.add_plugins_to_uninstall(None, False, None)


class TestPluginInstallationTaskExecutionFuncs(TestCase):
    def setUp(self):
        super(TestPluginInstallationTaskExecutionFuncs, self).setUp()
        self.ctx_mock = mock.Mock()
        self.ctx_mock.send_event.return_value = 'send_event'
        self.ctx_mock.execute_task.return_value = 'execute_task'
        self.sequence_mock = mock.Mock()

    def test_add_plugin_to_install_adds_to_sequence(self):
        plugins_list = ['something']
        utils.add_plugins_to_install(
            self.ctx_mock, plugins_list, self.sequence_mock)

        self.ctx_mock.send_event.assert_called()
        self.ctx_mock.execute_task.assert_called_with(
            task_name='cloudify_agent.operations.install_plugins',
            kwargs={'plugins': plugins_list})
        self.sequence_mock.add.assert_called_with('send_event', 'execute_task')

    def test_add_plugin_to_uninstall_adds_to_sequence(self):
        plugins_list = ['something']
        utils.add_plugins_to_uninstall(
            self.ctx_mock, plugins_list, self.sequence_mock)

        self.ctx_mock.send_event.assert_called()
        self.ctx_mock.execute_task.assert_called_with(
            task_name='cloudify_agent.operations.uninstall_plugins',
            kwargs={
                'plugins': plugins_list,
                'delete_managed_plugins': False})
        self.sequence_mock.add.assert_called_with('send_event', 'execute_task')
