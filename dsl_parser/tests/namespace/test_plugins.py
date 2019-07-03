########
# Copyright (c) 2018 Cloudify Platform Ltd. All rights reserved
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

import yaml

from dsl_parser import constants
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestNamespacedPlugins(AbstractTestParser):
    base_blueprint = """
node_types:
  type:
    properties:
      prop1:
        default: value
  cloudify.nodes.Compute:
    properties:
      prop1:
        default: value
node_templates:
  node1:
    type: type
    interfaces:
     interface:
       op: plugin1.op
  node2:
    type: cloudify.nodes.Compute
    interfaces:
     interface:
       op: plugin2.op
"""

    base_plugin_def = {constants.PLUGIN_DISTRIBUTION: 'dist',
                       constants.PLUGIN_DISTRIBUTION_RELEASE: 'release',
                       constants.PLUGIN_DISTRIBUTION_VERSION: 'version',
                       constants.PLUGIN_INSTALL_KEY: True,
                       constants.PLUGIN_INSTALL_ARGUMENTS_KEY: '123',
                       constants.PLUGIN_PACKAGE_NAME: 'name',
                       constants.PLUGIN_PACKAGE_VERSION: 'version',
                       constants.PLUGIN_SOURCE_KEY: 'source',
                       constants.PLUGIN_SUPPORTED_PLATFORM: 'any'}

    @staticmethod
    def _expected_plugin(name, plugin_def):
        plugin = plugin_def.copy()
        plugin['name'] = name
        return plugin

    def test_basic_namespaced_plugin_fields(self):
        imported_yaml = self.base_blueprint
        deployment_plugin_def = self.base_plugin_def.copy()
        deployment_plugin_def[constants.PLUGIN_EXECUTOR_KEY] =\
            'central_deployment_agent'
        host_plugin_def = self.base_plugin_def.copy()
        host_plugin_def[constants.PLUGIN_EXECUTOR_KEY] = 'host_agent'
        raw_parsed = yaml.safe_load(imported_yaml)
        raw_parsed['plugins'] = {
            'plugin1': deployment_plugin_def,
            'plugin2': host_plugin_def
        }
        import_file_name = self.make_yaml_file(yaml.safe_dump(raw_parsed))

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        expected_plugin1 = self._expected_plugin('test--plugin1',
                                                 deployment_plugin_def)
        expected_plugin2 = self._expected_plugin('test--plugin2',
                                                 host_plugin_def)
        plugin1 = parsed_yaml[constants.DEPLOYMENT_PLUGINS_TO_INSTALL][0]
        node2 = self.get_node_by_name(parsed_yaml, 'test--node2')
        plugin2 = node2[constants.PLUGINS_TO_INSTALL][0]
        self.assertEqual(expected_plugin1, plugin1)
        self.assertEqual(expected_plugin2, plugin2)

    def test_basic_namespace_multi_import(self):
        imported_yaml = self.base_blueprint
        deployment_plugin_def = self.base_plugin_def.copy()
        deployment_plugin_def[constants.PLUGIN_EXECUTOR_KEY] =\
            'central_deployment_agent'
        host_plugin_def = self.base_plugin_def.copy()
        host_plugin_def[constants.PLUGIN_EXECUTOR_KEY] = 'host_agent'
        raw_parsed = yaml.safe_load(imported_yaml)
        raw_parsed['plugins'] = {
            'plugin1': deployment_plugin_def,
            'plugin2': host_plugin_def
        }
        import_file_name = self.make_yaml_file(yaml.safe_dump(raw_parsed))

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
    -   {2}--{1}
""".format('test', import_file_name, 'other_test')
        parsed_yaml = self.parse(main_yaml)
        expected_test_plugin1 = self._expected_plugin('test--plugin1',
                                                      deployment_plugin_def)
        expected_other_test_plugin1 = self._expected_plugin(
            'other_test--plugin1', deployment_plugin_def)
        expected_test_plugin2 = self._expected_plugin('test--plugin2',
                                                      host_plugin_def)
        expected_other_test_plugin2 = self._expected_plugin(
            'other_test--plugin2', host_plugin_def)
        other_test_plugin1 =\
            parsed_yaml[constants.DEPLOYMENT_PLUGINS_TO_INSTALL][0]
        test_plugin1 = parsed_yaml[constants.DEPLOYMENT_PLUGINS_TO_INSTALL][1]
        test_node2 = self.get_node_by_name(parsed_yaml, 'test--node2')
        test_plugin2 = test_node2[constants.PLUGINS_TO_INSTALL][0]
        other_test_node2 =\
            self.get_node_by_name(parsed_yaml, 'other_test--node2')
        other_test_plugin2 = other_test_node2[constants.PLUGINS_TO_INSTALL][0]
        self.assertEqual(expected_test_plugin1, test_plugin1)
        self.assertEqual(expected_other_test_plugin1, other_test_plugin1)
        self.assertEqual(expected_test_plugin2, test_plugin2)
        self.assertEqual(expected_other_test_plugin2, other_test_plugin2)

    def test_plugin_collision(self):
        imported_yaml = self.base_blueprint
        deployment_plugin_def = self.base_plugin_def.copy()
        deployment_plugin_def[constants.PLUGIN_EXECUTOR_KEY] =\
            'central_deployment_agent'
        host_plugin_def = self.base_plugin_def.copy()
        host_plugin_def[constants.PLUGIN_EXECUTOR_KEY] = 'host_agent'
        raw_parsed = yaml.safe_load(imported_yaml)
        raw_parsed['plugins'] = {
            'plugin1': deployment_plugin_def,
            'plugin2': host_plugin_def
        }
        import_file_name = self.make_yaml_file(yaml.safe_dump(raw_parsed))

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
plugins:
  plugin1:
    distribution: dist
    distribution_release: release
    distribution_version: version
    executor: central_deployment_agent
    install: true
    install_arguments: '123'
    package_name: name
    package_version: version
    source: source
    supported_platform: any
node_templates:
  node3:
    type: test--type
    interfaces:
     interface:
       op: plugin1.op
""".format('test', import_file_name)
        parsed = self.parse(main_yaml)
        expected_plugin1 = self._expected_plugin('plugin1',
                                                 deployment_plugin_def)
        expected_plugin2 = self._expected_plugin('test--plugin2',
                                                 host_plugin_def)
        expected_test_plugin1 = self._expected_plugin('test--plugin1',
                                                      deployment_plugin_def)
        plugin1 = parsed[constants.DEPLOYMENT_PLUGINS_TO_INSTALL][1]
        node2 = self.get_node_by_name(parsed, 'test--node2')
        plugin2 = node2[constants.PLUGINS_TO_INSTALL][0]
        test_plugin1 = parsed[constants.DEPLOYMENT_PLUGINS_TO_INSTALL][0]
        self.assertEqual(expected_plugin1, plugin1)
        self.assertEqual(expected_plugin2, plugin2)
        self.assertEqual(expected_test_plugin1, test_plugin1)
        self.assertEquals(parsed[constants.HOST_AGENT_PLUGINS_TO_INSTALL],
                          [plugin2])

    def test_plugins_merging_with_no_collision(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  type:
    properties:
      prop1:
        default: value
  cloudify.nodes.Compute:
    properties:
      prop1:
        default: value
node_templates:
  node2:
    type: cloudify.nodes.Compute
    interfaces:
     interface:
       op: plugin2.op
"""

        host_plugin_def = self.base_plugin_def.copy()
        host_plugin_def[constants.PLUGIN_EXECUTOR_KEY] = 'host_agent'
        raw_parsed = yaml.safe_load(imported_yaml)
        raw_parsed['plugins'] = {
            'plugin2': host_plugin_def
        }
        import_file_name = self.make_yaml_file(yaml.safe_dump(raw_parsed))

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
plugins:
  plugin1:
    distribution: dist
    distribution_release: release
    distribution_version: version
    executor: central_deployment_agent
    install: true
    install_arguments: '123'
    package_name: name
    package_version: version
    source: source
    supported_platform: any
node_templates:
  node1:
    type: test--type
    interfaces:
     interface:
       op: plugin1.op
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        deployment_plugin_def = self.base_plugin_def.copy()
        deployment_plugin_def[constants.PLUGIN_EXECUTOR_KEY] =\
            'central_deployment_agent'
        expected_plugin1 = self._expected_plugin('plugin1',
                                                 deployment_plugin_def)
        expected_plugin2 = self._expected_plugin('test--plugin2',
                                                 host_plugin_def)

        plugin1 = parsed_yaml[constants.DEPLOYMENT_PLUGINS_TO_INSTALL][0]
        node2 = self.get_node_by_name(parsed_yaml, 'test--node2')
        plugin2 = node2[constants.PLUGINS_TO_INSTALL][0]
        self.assertEqual(expected_plugin1, plugin1)
        self.assertEqual(expected_plugin2, plugin2)
