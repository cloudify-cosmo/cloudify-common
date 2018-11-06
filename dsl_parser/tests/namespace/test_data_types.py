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

from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestNamespaceDataTypes(AbstractTestParser):
    def test_basic_namespace_import(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
data_types:
    test_type:
        properties:
            prop:
                default: value
    using_test_type:
        properties:
            test_prop:
                type: test_type
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
node_types:
    type:
        properties:
            prop1:
                type: test::using_test_type
node_templates:
    node:
        type: type
""".format('test', import_file_name)
        self.parse_1_2(main_yaml)

    def test_basic_namespace_multi_import(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
data_types:
    test_type:
        properties:
            prop:
                default: value
    using_test_type:
        properties:
            test_prop:
                type: test_type
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
    -   {2}::{1}
node_types:
    type:
        properties:
            prop1:
                type: test::using_test_type
    other_type:
        properties:
            prop1:
                type: other_test::using_test_type
node_templates:
    node:
        type: type
    other_node:
        type: other_type
""".format('test', import_file_name, 'other_test')

        parsed_yaml = self.parse_1_3(main_yaml)
        node_properties = parsed_yaml['nodes'][0]['properties']
        self.assertEqual(
            node_properties['prop1']['test_prop']['prop'], 'value')
        other_node_properties = parsed_yaml['nodes'][1]['properties']
        self.assertEqual(
            other_node_properties['prop1']['test_prop']['prop'], 'value')

    def test_data_type_collision(self):
        imported_yaml = """
data_types:
    data1:
        properties:
            prop1:
                default: value1
"""
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = """
imports:
  - {0}::{1}
data_types:
    data1:
        properties:
            prop1:
                default: value2
node_types:
    type:
        properties:
            prop1:
                type: test::data1
            prop2:
                type: data1
node_templates:
    node:
        type: type
""".format('test', import_file_name)
        properties = self.parse_1_3(main_yaml)['nodes'][0]['properties']
        self.assertEqual(properties['prop1']['prop1'], 'value1')
        self.assertEqual(properties['prop2']['prop1'], 'value2')

    def test_multi_layer_import_collision(self):
        layer1 = """
data_types:
    data1:
        properties:
            prop1:
                default: value1
"""
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = """
imports:
  - {0}::{1}
data_types:
    data1:
        properties:
            prop1:
                default: value2
""".format('test1', layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        yaml = """
imports:
  - {0}::{1}
data_types:
    data1:
        properties:
            prop1:
                default: value3
node_types:
    type:
        properties:
            prop1:
                type: test::data1
            prop2:
                type: data1
            prop3:
                type: test::test1::data1
node_templates:
    node:
        type: type
""".format('test', layer2_import_path)
        properties = self.parse_1_3(yaml)['nodes'][0]['properties']
        self.assertEqual(properties['prop1']['prop1'], 'value2')
        self.assertEqual(properties['prop2']['prop1'], 'value3')
        self.assertEqual(properties['prop3']['prop1'], 'value1')

    def test_imports_merging_with_no_collision(self):
        imported_yaml = """
data_types:
    data1:
        properties:
            prop1:
                default: value1
"""
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = """
imports:
  - {0}::{1}
data_types:
    data2:
        properties:
            prop2:
                default: value2
node_types:
    type:
        properties:
            prop1:
                type: test::data1
            prop2:
                type: data2
node_templates:
    node:
        type: type
""".format('test', import_file_name)
        properties = self.parse_1_3(main_yaml)['nodes'][0]['properties']
        self.assertEqual(properties['prop1']['prop1'], 'value1')
        self.assertEqual(properties['prop2']['prop2'], 'value2')

    def test_derives_from_with_namespace_import(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
data_types:
    agent_connection:
        properties:
            username:
                type: string
                default: ubuntu
            key:
                type: string
                default: ~/.ssh/id_rsa
    agent_installer:
        properties:
            connection:
                type: agent_connection
                default: {}
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - {0}::{1}
node_types:
    vm_type:
        properties:
            agent:
                type: agent
node_templates:
    vm:
        type: vm_type
        properties:
            agent:
                connection:
                    key: /home/ubuntu/id_rsa
data_types:
    agent:
        derived_from: test::agent_installer
        properties:
            basedir:
                type: string
                default: /home/
""".format('test', import_file_name)

        parsed = self.parse(main_yaml)
        vm = parsed['nodes'][0]
        self.assertEqual(
            'ubuntu',
            vm['properties']['agent']['connection']['username'])
        self.assertEqual(
            '/home/ubuntu/id_rsa',
            vm['properties']['agent']['connection']['key'])
