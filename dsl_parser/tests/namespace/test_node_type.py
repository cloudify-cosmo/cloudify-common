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

from dsl_parser import constants
from dsl_parser.tests import scaling
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestGeneralNodeTypeNamespaceImport(AbstractTestParser):

    def test_node_type_import(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_templates:
    test_node:
        type: test::test_type
imports:
    -   {0}::{1}
""".format('test', import_file_name)

        parsed = self.parse(main_yaml)
        vm = parsed['nodes'][0]
        self.assertEqual(
            'value',
            vm['properties']['prop1'])

    def test_basic_namespace_multi_import(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_templates:
    test_node:
        type: test::test_type
    other_node:
        type: other::test_type
imports:
    -   {0}::{1}
    -   {2}::{1}
""".format('test', import_file_name, 'other')

        parsed = self.parse(main_yaml)
        test_node = parsed['nodes'][0]
        self.assertEqual(
            'value',
            test_node['properties']['prop1'])
        other_node = parsed['nodes'][1]
        self.assertEqual(
            'value',
            other_node['properties']['prop1'])

    def test_node_type_collision_import(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value2
node_templates:
    test_node1:
        type: test::test_type
    test_node2:
        type: test_type
imports:
    -   {0}::{1}
""".format('test', import_file_name)

        parsed = self.parse(main_yaml)
        vm = parsed['nodes'][0]
        self.assertEqual(
            'value2',
            vm['properties']['prop1'])
        vm = parsed['nodes'][1]
        self.assertEqual(
            'value',
            vm['properties']['prop1'])

    def test_merging_node_type_import(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type2:
    properties:
      prop1:
        default: value

node_templates:
    test_node:
        type: test::test_type
    test_node2:
        type: test_type2
imports:
    -   {0}::{1}
""".format('test', import_file_name)

        parsed = self.parse(main_yaml)
        vm = parsed['nodes'][0]
        self.assertEqual(
            'value',
            vm['properties']['prop1'])
        vm = parsed['nodes'][1]
        self.assertEqual(
            'value',
            vm['properties']['prop1'])

    def test_multi_layer_import_collision(self):
        layer1 = """
node_types:
  test_type:
    properties:
      prop1:
        default: value1
"""
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = """
imports:
  - {0}::{1}
node_types:
  test_type:
    properties:
      prop1:
        default: value2
""".format('test1', layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = """
imports:
  - {0}::{1}
node_types:
  test_type:
    properties:
      prop1:
        default: value3
node_templates:
    test_node:
        type: test_type
    test_node2:
        type: test::test_type
    test_node3:
        type: test::test1::test_type
""".format('test', layer2_import_path)
        parsed = self.parse_1_3(main_yaml)
        for i in xrange(0, 3):
            vm = parsed['nodes'][i]
            self.assertEqual(
                'value{0}'.format(i+1),
                vm['properties']['prop1'])

    def test_multi_layer_same_import_collision(self):
        layer1 = """
node_types:
  test_type:
    properties:
      prop1:
        default: value1
"""
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = """
imports:
  - {0}::{1}
node_templates:
  test_node:
    type: test1::test_type
""".format('test1', layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = """
imports:
  - {0}::{1}
  - {2}
node_templates:
    test_node2:
        type: test::test_type
""".format('test', layer1_import_path, layer2_import_path)
        parsed = self.parse_1_3(main_yaml)
        vm = parsed['nodes'][0]
        self.assertEqual(
            'value1',
            vm['properties']['prop1'])
        vm = parsed['nodes'][1]
        self.assertEqual(
            'value1',
            vm['properties']['prop1'])


class TestDetailNodeTypeNamespaceImport(AbstractTestParser):
    def test_derived_from(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type2:
    derived_from: test::test_type
node_templates:
    test_node:
        type: test_type2
imports:
    -   {0}::{1}
""".format('test', import_file_name)

        parsed = self.parse(main_yaml)
        vm = parsed['nodes'][0]
        self.assertEqual(
            'value',
            vm['properties']['prop1'])


class TestNamespacedMultiInstance(scaling.BaseTestMultiInstance):
    def test_scalable(self):
        imported_yaml = self.BASE_BLUEPRINT + """
    host:
        type: cloudify.nodes.Compute
        capabilities:
            scalable:
                properties:
                    default_instances: 2
                    min_instances: 1
                    max_instances: 10
"""

        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
""".format('test', import_file_name)

        multi_plan = self.parse_multi(main_yaml)
        nodes_instances = multi_plan[constants.NODE_INSTANCES]
        self.assertEquals(2, len(nodes_instances))
        self.assertEquals(2, len(set(self._node_ids(nodes_instances))))

        self.assertIn('host_', nodes_instances[0]['id'])
        self.assertIn('host_', nodes_instances[1]['id'])
        self.assertEquals(nodes_instances[0]['id'],
                          nodes_instances[0]['host_id'])
        self.assertEquals(nodes_instances[1]['id'],
                          nodes_instances[1]['host_id'])
        node = multi_plan[constants.NODES][0]
        node_props = node[constants.CAPABILITIES]['scalable']['properties']
        self.assertEqual(1, node_props['min_instances'])
        self.assertEqual(10, node_props['max_instances'])
