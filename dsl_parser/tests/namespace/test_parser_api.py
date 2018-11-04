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


class TestNamespaceImport(AbstractTestParser):

    def test_node_type_import(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value"""
        bottom_file_name = self.make_yaml_file(yaml)

        top_level_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_templates:
    test_node:
        type: test::test_type
imports:
    -   {0}::{1}
""".format('test', bottom_file_name)

        parsed = self.parse(top_level_yaml)
        vm = parsed['nodes'][0]
        self.assertEqual(
            'value',
            vm['properties']['prop1'])

    def test_node_type_collision_import(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value"""
        bottom_file_name = self.make_yaml_file(yaml)

        top_level_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
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
""".format('test', bottom_file_name)

        self.parse(top_level_yaml)

    def test_merging_node_type_import(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value"""
        bottom_file_name = self.make_yaml_file(yaml)

        top_level_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
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
""".format('test', bottom_file_name)

        self.parse(top_level_yaml)
