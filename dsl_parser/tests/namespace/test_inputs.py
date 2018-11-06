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
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestNamespacedInputs(AbstractTestParser):

    def test_inputs_definition(self):
        imported_yaml = """
inputs:
    port:
        description: the port
        default: 8080
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        self.assertEqual(1, len(parsed_yaml[constants.INPUTS]))
        self.assertEqual(
            8080,
            parsed_yaml[constants.INPUTS]['test::port']['default'])
        self.assertEqual(
            'the port',
            parsed_yaml[constants.INPUTS]['test::port']['description'])

    def test_basic_namespace_multi_import(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
inputs:
    port:
        description: the port
        default: 8080
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
    -   {2}::{1}
""".format('test', import_file_name, 'other_test')

        parsed_yaml = self.parse(main_yaml)
        self.assertEqual(2, len(parsed_yaml[constants.INPUTS]))
        self.assertEqual(
            8080,
            parsed_yaml[constants.INPUTS]['test::port']['default'])
        self.assertEqual(
            'the port',
            parsed_yaml[constants.INPUTS]['test::port']['description'])
        self.assertEqual(
            8080,
            parsed_yaml[constants.INPUTS]['other_test::port']['default'])
        self.assertEqual(
            'the port',
            parsed_yaml[constants.INPUTS]['other_test::port']['description'])

    def test_input_collision(self):
        imported_yaml = """
inputs:
    port:
        description: one
        default: 1
"""
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = """
imports:
  - {0}::{1}
inputs:
    port:
        description: two
        default: 2
""".format('test', import_file_name)
        parsed_yaml = self.parse_1_3(main_yaml)
        self.assertEqual(2, len(parsed_yaml[constants.INPUTS]))
        self.assertEqual(
            1,
            parsed_yaml[constants.INPUTS]['test::port']['default'])
        self.assertEqual(
            'one',
            parsed_yaml[constants.INPUTS]['test::port']['description'])
        self.assertEqual(2, parsed_yaml[constants.INPUTS]['port']['default'])
        self.assertEqual('two',
                         parsed_yaml[constants.INPUTS]['port']['description'])

    def test_multi_layer_import_collision(self):
        layer1 = """
inputs:
    port:
        description: one
        default: 1
"""
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = """
imports:
  - {0}::{1}
inputs:
    port:
        description: two
        default: 2
""".format('test1', layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = """
imports:
  - {0}::{1}
inputs:
    port:
        description: three
        default: 3
""".format('test', layer2_import_path)
        parsed_yaml = self.parse_1_3(main_yaml)
        self.assertEqual(3, len(parsed_yaml[constants.INPUTS]))
        self.assertEqual(
            1,
            parsed_yaml[constants.INPUTS]['test::test1::port']['default'])
        self.assertEqual(
            'one',
            parsed_yaml[constants.INPUTS]['test::test1::port']['description'])
        self.assertEqual(
            2,
            parsed_yaml[constants.INPUTS]['test::port']['default'])
        self.assertEqual(
            'two',
            parsed_yaml[constants.INPUTS]['test::port']['description'])
        self.assertEqual(3, parsed_yaml[constants.INPUTS]['port']['default'])
        self.assertEqual('three',
                         parsed_yaml[constants.INPUTS]['port']['description'])

    def test_imports_merging_with_no_collision(self):
        imported_yaml = """
inputs:
    port1:
        description: one
        default: 1
"""
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = """
imports:
  - {0}::{1}
inputs:
    port2:
        description: two
        default: 2

""".format('test', import_file_name)
        parsed_yaml = self.parse_1_3(main_yaml)
        self.assertEqual(2, len(parsed_yaml[constants.INPUTS]))
        self.assertEqual(
            1,
            parsed_yaml[constants.INPUTS]['test::port1']['default'])
        self.assertEqual(
            'one',
            parsed_yaml[constants.INPUTS]['test::port1']['description'])
        self.assertEqual(2, parsed_yaml[constants.INPUTS]['port2']['default'])
        self.assertEqual(
            'two',
            parsed_yaml[constants.INPUTS]['port2']['description'])
