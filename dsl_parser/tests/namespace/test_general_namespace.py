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

from dsl_parser import exceptions, constants
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestGeneralNamespace(AbstractTestParser):
    def test_import_with_two_namespace(self):
        imported_yaml = """
inputs:
    port:
        default: 8080
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{0}->{1}
""".format('test', import_file_name)
        self.assertRaises(exceptions.DSLParsingLogicException,
                          self.parse,
                          main_yaml)

    def test_namespace_delimiter_can_be_used_with_no_import_related(self):
        imported_yaml = """
inputs:
    ->port:
        default: 8080
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}
""".format(import_file_name)
        parsed_yaml = self.parse(main_yaml)
        self.assertEqual(1, len(parsed_yaml[constants.INPUTS]))
        self.assertEqual(
            8080,
            parsed_yaml[constants.INPUTS]['->port']['default'])

    def test_mixing_regular_import_with_namespace_import(self):
        basic_input = """
inputs:
    {0}:
        default: 1
"""
        layer1 = basic_input.format('port')
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = """
imports:
  - {0}
""".format(layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = """
imports:
  - {0}->{1}
  - {2}->{1}
""".format('test', layer2_import_path, 'other_test')
        parsed_yaml = self.parse_1_3(main_yaml)
        inputs = parsed_yaml[constants.INPUTS]
        self.assertEqual(2, len(inputs))
        self.assertIn('test->port', inputs)
        self.assertIn('other_test->port', inputs)

    def test_namespace_on_cloudify_basic_types(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   test->http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
"""
        self.assertRaises(exceptions.DSLParsingLogicException,
                          self.parse, yaml)

    def test_namespace_with_cloudify_types_from_imported(self):
        layer1 = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
"""
        layer1_import_path = self.make_yaml_file(layer1)
        main_yaml = """
imports:
  - {0}->{1}
""".format('test', layer1_import_path)
        parsed_yaml = self.parse_1_3(main_yaml)
        workflows = parsed_yaml[constants.WORKFLOWS]
        self.assertIn('install', workflows)

    def test_namespace_on_cloudify_types_from_imported(self):
        layer1 = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   test->http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
"""
        layer1_import_path = self.make_yaml_file(layer1)
        main_yaml = """
imports:
  - {0}->{1}
""".format('test', layer1_import_path)
        self.assertRaises(exceptions.DSLParsingLogicException,
                          self.parse, main_yaml)
