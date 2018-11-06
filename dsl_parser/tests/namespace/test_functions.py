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
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestNamespacedWithFunctions(AbstractTestParser):
    def test_concat_with_namespace(self):
        imported_yaml = """
outputs:
    port:
        description: the port
        value: { concat: [one, two, three] }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
""".format('test', import_file_name)

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml))
        self.assertEqual(1, len(parsed_yaml[constants.OUTPUTS]))
        self.assertEqual(
            'onetwothree',
            parsed_yaml[constants.OUTPUTS]['test::port']['value'])

    def test_get_secret(self):
        imported_yaml = """
outputs:
    port:
        description: the port
        value: { get_secret: secret }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
""".format('test', import_file_name)

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml),
                                              self._get_secret_mock)
        self.assertEqual(
            {'get_secret': 'secret'},
            parsed_yaml[constants.OUTPUTS]['test::port']['value'])

    def test_get_property(self):
        imported_yaml = """
outputs:
    port:
        description: the port
        value: { get_property: [node, key] }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
node_types:
    test_type:
        properties:
            key:
                default: value
node_templates:
    node:
        type: test_type
""".format('test', import_file_name)

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml),
                                              self._get_secret_mock)
        self.assertEqual(
            'value',
            parsed_yaml[constants.OUTPUTS]['test::port']['value'])

    def test_get_attribute(self):
        imported_yaml = """
outputs:
    port:
        description: the port
        value: { get_attribute: [node, key] }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
node_types:
    test_type:
        properties:
node_templates:
    node:
        type: test_type
""".format('test', import_file_name)

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml),
                                              self._get_secret_mock)
        self.assertEqual(
            {'get_attribute': ['node', 'key']},
            parsed_yaml[constants.OUTPUTS]['test::port']['value'])

    def test_get_capability(self):
        imported_yaml = """
outputs:
    port:
        description: the port
        value: { get_capability: [ dep_1, cap_a ] }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
""".format('test', import_file_name)

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml),
                                              self._get_secret_mock)
        self.assertEqual(
            {'get_capability': ['dep_1', 'cap_a']},
            parsed_yaml[constants.OUTPUTS]['test::port']['value'])
