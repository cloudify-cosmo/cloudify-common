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


class TestNamespacedCapabilities(AbstractTestParser):
    default_capability_name = 'port'
    default_capability_description = 'the port'
    default_capability_value = 8080

    @staticmethod
    def _basic_capability(description=default_capability_description,
                          value=default_capability_value,
                          name=default_capability_name):
        return """
capabilities:
    {2}:
        description: {0}
        value: {1}
""".format(description, value, name)

    def _assert_capability(self, capability, description, value):
        self.assertEqual(capability['description'], description)
        self.assertEqual(capability['value'], value)

    def test_capabilities_definition(self):
        imported_yaml = self._basic_capability()
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse_1_3(main_yaml)
        capabilities = parsed_yaml[constants.CAPABILITIES]
        self.assertEqual(1, len(capabilities))
        self._assert_capability(
            capability=capabilities['test->port'],
            description=self.default_capability_description,
            value=self.default_capability_value
        )

    def test_basic_namespace_multi_import(self):
        imported_yaml = self._basic_capability()
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
    -   {2}->{1}
""".format('test', import_file_name, 'other_test')

        parsed_yaml = self.parse_1_3(main_yaml)
        capabilities = parsed_yaml[constants.CAPABILITIES]
        self.assertEqual(2, len(capabilities))
        self._assert_capability(
            capability=capabilities['test->port'],
            description=self.default_capability_description,
            value=self.default_capability_value
        )
        self._assert_capability(
            capability=capabilities['other_test->port'],
            description=self.default_capability_description,
            value=self.default_capability_value
        )

    def test_capability_collision(self):
        imported_yaml = self._basic_capability('one', 1)
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = self._basic_capability('two', 2) + """
imports:
  - {0}->{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse_1_3(main_yaml)
        capabilities = parsed_yaml[constants.CAPABILITIES]
        self.assertEqual(2, len(capabilities))
        self._assert_capability(
            capability=capabilities['test->port'],
            description='one',
            value=1
        )
        self._assert_capability(
            capability=capabilities['port'],
            description='two',
            value=2
        )

    def test_multi_layer_import_collision(self):
        layer1 = self._basic_capability('one', 1)
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = self._basic_capability('two', 2) + """
imports:
  - {0}->{1}
""".format('test1', layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = self._basic_capability('three', 3) + """
imports:
  - {0}->{1}
""".format('test', layer2_import_path)
        parsed_yaml = self.parse_1_3(main_yaml)
        capabilities = parsed_yaml[constants.CAPABILITIES]
        self.assertEqual(3, len(capabilities))
        self._assert_capability(
            capability=capabilities['test->test1->port'],
            description='one',
            value=1
        )
        self._assert_capability(
            capability=capabilities['test->port'],
            description='two',
            value=2
        )
        self._assert_capability(
            capability=capabilities['port'],
            description='three',
            value=3
        )

    def test_capabilities_merging_with_no_collision(self):
        imported_yaml = self._basic_capability('one', 1)
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = self._basic_capability('two', 2, 'port2') + """
imports:
  - {0}->{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse_1_3(main_yaml)
        capabilities = parsed_yaml[constants.CAPABILITIES]
        self.assertEqual(2, len(capabilities))
        self._assert_capability(
            capability=capabilities['test->port'],
            description='one',
            value=1
        )
        self._assert_capability(
            capability=capabilities['port2'],
            description='two',
            value=2
        )
