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
    @staticmethod
    def _default_input(description, default, name='port'):
        return """
inputs:
    {2}:
        description: {0}
        default: {1}
""".format(description, default, name)

    def _assert_input(self, inputs, name, description, default):
        self.assertEqual(default, inputs[name]['default'])
        self.assertEqual(description, inputs[name]['description'])

    def test_inputs_definition(self):
        imported_yaml = self._default_input('the port', 8080)
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        inputs = parsed_yaml[constants.INPUTS]
        self.assertEqual(1, len(inputs))
        self._assert_input(inputs, 'test->port', 'the port', 8080)

    def test_basic_namespace_multi_import(self):
        imported_yaml = self._default_input('the port', 8080)
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
    -   {2}->{1}
""".format('test', import_file_name, 'other_test')

        parsed_yaml = self.parse(main_yaml)
        inputs = parsed_yaml[constants.INPUTS]
        self.assertEqual(2, len(inputs))
        self._assert_input(inputs, 'test->port', 'the port', 8080)
        self._assert_input(inputs, 'other_test->port', 'the port', 8080)

    def test_input_collision(self):
        imported_yaml = self._default_input('one', 1)
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = """
imports:
  - {0}->{1}
""".format('test', import_file_name) + self._default_input('two', 2)
        parsed_yaml = self.parse_1_3(main_yaml)
        inputs = parsed_yaml[constants.INPUTS]
        self.assertEqual(2, len(inputs))
        self._assert_input(inputs, 'test->port', 'one', 1)
        self._assert_input(inputs, 'port', 'two', 2)

    def test_multi_layer_import_collision(self):
        layer1 = self._default_input('one', 1)
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = """
imports:
  - {0}->{1}
""".format('test1', layer1_import_path) + self._default_input('two', 2)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = """
imports:
  - {0}->{1}
""".format('test', layer2_import_path) + self._default_input('three', 3)
        parsed_yaml = self.parse_1_3(main_yaml)
        inputs = parsed_yaml[constants.INPUTS]
        self.assertEqual(3, len(inputs))
        self._assert_input(inputs, 'test->test1->port', 'one', 1)
        self._assert_input(inputs, 'test->port', 'two', 2)
        self._assert_input(inputs, 'port', 'three', 3)

    def test_inputs_merging_with_no_collision(self):
        imported_yaml = self._default_input('one', 1, 'port1')
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = """
imports:
  - {0}->{1}
""".format('test', import_file_name) + self._default_input('two', 2, 'port2')
        parsed_yaml = self.parse_1_3(main_yaml)
        inputs = parsed_yaml[constants.INPUTS]
        self.assertEqual(2, len(inputs))
        self._assert_input(inputs, 'test->port1', 'one', 1)
        self._assert_input(inputs, 'port2', 'two', 2)
