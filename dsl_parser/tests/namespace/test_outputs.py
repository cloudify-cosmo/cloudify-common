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


class TestNamespacedOutputs(AbstractTestParser):
    @staticmethod
    def _default_output(description, value, name='port'):
        return """
outputs:
    {2}:
        description: {0}
        value: {1}
""".format(description, value, name)

    def _assert_output(self, outputs, name, description, value):
        self.assertEqual(value, outputs[name]['value'])
        self.assertEqual(description, outputs[name]['description'])

    def test_outputs_definition(self):
        imported_yaml = self._default_output('the port', 1)
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        outputs = parsed_yaml[constants.OUTPUTS]
        self.assertEqual(1, len(outputs))
        self._assert_output(outputs, 'test--port', 'the port', 1)

    def test_basic_namespace_multi_import(self):
        imported_yaml = self._default_output('the port', 8080)
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
    -   {2}--{1}
""".format('test', import_file_name, 'other_test')

        parsed_yaml = self.parse(main_yaml)
        outputs = parsed_yaml[constants.OUTPUTS]
        self.assertEqual(2, len(outputs))
        self._assert_output(outputs, 'test--port', 'the port', 8080)
        self._assert_output(outputs, 'other_test--port', 'the port', 8080)

    def test_outputs_collision(self):
        imported_yaml = self._default_output('one', 1)
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = """
imports:
  - {0}--{1}
""".format('test', import_file_name) + self._default_output('two', 2)
        parsed_yaml = self.parse_1_3(main_yaml)
        outputs = parsed_yaml[constants.OUTPUTS]
        self.assertEqual(2, len(outputs))
        self._assert_output(outputs, 'test--port', 'one', 1)
        self._assert_output(outputs, 'port', 'two', 2)

    def test_multi_layer_import_collision(self):
        layer1 = self._default_output('one', 1)
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = """
imports:
  - {0}--{1}
""".format('test1', layer1_import_path) + self._default_output('two', 2)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = """
imports:
  - {0}--{1}
""".format('test', layer2_import_path) + self._default_output('three', 3)
        parsed_yaml = self.parse_1_3(main_yaml)
        outputs = parsed_yaml[constants.OUTPUTS]
        self.assertEqual(3, len(outputs))
        self._assert_output(outputs, 'test--test1--port', 'one', 1)
        self._assert_output(outputs, 'test--port', 'two', 2)
        self._assert_output(outputs, 'port', 'three', 3)

    def test_outputs_merging_with_no_collision(self):
        imported_yaml = self._default_output('one', 1, 'port1')
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = """
imports:
  - {0}--{1}
""".format('test', import_file_name) + self._default_output('two', 2, 'port2')
        parsed_yaml = self.parse_1_3(main_yaml)
        outputs = parsed_yaml[constants.OUTPUTS]
        self.assertEqual(2, len(outputs))
        self._assert_output(outputs, 'test--port1', 'one', 1)
        self._assert_output(outputs, 'port2', 'two', 2)
