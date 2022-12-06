########
# Copyright (c) 2021 Cloudify Platform Ltd. All rights reserved
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

from dsl_parser import exceptions, functions
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestGetEnvironmentCapability(AbstractTestParser):
    def setUp(self):
        super(TestGetEnvironmentCapability, self).setUp()
        self.mock_storage = self.mock_evaluation_storage(
            capabilities={
                'dep1': {
                    'cap_a': 'value_a_1',
                    'cap_b': 'value_b_1'
                }
            },
            labels={
                'csys-obj-parent': ['dep1'],
            }
        )

    def test_evaluate_functions(self):

        payload = {
            'a': {'get_environment_capability': 'cap_a'},
            'b': {'get_environment_capability': 'cap_b'},
        }

        functions.evaluate_functions(payload, {}, self.mock_storage)

        self.assertEqual(payload['a'], 'value_a_1')
        self.assertEqual(payload['b'], 'value_b_1')

    def test_reports_requirements(self):
        yaml = """
node_types:
    type:
        properties:
            property_1: {}
node_templates:
    node:
        type: type
        properties:
            property_1: { get_environment_capability: cap_a }
outputs:
    out1:
        value: { get_environment_capability: [cap_b, attr] }
"""
        plan = self.parse(yaml)
        assert 'requirements' in plan
        requirements = plan['requirements']
        assert 'parent_capabilities' in requirements
        parent_capabilities = requirements['parent_capabilities']
        assert ['cap_a'] in parent_capabilities
        assert ['cap_b', 'attr'] in parent_capabilities

    def test_get_environment_capability_in_properties(self):
        yaml = """
node_types:
    type:
        properties:
            property_1: {}
            property_2: {}
node_templates:
    node:
        type: type
        properties:
            property_1: { get_environment_capability: cap_a }
            property_2: { get_environment_capability: cap_b }
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        node = self.get_node_by_name(parsed, 'node')
        self.assertEqual({'get_environment_capability': 'cap_a'},
                         node['properties']['property_1'])
        self.assertEqual({'get_environment_capability': 'cap_b'},
                         node['properties']['property_2'])

        functions.evaluate_functions(parsed, {}, self.mock_storage)
        self.assertEqual(node['properties']['property_1'], 'value_a_1')
        self.assertEqual(node['properties']['property_2'], 'value_b_1')

    def test_get_environment_capability_in_outputs(self):
        yaml = """
node_types:
    type: {}
node_templates:
    node:
        type: type
outputs:
    output_1:
        value: { get_environment_capability: cap_a }
    output_2:
        value: { get_environment_capability: cap_b }
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        outputs = parsed.outputs
        self.assertEqual({'get_environment_capability': 'cap_a'},
                         outputs['output_1']['value'])
        self.assertEqual({'get_environment_capability': 'cap_b'},
                         outputs['output_2']['value'])

        functions.evaluate_functions(parsed, {}, self.mock_storage)
        self.assertEqual(outputs['output_1']['value'], 'value_a_1')
        self.assertEqual(outputs['output_2']['value'], 'value_b_1')

    def test_get_environment_capability_in_inputs(self):
        yaml = """
inputs:
    input_1:
        default: { get_environment_capability: cap_a }
    input_2:
        default: { get_environment_capability: cap_b }
node_types:
    type: {}
node_templates:
    node:
        type: type
outputs:
    output_1:
        value: { get_input: input_1 }

    output_2:
        value: { get_input: input_2 }
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        outputs = parsed.outputs
        self.assertEqual({'get_environment_capability': 'cap_a'},
                         outputs['output_1']['value'])
        self.assertEqual({'get_environment_capability': 'cap_b'},
                         outputs['output_2']['value'])

        functions.evaluate_functions(parsed, {}, self.mock_storage)
        self.assertEqual(outputs['output_1']['value'], 'value_a_1')
        self.assertEqual(outputs['output_2']['value'], 'value_b_1')

    def test_get_environment_capability_invalid_argument_dict(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_environment_capability: { item1: item2 } }
"""
        self.assertRaisesRegex(
            exceptions.FunctionValidationError,
            '`get_environment_capability` function argument should be',
            self.parse_1_3,
            yaml)

    def test_get_environment_capability_invalid_argument_list(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_environment_capability: [item1] }
"""
        self.assertRaisesRegex(
            exceptions.FunctionValidationError,
            '`get_environment_capability` function arguments can\'t be',
            self.parse_1_3,
            yaml)

    def test_get_environment_capability_invalid_argument_complex(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_environment_capability: [item1, {foo: boo}] }
"""
        self.assertRaisesRegex(
            ValueError,
            '`get_environment_capability` function arguments can\'t be',
            self.parse_1_3,
            yaml)
