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

from dsl_parser import functions
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.constants import INTER_DEPLOYMENT_FUNCTIONS, OUTPUTS
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestGetCapability(AbstractTestParser):
    def setUp(self):
        super(TestGetCapability, self).setUp()
        self.mock_storage = self.mock_evaluation_storage(
            capabilities={
                'dep_1': {
                    'cap_a': 'value_a_1',
                    'cap_b': 'value_b_1'
                },
                'dep_2': {
                    'cap_a': 'value_a_2',
                    'cap_b': 'value_b_2'
                }
            }
        )

    def test_has_intrinsic_functions_property(self):
        yaml = """
relationships:
    cloudify.relationships.contained_in: {}
plugins:
    p:
        executor: central_deployment_agent
        install: false
node_types:
    webserver_type: {}
node_templates:
    node:
        type: webserver_type
    webserver:
        type: webserver_type
        interfaces:
            test:
                op_with_no_get_capability:
                    implementation: p.p
                    inputs:
                        a: 1
                op_with_get_capability:
                    implementation: p.p
                    inputs:
                        a:
                            get_capability:
                                - deployment_id
                                - node_template_capability_id
        relationships:
            -   type: cloudify.relationships.contained_in
                target: node
                source_interfaces:
                    test:
                        op_with_no_get_capability:
                            implementation: p.p
                            inputs:
                                a: 1
                        op_with_get_capability:
                            implementation: p.p
                            inputs:
                                a:
                                    get_capability:
                                        - deployment_id
                                        - source_op_capability_id
                target_interfaces:
                    test:
                        op_with_no_get_capability:
                            implementation: p.p
                            inputs:
                                a: 1
                        op_with_get_capability:
                            implementation: p.p
                            inputs:
                                a:
                                    get_capability:
                                        - deployment_id
                                        - target_op_capability_id
"""
        parsed = prepare_deployment_plan(self.parse(yaml))
        webserver_node = None
        for node in parsed.node_templates:
            if node['id'] == 'webserver':
                webserver_node = node
                break
        self.assertIsNotNone(webserver_node)

        def assertion(operations):
            op = operations['test.op_with_no_get_capability']
            self.assertIs(False, op.get('has_intrinsic_functions'))
            op = operations['test.op_with_get_capability']
            self.assertIs(True, op.get('has_intrinsic_functions'))

        assertion(webserver_node['operations'])
        assertion(webserver_node['relationships'][0]['source_operations'])
        assertion(webserver_node['relationships'][0]['target_operations'])

    def test_evaluate_functions(self):

        payload = {
            'a': {'get_capability': ['dep_1', 'cap_a']},
            'b': {'get_capability': ['dep_2', 'cap_a']},
            'c': {'concat': [
                {'get_capability': ['dep_1', 'cap_b']},
                {'get_capability': ['dep_2', 'cap_b']}
            ]}
        }

        functions.evaluate_functions(payload, {}, self.mock_storage)

        self.assertEqual(payload['a'], 'value_a_1')
        self.assertEqual(payload['b'], 'value_a_2')
        self.assertEqual(payload['c'], 'value_b_1value_b_2')

    def test_node_template_properties_simple(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_capability: [ dep_1, cap_a ]}
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        node = self.get_node_by_name(parsed, 'node')
        self.assertEqual({'get_capability': ['dep_1', 'cap_a']},
                         node['properties']['property'])

        functions.evaluate_functions(parsed, {}, self.mock_storage)
        self.assertEqual(node['properties']['property'], 'value_a_1')

    def test_capabilities_in_outputs(self):
        yaml = """
node_types:
  type: {}
node_templates:
  node:
    type: type
outputs:
  output:
    value: { get_capability: [ dep_1, cap_a ]}
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        outputs = parsed.outputs
        self.assertEqual({'get_capability': ['dep_1', 'cap_a']},
                         outputs['output']['value'])

        functions.evaluate_functions(parsed, {}, self.mock_storage)
        self.assertEqual(outputs['output']['value'], 'value_a_1')

    def test_capabilities_in_inputs(self):
        yaml = """
inputs:
  input:
    default: { get_capability: [ dep_1, cap_a ]}
node_types:
  type: {}
node_templates:
  node:
    type: type
outputs:
  output:
    value: { get_input: input }
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        outputs = parsed.outputs

        # `get_input` is evaluated at parse time, so we expect to see it
        # replaced here with the `get_capability` function
        self.assertEqual({'get_capability': ['dep_1', 'cap_a']},
                         outputs['output']['value'])

        functions.evaluate_functions(parsed, {}, self.mock_storage)
        self.assertEqual(outputs['output']['value'], 'value_a_1')

    def test_get_capability_not_list(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_capability: i_should_be_a_list }
"""
        self.assertRaisesRegex(
            ValueError,
            "`get_capability` function argument should be a list. ",
            self.parse_1_3,
            yaml)

    def test_get_capability_short_list(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_capability: [ only_one_item ] }
"""
        self.assertRaisesRegex(
            ValueError,
            "`get_capability` function argument should be a list "
            "with 2 elements at least",
            self.parse_1_3,
            yaml)

    def test_get_capability_first_complex(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_capability: [ [list] , value ] }
"""
        self.assertRaisesRegex(
            ValueError,
            "`get_capability` function arguments can't be complex "
            "values; only strings/ints/functions are accepted. ",
            self.parse_1_3,
            yaml)

    def test_get_capability_registers_to_plan(self):
        yaml = """
node_types:
  type: {}
node_templates:
  node:
    type: type
outputs:
  output1:
    value: { get_capability: [ { get_capability: [ dep_1, cap_a ]}, cap_b ]}
  output2:
    value: { get_capability:
               [ { get_attribute: [node, doesnt_matter] }, cap_a ]}
"""

        def _assert_capability_is_registered(capability_path, expected_value):
            assert any(
                capability_path == idd['function_identifier']
                and expected_value == idd['target_deployment']
                for idd in parsed[INTER_DEPLOYMENT_FUNCTIONS]
            )

        parsed = prepare_deployment_plan(self.parse(yaml))
        get_cap_path1 = '{0}.output1.value.get_capability'.format(OUTPUTS)
        cap_path1_func = {'get_capability': ['dep_1', 'cap_a']}
        get_cap_path2 = '{0}.output1.value.get_capability[0].get_capability' \
                        ''.format(OUTPUTS)
        cap_path2_func = 'dep_1'
        get_cap_path3 = '{0}.output2.value.get_capability'.format(OUTPUTS)
        cap_path3_func = {'get_attribute': ['node', 'doesnt_matter']}

        assert INTER_DEPLOYMENT_FUNCTIONS in parsed
        _assert_capability_is_registered(get_cap_path1, (None, cap_path1_func))
        _assert_capability_is_registered(get_cap_path2, ('dep_1',
                                                         cap_path2_func))
        _assert_capability_is_registered(get_cap_path3, (None, cap_path3_func))

    def test_idd_stores_node_context(self):
        yaml = """
node_types:
  type:
    properties:
        x: {}
node_templates:
  node:
    type: type
    properties:
        x: {get_capability: [{get_attribute: [SELF, x]}, cap_a]}
"""

        parsed = prepare_deployment_plan(self.parse(yaml))
        instance_id = parsed['node_instances'][0]['id']
        idds = parsed[INTER_DEPLOYMENT_FUNCTIONS]
        assert len(idds) == 1
        assert 'SELF' in idds[0]['context']
        assert instance_id == idds[0]['context']['SELF']

    def test_idd_stores_relationship_context(self):
        yaml = """
node_types:
  type: {}
relationships:
  cloudify.relationships.depends_on:
    properties:
      connection_type:
        default: all_to_one
plugins:
  p:
    executor: central_deployment_agent
    install: false
node_templates:
  node1:
    type: type
  node2:
    type: type
  node3:
    type: type
    relationships:
      - target: node1
        type: cloudify.relationships.depends_on
        source_interfaces:
          int1:
            op1:
              implementation: p.p
              inputs:
                a: {get_capability: [dep1, cap1]}
      - target: node2
        type: cloudify.relationships.depends_on
        target_interfaces:
          int1:
            op1:
              implementation: p.p
              inputs:
                a: {get_capability: [dep1, cap1]}
"""

        parsed = prepare_deployment_plan(self.parse(yaml))
        idds = parsed[INTER_DEPLOYMENT_FUNCTIONS]

        instance_ids = {
            ni['node_id']: ni['id'] for ni in parsed['node_instances']
        }
        contexts = {
            (idd['context']['SOURCE'], idd['context']['TARGET'])
            for idd in idds
        }
        assert contexts == {
            (instance_ids['node3'], instance_ids['node1']),
            (instance_ids['node3'], instance_ids['node2'])
        }
