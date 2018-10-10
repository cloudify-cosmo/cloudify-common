########
# Copyright (c) 2018 GigaSpaces Technologies Ltd. All rights reserved
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
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestGetCapability(AbstractTestParser):
    def setUp(self):
        self.shared_deployments = {
            'dep_1': {
                'capabilities': {
                    'cap_a': 'value_a_1',
                    'cap_b': 'value_b_1'
                }
            },
            'dep_2': {
                'capabilities': {
                    'cap_a': 'value_a_2',
                    'cap_b': 'value_b_2'
                }
            }
        }
        super(TestGetCapability, self).setUp()

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
                        a: { get_capability: [ deployment_id, node_template_capability_id ] }
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
                                a: { get_capability: [ deployment_id, source_op_capability_id ] }
                target_interfaces:
                    test:
                        op_with_no_get_capability:
                            implementation: p.p
                            inputs:
                                a: 1
                        op_with_get_capability:
                            implementation: p.p
                            inputs:
                                a: { get_capability: [ deployment_id, target_op_capability_id ] }
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


    def get_capability_mock(self, capability_path):
        dep_id, cap_id = capability_path[0], capability_path[1]
        return self.shared_deployments[dep_id]['capabilities'][cap_id]

    def test_evaluate_functions(self):

        payload = {
            'a': {'get_capability': ['dep_1', 'cap_a']},
            'b': {'get_capability': ['dep_2', 'cap_a']},
            'c': {'concat': [
                {'get_capability': ['dep_1', 'cap_b']},
                {'get_capability': ['dep_2', 'cap_b']}
            ]}
        }

        functions.evaluate_functions(payload,
                                     {},
                                     None,
                                     None,
                                     None,
                                     None,
                                     self.get_capability_mock)

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
        parsed = prepare_deployment_plan(self.parse_1_3(yaml),
                                         self._get_secret_mock)
        node = self.get_node_by_name(parsed, 'node')
        self.assertEqual({'get_capability': ['dep_1', 'cap_a']},
                         node['properties']['property'])

        functions.evaluate_functions(
            parsed,
            {},
            None,
            None,
            None,
            None,
            self.get_capability_mock,
        )
        self.assertEqual(node['properties']['property'], 'value_a_1')
