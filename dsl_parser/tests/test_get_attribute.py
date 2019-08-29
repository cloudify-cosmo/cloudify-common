########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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

import testtools.testcase

from dsl_parser import constants
from dsl_parser import exceptions
from dsl_parser import functions
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestGetAttribute(AbstractTestParser):

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
                op_with_no_get_attribute:
                    implementation: p.p
                    inputs:
                        a: 1
                op_with_get_attribute:
                    implementation: p.p
                    inputs:
                        a: { get_attribute: [SELF, a] }
        relationships:
            -   type: cloudify.relationships.contained_in
                target: node
                source_interfaces:
                    test:
                        op_with_no_get_attribute:
                            implementation: p.p
                            inputs:
                                a: 1
                        op_with_get_attribute:
                            implementation: p.p
                            inputs:
                                a: { get_attribute: [SOURCE, a] }
                target_interfaces:
                    test:
                        op_with_no_get_attribute:
                            implementation: p.p
                            inputs:
                                a: 1
                        op_with_get_attribute:
                            implementation: p.p
                            inputs:
                                a: { get_attribute: [TARGET, a] }
"""
        parsed = prepare_deployment_plan(self.parse(yaml))
        webserver_node = None
        for node in parsed.node_templates:
            if node['id'] == 'webserver':
                webserver_node = node
                break
        self.assertIsNotNone(webserver_node)

        def assertion(operations):
            op = operations['test.op_with_no_get_attribute']
            self.assertIs(False, op.get('has_intrinsic_functions'))
            op = operations['test.op_with_get_attribute']
            self.assertIs(True, op.get('has_intrinsic_functions'))

        assertion(webserver_node['operations'])
        assertion(webserver_node['relationships'][0]['source_operations'])
        assertion(webserver_node['relationships'][0]['target_operations'])


class TestEvaluateFunctions(AbstractTestParser):

    def test_evaluate_functions(self):
        def _node_instance(node_id, runtime_proerties):
            return {
                'id': node_id,
                'node_id': node_id,
                'runtime_properties': runtime_proerties
            }

        node_instances = [
            _node_instance('node1', {'a': 'a_val'}),
            _node_instance('node2', {'b': 'b_val'}),
            _node_instance('node3', {'c': 'c_val'}),
            _node_instance('node4', {'d': 'd_val'}),
        ]

        storage = self._mock_evaluation_storage(node_instances=node_instances)
        payload = {
            'a': {'get_attribute': ['SELF', 'a']},
            'b': {'get_attribute': ['node2', 'b']},
            'c': {'get_attribute': ['SOURCE', 'c']},
            'd': {'get_attribute': ['TARGET', 'd']},
            'f': {'concat': [
                {'get_attribute': ['SELF', 'a']},
                {'get_attribute': ['node2', 'b']},
                {'get_attribute': ['SOURCE', 'c']},
                {'get_attribute': ['TARGET', 'd']}
            ]}
        }

        context = {
            'self': 'node1',
            'source': 'node3',
            'target': 'node4'
        }

        functions.evaluate_functions(payload, context, storage)

        self.assertEqual(payload['a'], 'a_val')
        self.assertEqual(payload['b'], 'b_val')
        self.assertEqual(payload['c'], 'c_val')
        self.assertEqual(payload['d'], 'd_val')
        self.assertEqual(payload['f'], 'a_valb_valc_vald_val')

    def test_process_attribute_relationship_ambiguity_resolution(self):

        node_instances = [
            {
                'id': 'node1_1',
                'node_id': 'node1',
                'relationships': [
                    {'target_name': 'node2', 'target_id': 'node2_1'}
                ]
            },
            {
                'id': 'node2_1',
                'node_id': 'node2',
                'runtime_properties': {
                    'key': 'value1'
                }
            },
            {
                'id': 'node2_2',
                'node_id': 'node2',
                'runtime_properties': {
                    'key': 'value2'
                }
            }
        ]

        nodes = [{'id': 'node1'}, {'id': 'node2'}]
        storage = self._mock_evaluation_storage(
            node_instances=node_instances, nodes=nodes)

        payload = {'a': {'get_attribute': ['node2', 'key']}}
        context = {'self': 'node1_1'}

        result = functions.evaluate_functions(payload.copy(), context, storage)
        self.assertEqual(result['a'], 'value1')

        # sanity
        node_instances[0]['relationships'] = []
        storage = self._mock_evaluation_storage(
            node_instances=node_instances, nodes=nodes)
        self.assertRaises(
            exceptions.FunctionEvaluationError,
            functions.evaluate_functions, payload.copy(), context, storage)

    def test_process_attribute_scaling_group_ambiguity_resolution_node_operation(self):  # noqa
        for index in [1, 2]:
            context = {'self': 'node3_{0}'.format(index)}
            self._test_process_attribute_scaling_group_ambiguity_resolution(
                context, index)

    def test_process_attribute_scaling_group_ambiguity_resolution_relationship_operation_source(self):  # noqa
        for index in [1, 2]:
            context = {'source': 'node3_{0}'.format(index),
                       'target': 'stub'}
            self._test_process_attribute_scaling_group_ambiguity_resolution(
                context, index)

    def test_process_attribute_scaling_group_ambiguity_resolution_relationship_operation_target(self):  # noqa
        for index in [1, 2]:
            context = {'target': 'node3_{0}'.format(index),
                       'source': 'stub'}

            self._test_process_attribute_scaling_group_ambiguity_resolution(
                context, index)

    def _test_process_attribute_scaling_group_ambiguity_resolution(
            self, context, index):
        node_instances = [
            {
                'id': 'node1_1',
                'node_id': 'node1',
                'scaling_groups': [{'name': 'g1', 'id': 'g1_1'}]
            },
            {
                'id': 'node2_1',
                'node_id': 'node2',
                'scaling_groups': [{'name': 'g2', 'id': 'g2_1'}],
                'relationships': [{
                    'target_name': 'node1',
                    'target_id': 'node1_1'}]
            },
            {
                'id': 'node3_1',
                'node_id': 'node3',
                'scaling_groups': [{'name': 'g3', 'id': 'g3_1'}],
                'relationships': [{
                    'target_name': 'node2',
                    'target_id': 'node2_1'}]
            },
            {
                'id': 'node4_1',
                'node_id': 'node4',
                'scaling_groups': [{'name': 'g1', 'id': 'g1_1'}]
            },
            {
                'id': 'node5_1',
                'node_id': 'node5',
                'scaling_groups': [{'name': 'g5', 'id': 'g5_1'}],
                'relationships': [{
                    'target_name': 'node4',
                    'target_id': 'node4_1'}]
            },
            {
                'id': 'node6_1',
                'node_id': 'node6',
                'scaling_groups': [{'name': 'g6', 'id': 'g6_1'}],
                'relationships': [{
                    'target_name': 'node5',
                    'target_id': 'node5_1'}],
                'runtime_properties': {
                    'key': 'value6_1'
                }
            },
            {
                'id': 'node1_2',
                'node_id': 'node1',
                'scaling_groups': [{'name': 'g1', 'id': 'g1_2'}]
            },
            {
                'id': 'node2_2',
                'node_id': 'node2',
                'scaling_groups': [{'name': 'g2', 'id': 'g2_2'}],
                'relationships': [{
                    'target_name': 'node1',
                    'target_id': 'node1_2'}]
            },
            {
                'id': 'node3_2',
                'node_id': 'node3',
                'scaling_groups': [{'name': 'g3', 'id': 'g3_2'}],
                'relationships': [{
                    'target_name': 'node2',
                    'target_id': 'node2_2'}],
            },
            {
                'id': 'node4_2',
                'node_id': 'node4',
                'scaling_groups': [{'name': 'g1', 'id': 'g1_2'}]
            },
            {
                'id': 'node5_2',
                'node_id': 'node5',
                'scaling_groups': [{'name': 'g5', 'id': 'g5_2'}],
                'relationships': [{
                    'target_name': 'node4',
                    'target_id': 'node4_2'}]
            },
            {
                'id': 'node6_2',
                'node_id': 'node6',
                'scaling_groups': [{'name': 'g6', 'id': 'g6_2'}],
                'relationships': [{
                    'target_name': 'node5',
                    'target_id': 'node5_2'}],
                'runtime_properties': {
                    'key': 'value6_2'
                }
            },
            {'id': 'stub', 'node_id': 'stub'}
        ]

        nodes_by_id = {}
        for node_instance in node_instances:
            nodes_by_id[node_instance['node_id']] = {
                'id': node_instance['node_id'],
                'relationships': [
                    {'target_id': r['target_name'],
                     'type_hierarchy': [constants.CONTAINED_IN_REL_TYPE]}
                    for r in node_instance.get('relationships', [])],

            }
        storage = self._mock_evaluation_storage(
            node_instances=node_instances, nodes=list(nodes_by_id.values()))
        payload = {'a': {'get_attribute': ['node6', 'key']}}
        functions.evaluate_functions(payload, context, storage)

        self.assertEqual(payload['a'], 'value6_{0}'.format(index))

    def test_process_attributes_properties_fallback(self):
        node_instances = [{
            'id': 'node',
            'node_id': 'webserver',
            'runtime_properties': {}
        }]
        nodes = [{
            'id': 'webserver',
            'properties': {
                'a': 'a_val',
                'b': 'b_val',
                'c': 'c_val',
                'd': 'd_val',
            }
        }]
        storage = self._mock_evaluation_storage(node_instances, nodes)
        payload = {
            'a': {'get_attribute': ['SELF', 'a']},
            'b': {'get_attribute': ['webserver', 'b']},
            'c': {'get_attribute': ['SOURCE', 'c']},
            'd': {'get_attribute': ['TARGET', 'd']},
        }

        context = {
            'self': 'node',
            'source': 'node',
            'target': 'node'
        }

        functions.evaluate_functions(payload, context, storage)

        self.assertEqual(payload['a'], 'a_val')
        self.assertEqual(payload['b'], 'b_val')
        self.assertEqual(payload['c'], 'c_val')
        self.assertEqual(payload['d'], 'd_val')

    def test_process_attributes_no_value(self):
        node_instances = [{
            'id': 'node_1',
            'node_id': 'node',
            'runtime_properties': {}
        }]
        nodes = [{'id': 'node'}]
        storage = self._mock_evaluation_storage(node_instances, nodes)
        payload = {'a': {'get_attribute': ['node', 'a']}}
        functions.evaluate_functions(payload, {}, storage)
        self.assertIsNone(payload['a'])

    def test_missing_self_ref(self):
        payload = {'a': {'get_attribute': ['SELF', 'a']}}
        with testtools.testcase.ExpectedException(
                exceptions.FunctionEvaluationError,
                '.*SELF is missing.*'):
            functions.evaluate_functions(
                payload, {}, self._mock_evaluation_storage())

    def test_missing_source_ref(self):
        payload = {'a': {'get_attribute': ['SOURCE', 'a']}}
        with testtools.testcase.ExpectedException(
                exceptions.FunctionEvaluationError,
                '.*SOURCE is missing.*'):
            functions.evaluate_functions(
                payload, {}, self._mock_evaluation_storage())

    def test_missing_target_ref(self):
        payload = {'a': {'get_attribute': ['TARGET', 'a']}}
        with testtools.testcase.ExpectedException(
                exceptions.FunctionEvaluationError,
                '.*TARGET is missing.*'):
            functions.evaluate_functions(
                payload, {}, self._mock_evaluation_storage())

    def test_no_instances(self):
        payload = {'a': {'get_attribute': ['node', 'a']}}
        with testtools.testcase.ExpectedException(
                exceptions.FunctionEvaluationError,
                '.*has no instances.*'):
            functions.evaluate_functions(
                payload, {}, self._mock_evaluation_storage())

    def test_too_many_instances(self):
        instances = [
            {'id': '1', 'node_id': 'node'},
            {'id': '2', 'node_id': 'node'}
        ]
        storage = self._mock_evaluation_storage(instances)
        payload = {'a': {'get_attribute': ['node', 'a']}}
        with testtools.testcase.ExpectedException(
                exceptions.FunctionEvaluationError,
                '.*unambiguously.*'):
            functions.evaluate_functions(payload, {}, storage)
