########
# Copyright (c) 2013-2019 Cloudify Platform Ltd. All rights reserved
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

from dsl_parser.interfaces.constants import NO_OP
from dsl_parser.interfaces.utils import operation_mapping
from dsl_parser.exceptions import DSLParsingLogicException
from dsl_parser.tests.test_parser_api import op_struct, BaseParserApiTest


class TestRelationships(BaseParserApiTest):
    def test_empty_top_level_relationships(self):
        yaml = self.MINIMAL_BLUEPRINT + """
relationships: {}
        """
        result = self.parse(yaml)
        self._assert_minimal_blueprint(result)
        self.assertEquals(0, len(result['relationships']))

    def test_empty_top_level_relationships_empty_relationship(self):
        yaml = self.MINIMAL_BLUEPRINT + """
relationships:
    test_relationship: {}
        """
        result = self.parse(yaml)
        self._assert_minimal_blueprint(result)
        self.assertEqual({'name': 'test_relationship', 'properties': {},
                          'source_interfaces': {}, 'target_interfaces': {},
                          'type_hierarchy': ['test_relationship']},
                         result['relationships']['test_relationship'])

    def test_top_level_relationships_single_complete_relationship(self):
        yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
relationships:
    empty_rel: {}
    test_relationship:
        derived_from: empty_rel
        source_interfaces:
            test_interface3:
                test_interface3_op1: {}
        target_interfaces:
            test_interface4:
                test_interface4_op1:
                    implementation: test_plugin.task_name
                    inputs: {}
        """
        result = self.parse(yaml)
        self._assert_blueprint(result)
        self.assertEqual({'name': 'empty_rel', 'properties': {},
                          'source_interfaces': {},
                          'target_interfaces': {},
                          'type_hierarchy': ['empty_rel']},
                         result['relationships']['empty_rel'])
        test_relationship = result['relationships']['test_relationship']
        self.assertEquals('test_relationship', test_relationship['name'])
        self.assertEquals(test_relationship['type_hierarchy'],
                          ['empty_rel', 'test_relationship'])
        result_test_interface_3 = \
            test_relationship['source_interfaces']['test_interface3']
        self.assertEquals(NO_OP,
                          result_test_interface_3['test_interface3_op1'])
        result_test_interface_4 = \
            test_relationship['target_interfaces']['test_interface4']
        self.assertEquals(
            operation_mapping(implementation='test_plugin.task_name',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            result_test_interface_4['test_interface4_op1'])

    def test_top_level_relationships_recursive_imports(self):
        bottom_level_yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
relationships:
    empty_rel: {}
    test_relationship:
        derived_from: empty_rel
        source_interfaces:
            test_interface2:
                install:
                    implementation: test_plugin.install
                    inputs: {}
                terminate:
                    implementation: test_plugin.terminate
                    inputs: {}
        """

        bottom_file_name = self.make_yaml_file(bottom_level_yaml)
        mid_level_yaml = """
relationships:
    test_relationship2:
        derived_from: "test_relationship3"
imports:
    -   {0}""".format(bottom_file_name)
        mid_file_name = self.make_yaml_file(mid_level_yaml)
        top_level_yaml = """
relationships:
    test_relationship3:
        target_interfaces:
            test_interface2:
                install:
                    implementation: test_plugin.install
                    inputs: {}
                terminate:
                    implementation: test_plugin.terminate
                    inputs: {}

imports:
    - """ + mid_file_name

        result = self.parse(top_level_yaml)
        self._assert_blueprint(result)
        self.assertEqual({'name': 'empty_rel', 'properties': {},
                          'source_interfaces': {}, 'target_interfaces': {},
                          'type_hierarchy': ['empty_rel']},
                         result['relationships']['empty_rel'])
        test_relationship = result['relationships']['test_relationship']
        self.assertEquals('test_relationship',
                          test_relationship['name'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.install',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            test_relationship['source_interfaces'][
                'test_interface2']['install'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.terminate',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            test_relationship['source_interfaces'][
                'test_interface2']['terminate'])
        self.assertEquals(
            2, len(test_relationship['source_interfaces'][
                       'test_interface2']))
        self.assertEquals(6, len(test_relationship))

        test_relationship2 = result['relationships']['test_relationship2']
        self.assertEquals('test_relationship2',
                          test_relationship2['name'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.install',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            test_relationship2['target_interfaces'][
                'test_interface2']['install'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.terminate',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            test_relationship2['target_interfaces'][
                'test_interface2']['terminate'])
        self.assertEquals(
            2, len(test_relationship2['target_interfaces'][
                       'test_interface2']))
        self.assertEquals(6, len(test_relationship2))

        test_relationship3 = result['relationships']['test_relationship3']
        self.assertEquals('test_relationship3', test_relationship3['name'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.install',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            test_relationship3['target_interfaces'][
                'test_interface2']['install'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.terminate',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            test_relationship3['target_interfaces'][
                'test_interface2']['terminate'])
        self.assertEquals(
            2, len(test_relationship3['target_interfaces'][
                       'test_interface2']))
        self.assertEquals(5, len(test_relationship3))

    def test_top_level_relationship_properties(self):
        yaml = self.MINIMAL_BLUEPRINT + """
relationships:
    test_relationship:
        properties:
            without_default_value: {}
            with_simple_default_value:
                default: 1
            with_object_default_value:
                default:
                    comp1: 1
                    comp2: 2
        """
        result = self.parse(yaml)
        self._assert_minimal_blueprint(result)
        relationships = result['relationships']
        self.assertEquals(1, len(relationships))
        test_relationship = relationships['test_relationship']
        properties = test_relationship['properties']
        self.assertIn('without_default_value', properties)
        self.assertIn('with_simple_default_value', properties)
        self.assertEquals({'default': 1}, properties[
            'with_simple_default_value'])
        self.assertEquals({'default': {'comp1': 1, 'comp2': 2}}, properties[
            'with_object_default_value'])

    def test_top_level_relationship_properties_inheritance(self):
        yaml = self.MINIMAL_BLUEPRINT + """
relationships:
    test_relationship1:
        properties:
            prop1: {}
            prop2: {}
            prop3:
                default: prop3_value_1
            derived1:
                default: derived1_value
    test_relationship2:
        derived_from: test_relationship1
        properties:
            prop2:
                default: prop2_value_2
            prop3:
                default: prop3_value_2
            prop4: {}
            prop5: {}
            prop6:
                default: prop6_value_2
            derived2:
                default: derived2_value
    test_relationship3:
        derived_from: test_relationship2
        properties:
            prop5:
                default: prop5_value_3
            prop6:
                default: prop6_value_3
            prop7: {}
        """
        result = self.parse(yaml)
        self._assert_minimal_blueprint(result)
        relationships = result['relationships']
        self.assertEquals(3, len(relationships))
        r1_properties = relationships['test_relationship1']['properties']
        r2_properties = relationships['test_relationship2']['properties']
        r3_properties = relationships['test_relationship3']['properties']
        self.assertEquals(4, len(r1_properties))
        self.assertIn('prop1', r1_properties)
        self.assertIn('prop2', r1_properties)
        self.assertIn('prop3', r1_properties)
        self.assertIn('derived1', r1_properties)
        self.assertEquals({'default': 'prop3_value_1'}, r1_properties['prop3'])
        self.assertEquals({'default': 'derived1_value'}, r1_properties[
            'derived1'])
        self.assertEquals(8, len(r2_properties))
        self.assertIn('prop1', r2_properties)
        self.assertIn('prop2', r2_properties)
        self.assertIn('prop3', r2_properties)
        self.assertIn('prop4', r2_properties)
        self.assertIn('prop5', r2_properties)
        self.assertIn('prop6', r2_properties)
        self.assertIn('derived1', r2_properties)
        self.assertIn('derived2', r2_properties)
        self.assertEquals({'default': 'prop2_value_2'}, r2_properties[
            'prop2'])
        self.assertEquals({'default': 'prop3_value_2'}, r2_properties[
            'prop3'])
        self.assertEquals({'default': 'prop6_value_2'}, r2_properties[
            'prop6'])
        self.assertEquals({'default': 'derived1_value'}, r2_properties[
            'derived1'])
        self.assertEquals({'default': 'derived2_value'}, r2_properties[
            'derived2'])
        self.assertEquals(9, len(r3_properties))
        self.assertIn('prop1', r3_properties)
        self.assertIn('prop2', r3_properties)
        self.assertIn('prop3', r3_properties)
        self.assertIn('prop4', r3_properties)
        self.assertIn('prop5', r3_properties)
        self.assertIn('prop6', r3_properties)
        self.assertIn('prop7', r3_properties)
        self.assertIn('derived1', r3_properties)
        self.assertIn('derived2', r3_properties)
        self.assertEquals({'default': 'prop2_value_2'}, r3_properties[
            'prop2'])
        self.assertEquals({'default': 'prop3_value_2'}, r3_properties[
            'prop3'])
        self.assertEquals({'default': 'prop5_value_3'}, r3_properties[
            'prop5'])
        self.assertEquals({'default': 'prop6_value_3'}, r3_properties[
            'prop6'])
        self.assertEquals({'default': 'derived1_value'}, r3_properties[
            'derived1'])
        self.assertEquals({'default': 'derived2_value'}, r3_properties[
            'derived2'])

    def test_instance_relationships_empty_relationships_section(self):
        yaml = self.MINIMAL_BLUEPRINT + """
        relationships: []
        """
        result = self.parse(yaml)
        self._assert_minimal_blueprint(result)
        self.assertTrue(isinstance(result['nodes'][0]['relationships'], list))
        self.assertEqual(0, len(result['nodes'][0]['relationships']))

    def test_instance_relationships_standard_relationship(self):
        yaml = self.MINIMAL_BLUEPRINT + """
    test_node2:
        type: test_type
        relationships:
            -   type: test_relationship
                target: test_node
                source_interfaces:
                    test_interface1:
                        install: test_plugin.install
relationships:
    test_relationship: {}
plugins:
    test_plugin:
        executor: central_deployment_agent
        source: dummy
        """
        result = self.parse(yaml)
        self.assertEquals(2, len(result['nodes']))
        nodes = self._sort_result_nodes(result['nodes'], ['test_node',
                                                          'test_node2'])
        self.assertEquals('test_node2', nodes[1]['id'])
        self.assertEquals(1, len(nodes[1]['relationships']))
        relationship = nodes[1]['relationships'][0]
        self.assertEquals('test_relationship', relationship['type'])
        self.assertEquals('test_node', relationship['target_id'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.install', inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            relationship['source_interfaces']['test_interface1']['install'])
        relationship_source_operations = relationship['source_operations']
        self.assertEqual(op_struct('test_plugin', 'install',
                                   executor='central_deployment_agent'),
                         relationship_source_operations['install'])
        self.assertEqual(
            op_struct('test_plugin', 'install',
                      executor='central_deployment_agent'),
            relationship_source_operations['test_interface1.install'])
        self.assertEqual(2, len(relationship_source_operations))

        self.assertEquals(8, len(relationship))
        plugin_def = nodes[1]['plugins'][0]
        self.assertEquals('test_plugin', plugin_def['name'])

    def test_instance_relationships_duplicate_relationship(self):
        # right now, having two relationships with the same (type,target)
        # under one node is valid
        yaml = self.MINIMAL_BLUEPRINT + """
    test_node2:
        type: test_type
        relationships:
            -   type: test_relationship
                target: test_node
            -   type: test_relationship
                target: test_node
relationships:
    test_relationship: {}
        """
        result = self.parse(yaml)
        self.assertEquals(2, len(result['nodes']))
        nodes = self._sort_result_nodes(result['nodes'], ['test_node',
                                                          'test_node2'])
        self.assertEquals('test_node2', nodes[1]['id'])
        self.assertEquals(2, len(nodes[1]['relationships']))
        self.assertEquals('test_relationship',
                          nodes[1]['relationships'][0]['type'])
        self.assertEquals('test_relationship',
                          nodes[1]['relationships'][1]['type'])
        self.assertEquals('test_node',
                          nodes[1]['relationships'][0]['target_id'])
        self.assertEquals('test_node',
                          nodes[1]['relationships'][1]['target_id'])
        self.assertEquals(8, len(nodes[1]['relationships'][0]))
        self.assertEquals(8, len(nodes[1]['relationships'][1]))

    def test_instance_relationships_relationship_inheritance(self):
        # possibly 'inheritance' is the wrong term to use here,
        # the meaning is for checking that the relationship properties from the
        # top-level relationships
        # section are used for instance-relationships which declare their types
        # note there are no overrides in this case; these are tested in the
        # next, more thorough test
        yaml = self.MINIMAL_BLUEPRINT + """
    test_node2:
        type: test_type
        relationships:
            -   type: test_relationship
                target: test_node
                source_interfaces:
                    interface1:
                        op1: test_plugin.task_name1
relationships:
    relationship: {}
    test_relationship:
        derived_from: relationship
        target_interfaces:
            interface2:
                op2:
                    implementation: test_plugin.task_name2
                    inputs: {}
plugins:
    test_plugin:
        executor: central_deployment_agent
        source: dummy
        """
        result = self.parse(yaml)
        self.assertEquals(2, len(result['nodes']))
        nodes = self._sort_result_nodes(result['nodes'], ['test_node',
                                                          'test_node2'])
        relationship = nodes[1]['relationships'][0]
        self.assertEquals('test_relationship', relationship['type'])
        self.assertEquals('test_node', relationship['target_id'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.task_name1',
                              inputs={}, executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            relationship['source_interfaces']['interface1']['op1'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.task_name2',
                              inputs={}, executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            relationship['target_interfaces']['interface2']['op2'])

        rel_source_ops = relationship['source_operations']

        self.assertEqual(op_struct('test_plugin', 'task_name1',
                                   executor='central_deployment_agent'),
                         rel_source_ops['op1'])
        self.assertEqual(op_struct('test_plugin', 'task_name1',
                                   executor='central_deployment_agent'),
                         rel_source_ops['interface1.op1'])
        self.assertEquals(2, len(rel_source_ops))

        rel_target_ops = relationship['target_operations']
        self.assertEqual(op_struct('test_plugin', 'task_name2',
                                   executor='central_deployment_agent'),
                         rel_target_ops['op2'])
        self.assertEqual(op_struct('test_plugin', 'task_name2',
                                   executor='central_deployment_agent'),
                         rel_target_ops['interface2.op2'])
        self.assertEquals(2, len(rel_target_ops))

        self.assertEquals(8, len(relationship))

    def test_instance_relationship_properties_inheritance(self):
        yaml = self.MINIMAL_BLUEPRINT + """
    test_node2:
        type: test_type
        properties:
            key: "val"
        relationships:
            -   type: empty_relationship
                target: test_node
                properties:
                    prop1: prop1_value_new
                    prop2: prop2_value_new
                    prop7: prop7_value_new_instance
relationships:
    empty_relationship:
        properties:
            prop1: {}
            prop2: {}
            prop7: {}
        """
        result = self.parse(yaml)
        self.assertEquals(2, len(result['nodes']))
        nodes = self._sort_result_nodes(result['nodes'], ['test_node',
                                                          'test_node2'])
        relationships = result['relationships']
        self.assertEquals(1, len(relationships))
        i_properties = nodes[1]['relationships'][0]['properties']
        self.assertEquals(3, len(i_properties))
        self.assertEquals('prop1_value_new', i_properties['prop1'])
        self.assertEquals('prop2_value_new', i_properties['prop2'])
        self.assertEquals('prop7_value_new_instance', i_properties['prop7'])

    def test_relationships_and_node_recursive_inheritance(self):
        # testing for a complete inheritance path for relationships
        # from top-level relationships to a relationship instance
        yaml = self.MINIMAL_BLUEPRINT + """
    test_node2:
        type: test_type
        relationships:
            -   type: relationship
                target: test_node
                source_interfaces:
                    test_interface3:
                        install: test_plugin.install
                target_interfaces:
                    test_interface1:
                        install: test_plugin.install
relationships:
    relationship:
        derived_from: parent_relationship
        source_interfaces:
            test_interface2:
                install:
                    implementation: test_plugin.install
                    inputs: {}
                terminate:
                    implementation: test_plugin.terminate
                    inputs: {}
    parent_relationship:
        target_interfaces:
            test_interface3:
                install: {}
plugins:
    test_plugin:
        executor: central_deployment_agent
        source: dummy
        """
        result = self.parse(yaml)
        self.assertEquals(2, len(result['nodes']))
        nodes = self._sort_result_nodes(result['nodes'], ['test_node',
                                                          'test_node2'])
        node_relationship = nodes[1]['relationships'][0]
        relationship = result['relationships']['relationship']
        parent_relationship = result['relationships']['parent_relationship']
        self.assertEquals(2, len(result['relationships']))
        self.assertEquals(5, len(parent_relationship))
        self.assertEquals(6, len(relationship))
        self.assertEquals(8, len(node_relationship))

        self.assertEquals('parent_relationship', parent_relationship['name'])
        self.assertEquals(1, len(parent_relationship['target_interfaces']))
        self.assertEquals(1, len(parent_relationship['target_interfaces']
                                 ['test_interface3']))
        self.assertEquals(
            {'implementation': '', 'inputs': {}, 'executor': None,
             'max_retries': None, 'retry_interval': None, 'timeout': None,
             'timeout_recoverable': None},
            parent_relationship['target_interfaces'][
                'test_interface3']['install'])

        self.assertEquals('relationship', relationship['name'])
        self.assertEquals('parent_relationship', relationship['derived_from'])
        self.assertEquals(1, len(relationship['target_interfaces']))
        self.assertEquals(1, len(relationship['target_interfaces']
                                 ['test_interface3']))
        self.assertEquals(
            NO_OP,
            relationship['target_interfaces']['test_interface3']['install'])
        self.assertEquals(1, len(relationship['source_interfaces']))
        self.assertEquals(2, len(relationship['source_interfaces']
                                 ['test_interface2']))
        self.assertEqual(
            operation_mapping(implementation='test_plugin.install', inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            relationship['source_interfaces']['test_interface2']['install'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.terminate',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            relationship['source_interfaces'][
                'test_interface2']['terminate'])

        self.assertEquals('relationship', node_relationship['type'])
        self.assertEquals('test_node', node_relationship['target_id'])
        self.assertEquals(2, len(node_relationship['target_interfaces']))
        self.assertEquals(1, len(node_relationship['target_interfaces']
                                 ['test_interface3']))
        self.assertEquals(
            NO_OP,
            node_relationship['target_interfaces'][
                'test_interface3']['install'])
        self.assertEquals(1, len(node_relationship['target_interfaces']
                                 ['test_interface1']))
        self.assertEqual(
            operation_mapping(implementation='test_plugin.install', inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            node_relationship['target_interfaces'][
                'test_interface1']['install'])
        self.assertEquals(2, len(node_relationship['source_interfaces']))
        self.assertEquals(1, len(node_relationship['source_interfaces']
                                 ['test_interface3']))
        self.assertEquals(
            operation_mapping(implementation='test_plugin.install', inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            node_relationship['source_interfaces'][
                'test_interface2']['install'])
        self.assertEquals(2, len(node_relationship['source_interfaces']
                                 ['test_interface2']))
        self.assertEquals(
            operation_mapping(implementation='test_plugin.install', inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            node_relationship['source_interfaces'][
                'test_interface2']['install'])
        self.assertEquals(
            operation_mapping(implementation='test_plugin.terminate',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            node_relationship['source_interfaces'][
                'test_interface2']['terminate'])

        rel_source_ops = node_relationship['source_operations']
        self.assertEquals(4, len(rel_source_ops))
        self.assertEqual(op_struct('test_plugin', 'install',
                                   executor='central_deployment_agent'),
                         rel_source_ops['test_interface2.install'])
        self.assertEqual(op_struct('test_plugin', 'install',
                                   executor='central_deployment_agent'),
                         rel_source_ops['test_interface3.install'])
        self.assertEqual(op_struct('test_plugin', 'terminate',
                                   executor='central_deployment_agent'),
                         rel_source_ops['terminate'])
        self.assertEqual(op_struct('test_plugin', 'terminate',
                                   executor='central_deployment_agent'),
                         rel_source_ops['test_interface2.terminate'])

        rel_target_ops = node_relationship['target_operations']
        self.assertEquals(2, len(rel_target_ops))
        self.assertEqual(op_struct('', '', {}),
                         rel_target_ops['test_interface3.install'])
        self.assertEqual(op_struct('test_plugin', 'install',
                                   executor='central_deployment_agent'),
                         rel_target_ops['test_interface1.install'])

    def test_relationship_interfaces_inheritance_merge(self):
        # testing for a complete inheritance path for relationships
        # from top-level relationships to a relationship instance
        yaml = self.MINIMAL_BLUEPRINT + """
    test_node2:
        type: test_type
        relationships:
            -   type: relationship
                target: test_node
                target_interfaces:
                    test_interface:
                        destroy: test_plugin.destroy1
                source_interfaces:
                    test_interface:
                        install2: test_plugin.install2
                        destroy2: test_plugin.destroy2
relationships:
    parent_relationship:
        target_interfaces:
            test_interface:
                install: {}
        source_interfaces:
            test_interface:
                install2: {}
    relationship:
        derived_from: parent_relationship
        target_interfaces:
            test_interface:
                install:
                    implementation: test_plugin.install
                    inputs: {}
                terminate:
                    implementation: test_plugin.terminate
                    inputs: {}
        source_interfaces:
            test_interface:
                install2:
                    implementation: test_plugin.install
                    inputs: {}
                terminate2:
                    implementation: test_plugin.terminate
                    inputs: {}

plugins:
    test_plugin:
        executor: central_deployment_agent
        source: dummy
        """
        result = self.parse(yaml)
        self.assertEquals(2, len(result['nodes']))
        nodes = self._sort_result_nodes(result['nodes'], ['test_node',
                                                          'test_node2'])
        node_relationship = nodes[1]['relationships'][0]
        relationship = result['relationships']['relationship']
        parent_relationship = result['relationships']['parent_relationship']
        self.assertEquals(2, len(result['relationships']))
        self.assertEquals(5, len(parent_relationship))
        self.assertEquals(6, len(relationship))
        self.assertEquals(8, len(node_relationship))

        self.assertEquals('parent_relationship', parent_relationship['name'])
        self.assertEquals(1, len(parent_relationship['target_interfaces']))
        self.assertEquals(1, len(parent_relationship['target_interfaces']
                                 ['test_interface']))
        self.assertIn('install', parent_relationship['target_interfaces']
                      ['test_interface'])
        self.assertEquals(1, len(parent_relationship['source_interfaces']))
        self.assertEquals(1, len(parent_relationship['source_interfaces']
                                 ['test_interface']))
        self.assertIn('install2', parent_relationship[
            'source_interfaces']['test_interface'])

        self.assertEquals('relationship', relationship['name'])
        self.assertEquals('parent_relationship', relationship['derived_from'])
        self.assertEquals(1, len(relationship['target_interfaces']))
        self.assertEquals(2, len(relationship['target_interfaces']
                                 ['test_interface']))
        self.assertEqual(
            operation_mapping(implementation='test_plugin.install', inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            relationship['target_interfaces']['test_interface']['install'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.terminate',
                              inputs={}, executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            relationship['target_interfaces'][
                'test_interface']['terminate'])
        self.assertEquals(1, len(relationship['source_interfaces']))
        self.assertEquals(
            2, len(relationship['source_interfaces']['test_interface']))
        self.assertEqual(
            operation_mapping(implementation='test_plugin.install', inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            relationship['source_interfaces']['test_interface']['install2'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.terminate',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            relationship['source_interfaces'][
                'test_interface']['terminate2'])

        self.assertEquals('relationship', node_relationship['type'])
        self.assertEquals('test_node', node_relationship['target_id'])
        self.assertEquals(1, len(node_relationship['target_interfaces']))
        self.assertEquals(
            3, len(node_relationship['target_interfaces']['test_interface']))
        self.assertEqual(
            operation_mapping(implementation='test_plugin.install',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            node_relationship['target_interfaces'][
                'test_interface']['install'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.terminate',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            relationship['target_interfaces'][
                'test_interface']['terminate'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.destroy1',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            node_relationship['target_interfaces'][
                'test_interface']['destroy'])
        self.assertEquals(1, len(node_relationship['source_interfaces']))
        self.assertEquals(
            3, len(node_relationship['source_interfaces'][
                       'test_interface']))
        self.assertEquals(
            operation_mapping(implementation='test_plugin.install2',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            node_relationship['source_interfaces'][
                'test_interface']['install2'])
        self.assertEqual(
            operation_mapping(implementation='test_plugin.terminate',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            relationship['source_interfaces']['test_interface']['terminate2'])
        self.assertEquals(
            operation_mapping(implementation='test_plugin.destroy2',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None),
            node_relationship['source_interfaces'][
                'test_interface']['destroy2'])

        rel_source_ops = node_relationship['source_operations']
        self.assertEqual(op_struct('test_plugin', 'install2',
                                   executor='central_deployment_agent'),
                         rel_source_ops['install2'])
        self.assertEqual(op_struct('test_plugin', 'install2',
                                   executor='central_deployment_agent'),
                         rel_source_ops['test_interface.install2'])
        self.assertEqual(op_struct('test_plugin', 'terminate',
                                   executor='central_deployment_agent'),
                         rel_source_ops['terminate2'])
        self.assertEqual(op_struct('test_plugin', 'terminate',
                                   executor='central_deployment_agent'),
                         rel_source_ops['test_interface.terminate2'])
        self.assertEqual(op_struct('test_plugin', 'destroy2',
                                   executor='central_deployment_agent'),
                         rel_source_ops['destroy2'])
        self.assertEqual(op_struct('test_plugin', 'destroy2',
                                   executor='central_deployment_agent'),
                         rel_source_ops['test_interface.destroy2'])
        self.assertEquals(6, len(rel_source_ops))

        rel_target_ops = node_relationship['target_operations']
        self.assertEqual(op_struct('test_plugin', 'install',
                                   executor='central_deployment_agent'),
                         rel_target_ops['install'])
        self.assertEqual(op_struct('test_plugin', 'install',
                                   executor='central_deployment_agent'),
                         rel_target_ops['test_interface.install'])
        self.assertEqual(op_struct('test_plugin', 'terminate',
                                   executor='central_deployment_agent'),
                         rel_target_ops['terminate'])
        self.assertEqual(op_struct('test_plugin', 'terminate',
                                   executor='central_deployment_agent'),
                         rel_target_ops['test_interface.terminate'])
        self.assertEqual(op_struct('test_plugin', 'destroy1',
                                   executor='central_deployment_agent'),
                         rel_target_ops['destroy'])
        self.assertEqual(op_struct('test_plugin', 'destroy1',
                                   executor='central_deployment_agent'),
                         rel_target_ops['test_interface.destroy'])
        self.assertEquals(6, len(rel_source_ops))

    def test_relationship_no_type_hierarchy(self):
        yaml = self.MINIMAL_BLUEPRINT + """
    test_node2:
        type: test_type
        relationships:
            -   type: relationship
                target: test_node
relationships:
    relationship: {}
        """
        result = self.parse(yaml)
        self.assertEquals(2, len(result['nodes']))
        nodes = self._sort_result_nodes(result['nodes'], ['test_node',
                                                          'test_node2'])
        relationship = nodes[1]['relationships'][0]
        self.assertTrue('type_hierarchy' in relationship)
        type_hierarchy = relationship['type_hierarchy']
        self.assertEqual(1, len(type_hierarchy))
        self.assertEqual('relationship', type_hierarchy[0])

    def test_relationship_type_hierarchy(self):
        yaml = self.MINIMAL_BLUEPRINT + """
    test_node2:
        type: test_type
        relationships:
            -   type: rel2
                target: test_node
relationships:
    relationship: {}
    rel2:
        derived_from: relationship
        """
        result = self.parse(yaml)
        self.assertEquals(2, len(result['nodes']))
        nodes = self._sort_result_nodes(result['nodes'], ['test_node',
                                                          'test_node2'])
        relationship = nodes[1]['relationships'][0]
        self.assertTrue('type_hierarchy' in relationship)
        type_hierarchy = relationship['type_hierarchy']
        self.assertEqual(2, len(type_hierarchy))
        self.assertEqual('relationship', type_hierarchy[0])
        self.assertEqual('rel2', type_hierarchy[1])

    def test_relationship_3_types_hierarchy(self):
        yaml = self.MINIMAL_BLUEPRINT + """
    test_node2:
        type: test_type
        relationships:
            -   type: rel3
                target: test_node
relationships:
    relationship: {}
    rel2:
        derived_from: relationship
    rel3:
        derived_from: rel2
        """
        result = self.parse(yaml)
        self.assertEquals(2, len(result['nodes']))
        nodes = self._sort_result_nodes(result['nodes'], ['test_node',
                                                          'test_node2'])
        relationship = nodes[1]['relationships'][0]
        self.assertTrue('type_hierarchy' in relationship)
        type_hierarchy = relationship['type_hierarchy']
        self.assertEqual(3, len(type_hierarchy))
        self.assertEqual('relationship', type_hierarchy[0])
        self.assertEqual('rel2', type_hierarchy[1])
        self.assertEqual('rel3', type_hierarchy[2])


class TestRelationshipsBasics(BaseParserApiTest):
    def test_relationship_type_properties_empty_properties(self):
        yaml = """
node_templates:
    test_node:
        type: test_type
node_types:
    test_type: {}
relationships:
    test_relationship:
        properties: {}
"""
        result = self.parse(yaml)
        self.assertEquals(1, len(result['nodes']))
        node = result['nodes'][0]
        self.assertEquals('test_node', node['id'])
        self.assertEquals('test_type', node['type'])
        relationship = result['relationships']['test_relationship']
        self.assertEquals({}, relationship['properties'])

    def test_relationship_type_properties_empty_property(self):
        yaml = self.MINIMAL_BLUEPRINT + """
relationships:
    test_relationship:
        properties:
            key: {}
"""
        result = self.parse(yaml)
        self.assertEquals(1, len(result['nodes']))
        node = result['nodes'][0]
        self.assertEquals('test_node', node['id'])
        self.assertEquals('test_type', node['type'])
        relationship = result['relationships']['test_relationship']
        self.assertEquals({'key': {}}, relationship['properties'])

    def test_relationship_type_properties_property_with_description_only(self):
        yaml = self.MINIMAL_BLUEPRINT + """
relationships:
    test_relationship:
        properties:
            key:
                description: property_desc
"""
        result = self.parse(yaml)
        self.assertEquals(1, len(result['nodes']))
        node = result['nodes'][0]
        self.assertEquals('test_node', node['id'])
        self.assertEquals('test_type', node['type'])
        relationship = result['relationships']['test_relationship']
        self.assertEquals({'key': {'description': 'property_desc'}},
                          relationship['properties'])

    def test_relationship_type_properties_standard_property(self):
        yaml = self.MINIMAL_BLUEPRINT + """
relationships:
    test_relationship:
        properties:
            key:
                default: val
                description: property_desc
                type: string
"""
        result = self.parse(yaml)
        self.assertEquals(1, len(result['nodes']))
        node = result['nodes'][0]
        self.assertEquals('test_node', node['id'])
        self.assertEquals('test_type', node['type'])
        relationship = result['relationships']['test_relationship']
        self.assertEquals(
            {'key': {'default': 'val', 'description': 'property_desc',
                     'type': 'string'}},
            relationship['properties'])


class TestDependsOnLifecycleOperation(BaseParserApiTest):
    basic_yaml = """
tosca_definitions_version: cloudify_dsl_1_3

plugins:
    test_plugin:
        executor: central_deployment_agent
        source: dummy

node_types:
      basic_type:
        interfaces:
          cloudify.interfaces.lifecycle:
            precreate: {}
            create: {}
            configure: {}

relationships:
    cloudify.relationships.depends_on_lifecycle_operation:
        properties:
          operation:
            type: string
            default: precreate
"""

    def test_valid_relationship_use(self):
        yaml = self.basic_yaml + """
node_templates:
    node:
        type: basic_type
        interfaces:
          cloudify.interfaces.lifecycle:
            create: test_plugin.do_nothing
            configure: test_plugin.do_nothing

    depends:
        type: basic_type
        relationships:
          - type: cloudify.relationships.depends_on_lifecycle_operation
            target: node
            properties:
              operation: create
"""
        parsed = self.parse(yaml)
        relationships = parsed['relationships']
        relationship = relationships[
            'cloudify.relationships.depends_on_lifecycle_operation']
        self.assertEquals(
            {'operation':
                {'default': 'precreate',
                 'type': 'string'}},
            relationship['properties'])

    def test_not_defined_lifecycle_operation_target(self):
        yaml = self.basic_yaml + """
node_templates:
    node:
        type: basic_type
        interfaces:
          cloudify.interfaces.lifecycle:
            configure: test_plugin.do_nothing

    depends:
        type: basic_type
        relationships:
          - type: cloudify.relationships.depends_on_lifecycle_operation
            target: node
            properties:
              operation: create
"""
        self.assertRaises(DSLParsingLogicException, self.parse, yaml)

    def test_not_defined_interfaces_in_target(self):
        yaml = self.basic_yaml + """
node_templates:
    node:
        type: basic_type

    depends:
        type: basic_type
        relationships:
          - type: cloudify.relationships.depends_on_lifecycle_operation
            target: node
            properties:
              operation: create
"""
        self.assertRaises(DSLParsingLogicException, self.parse, yaml)

    def test_not_existing_operation_target(self):
        yaml = self.basic_yaml + """
node_templates:
    node:
        type: basic_type
        interfaces:
          cloudify.interfaces.lifecycle:
            configure: test_plugin.do_nothing

    depends:
        type: basic_type
        relationships:
          - type: cloudify.relationships.depends_on_lifecycle_operation
            target: node
            properties:
              operation: not_existing
"""
        self.assertRaises(DSLParsingLogicException, self.parse, yaml)


class TestRelationshipInputValidation(BaseParserApiTest):
    def test_valid_input_types(self):
        yaml = """
relationships:
    test_relationship:
        properties:
            integer:
                type: integer
                default: 1
            list:
                type: list
                default: [1, 2]
            float:
                type: float
                default: 1.5
            string:
                type: string
                default: test
            regex:
                type: regex
                default: ^.$
            boolean:
                type: boolean
                default: false
        """
        parsed = self.parse(yaml)
        relationships = parsed['relationships']
        self.assertEquals(1, len(relationships))
        test_relationship = relationships['test_relationship']
        properties = test_relationship['properties']
        self.assertEqual(1, properties['integer']['default'])
        self.assertEqual([1, 2], properties['list']['default'])
        self.assertEqual(1.5, properties['float']['default'])
        self.assertEqual('test', properties['string']['default'])
        self.assertEqual('^.$', properties['regex']['default'])
        self.assertEqual(False, properties['boolean']['default'])

    def test_not_valid_input_type(self):
        yaml = """
relationships:
    test_relationship:
        properties:
            not_valid:
                type: test
                default: 1
        """
        self.assertRaises(DSLParsingLogicException, self.parse, yaml)
