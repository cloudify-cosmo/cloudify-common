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
from dsl_parser.tests import scaling
from dsl_parser.tests.abstract_test_parser import AbstractTestParser
from dsl_parser.tests.test_parser_api import op_struct


class TestDetailNodeTemplateNamespaceImport(AbstractTestParser):
    def test_deploy(self):
        imported_yaml = self.MINIMAL_BLUEPRINT + """
        instances:
            deploy: 2
            """
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        self.assertEquals(1, len(parsed_yaml['nodes']))
        node = parsed_yaml['nodes'][0]
        self.assertEquals('test->test_node', node['id'])
        self.assertEquals('test->test_type', node['type'])
        self.assertEquals('val', node[constants.PROPERTIES]['key'])
        self.assertEquals(2, node['instances']['deploy'])

    def test_namespaced_script_plugin_interface_use(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_0 + """
plugins:
    script:
        executor: central_deployment_agent
        install: false

node_types:
    type:
        interfaces:
            test:
                op:
                    implementation: stub.py
                    inputs: {}
                op2:
                    implementation: stub.py
                    inputs:
                        key:
                            default: value
relationships:
    relationship:
        source_interfaces:
            test:
                op:
                    implementation: stub.py
                    inputs: {}
        target_interfaces:
            test:
                op:
                    implementation: stub.py
                    inputs: {}
node_templates:
    node1:
        type: type
        relationships:
            -   target: node2
                type: relationship
    node2:
        type: type

"""
        self.make_file_with_name(content='content',
                                 filename='stub.py')
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_0 + """
imports:
- {0}->{1}
""".format('test', import_file_name)
        main_yaml_path = self.make_file_with_name(content=main_yaml,
                                                  filename='blueprint.yaml')
        result = self.parse_from_path(main_yaml_path)
        node = [n for n in result['nodes'] if n['name'] == 'test->node1'][0]
        relationship = node['relationships'][0]

        operation = node['operations']['test.op']
        operation2 = node['operations']['test.op2']
        source_operation = relationship['source_operations']['test.op']
        target_operation = relationship['target_operations']['test.op']

        def assert_operation(op, extra_properties=False):
            inputs = {'script_path': 'test->stub.py'}
            if extra_properties:
                inputs.update({'key': 'value'})
            self.assertEqual(op, op_struct(
                plugin_name='{0}->{1}'.format('test',
                                              constants.SCRIPT_PLUGIN_NAME),
                mapping=constants.SCRIPT_PLUGIN_RUN_TASK,
                inputs=inputs,
                executor='central_deployment_agent'))

        assert_operation(operation)
        assert_operation(operation2, extra_properties=True)
        assert_operation(source_operation)
        assert_operation(target_operation)


class TestNamespacedMultiInstance(scaling.BaseTestMultiInstance):
    def test_scalable(self):
        imported_yaml = self.BASE_BLUEPRINT + """
    host:
        type: cloudify.nodes.Compute
        capabilities:
            scalable:
                properties:
                    default_instances: 2
                    min_instances: 1
                    max_instances: 10
"""

        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        multi_plan = self.parse_multi(main_yaml)
        nodes_instances = multi_plan[constants.NODE_INSTANCES]
        self.assertEquals(2, len(nodes_instances))
        self.assertEquals(2, len(set(self._node_ids(nodes_instances))))

        self.assertIn('host_', nodes_instances[0]['id'])
        self.assertIn('host_', nodes_instances[1]['id'])
        self.assertEquals(nodes_instances[0]['id'],
                          nodes_instances[0]['host_id'])
        self.assertEquals(nodes_instances[1]['id'],
                          nodes_instances[1]['host_id'])
        node = multi_plan[constants.NODES][0]
        node_props = node[constants.CAPABILITIES]['scalable']['properties']
        self.assertEqual(1, node_props['min_instances'])
        self.assertEqual(10, node_props['max_instances'])
        self.assertEqual(2, node_props['current_instances'])
        self.assertEqual(2, node_props['planned_instances'])
        self.assertEqual(2, node_props['default_instances'])
