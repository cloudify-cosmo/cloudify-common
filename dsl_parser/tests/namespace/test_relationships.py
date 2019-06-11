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
from dsl_parser.interfaces.utils import operation_mapping
from dsl_parser.interfaces import constants as interfaces_const
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestNamespacedRelationsships(AbstractTestParser):
    def test_basic_namespaced_relationship(self):
        imported_yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
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
                    inputs:
                        key:
                            type: string
                            default: value
                    executor: central_deployment_agent
                    max_retries: 5
                    retry_interval: 6
                test_interface4_op2: test_plugin.task_name
        """
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse_1_3(main_yaml)
        test_relationship = (parsed_yaml[constants.RELATIONSHIPS]
                             ['test--test_relationship'])
        self.assertEquals('test--test_relationship', test_relationship['name'])
        self.assertEquals(test_relationship[constants.TYPE_HIERARCHY],
                          ['test--empty_rel', 'test--test_relationship'])
        result_test_interface_3 = \
            (test_relationship[interfaces_const.SOURCE_INTERFACES]
             ['test_interface3'])
        self.assertEquals(interfaces_const.NO_OP,
                          result_test_interface_3['test_interface3_op1'])
        result_test_interface_4 = \
            (test_relationship[interfaces_const.TARGET_INTERFACES]
             ['test_interface4'])
        self.assertEquals(
            operation_mapping(implementation='test--test_plugin.task_name',
                              inputs={'key':
                                      {'default': 'value',
                                       'type': 'string'}},
                              executor='central_deployment_agent',
                              max_retries=5,
                              retry_interval=6,
                              timeout=None,
                              timeout_recoverable=None,
                              timeout_error=None),
            result_test_interface_4['test_interface4_op1'])
        self.assertEquals(
            operation_mapping(implementation='test--test_plugin.task_name',
                              inputs={},
                              executor=None,
                              max_retries=None,
                              retry_interval=None,
                              timeout=None,
                              timeout_recoverable=None,
                              timeout_error=None),
            result_test_interface_4['test_interface4_op2'])

    def test_basic_namespace_multi_import(self):
        imported_yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
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
                    inputs:
                        key:
                            type: string
                            default: value
                    executor: central_deployment_agent
                    max_retries: 5
                    retry_interval: 6
        """
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
    -   {2}--{1}
""".format('test', import_file_name, 'other_test')
        parsed_yaml = self.parse_1_3(main_yaml)
        test_relationship = (parsed_yaml[constants.RELATIONSHIPS]
                             ['test--test_relationship'])
        self.assertEquals('test--test_relationship', test_relationship['name'])
        self.assertEquals(test_relationship[constants.TYPE_HIERARCHY],
                          ['test--empty_rel', 'test--test_relationship'])
        result_test_interface_3 = \
            (test_relationship[interfaces_const.SOURCE_INTERFACES]
             ['test_interface3'])
        self.assertEquals(interfaces_const.NO_OP,
                          result_test_interface_3['test_interface3_op1'])
        result_test_interface_4 = \
            (test_relationship[interfaces_const.TARGET_INTERFACES]
             ['test_interface4'])
        self.assertEquals(
            operation_mapping(implementation='test--test_plugin.task_name',
                              inputs={'key':
                                      {'default': 'value',
                                       'type': 'string'}},
                              executor='central_deployment_agent',
                              max_retries=5,
                              retry_interval=6,
                              timeout=None,
                              timeout_recoverable=None,
                              timeout_error=None),
            result_test_interface_4['test_interface4_op1'])
        test_relationship = (parsed_yaml[constants.RELATIONSHIPS]
                             ['other_test--test_relationship'])
        self.assertEquals('other_test--test_relationship',
                          test_relationship['name'])
        self.assertEquals(test_relationship[constants.TYPE_HIERARCHY],
                          ['other_test--empty_rel',
                           'other_test--test_relationship'])
        result_test_interface_3 = \
            (test_relationship[interfaces_const.SOURCE_INTERFACES]
                ['test_interface3'])
        self.assertEquals(interfaces_const.NO_OP,
                          result_test_interface_3['test_interface3_op1'])
        result_test_interface_4 = \
            (test_relationship[interfaces_const.TARGET_INTERFACES]
                ['test_interface4'])
        self.assertEquals(
            operation_mapping(
                implementation='other_test--test_plugin.task_name',
                inputs={'key':
                        {'default': 'value',
                         'type': 'string'}},
                executor='central_deployment_agent',
                max_retries=5,
                retry_interval=6,
                timeout=None,
                timeout_recoverable=None,
                timeout_error=None),
            result_test_interface_4['test_interface4_op1'])

    def test_relationship_collision(self):
        imported_yaml = self.MINIMAL_BLUEPRINT + """
relationships:
    test_relationship:
        properties:
            prop:
                default: 1
"""
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = """
imports:
  - {0}--{1}
relationships:
    test_relationship:
        properties:
            prop:
                default: 2
""".format('test', import_file_name)
        parsed = self.parse(main_yaml)
        relationships = parsed[constants.RELATIONSHIPS]
        self.assertEquals(2, len(relationships))
        test_relationship = relationships['test_relationship']
        properties = test_relationship[constants.PROPERTIES]
        self.assertEquals({'default': 2}, properties[
            'prop'])
        test_relationship = relationships['test--test_relationship']
        properties = test_relationship[constants.PROPERTIES]
        self.assertEquals({'default': 1}, properties[
            'prop'])

    def test_relationships_merging_with_no_collision(self):
        imported_yaml = self.MINIMAL_BLUEPRINT + """
relationships:
    test_relationship1:
        properties:
            prop:
                default: 1
"""
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = """
imports:
  - {0}--{1}
relationships:
    test_relationship2:
        properties:
            prop:
                default: 2
""".format('test', import_file_name)
        parsed = self.parse(main_yaml)
        relationships = parsed[constants.RELATIONSHIPS]
        self.assertEquals(2, len(relationships))
        test_relationship = relationships['test_relationship2']
        properties = test_relationship[constants.PROPERTIES]
        self.assertEquals({'default': 2}, properties[
            'prop'])
        test_relationship = relationships['test--test_relationship1']
        properties = test_relationship[constants.PROPERTIES]
        self.assertEquals({'default': 1}, properties[
            'prop'])

    def test_namespaced_script_plugin_use(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_0 + """
plugins:
    script:
        executor: central_deployment_agent
        install: false
relationships:
    relationship:
        source_interfaces:
            test:
                op:
                    implementation: stub.py
                    inputs: {}
                op2:
                    implementation: stub.py
                    inputs:
                        key:
                            default: value
        target_interfaces:
            test:
                op:
                    implementation: stub.py
                    inputs: {}
"""
        self.make_file_with_name(content='content',
                                 filename='stub.py')
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_0 + """
imports:
  - {0}--{1}
""".format('test', import_file_name)
        main_yaml_path = self.make_file_with_name(content=main_yaml,
                                                  filename='blueprint.yaml')
        parsed_yaml = self.parse_from_path(main_yaml_path)
        relationship =\
            parsed_yaml[constants.RELATIONSHIPS]['test--relationship']

        source_operation =\
            relationship[interfaces_const.SOURCE_INTERFACES]['test']
        target_operation =\
            relationship[interfaces_const.TARGET_INTERFACES]['test']

        def assert_operation(op, extra_properties=False):
            inputs = {}
            if extra_properties:
                inputs.update({'key': {'default': 'value'}})
            self.assertEqual(op, operation_mapping(
                implementation='test--stub.py',
                inputs=inputs,
                executor=None,
                max_retries=None,
                retry_interval=None,
                timeout=None,
                timeout_recoverable=None,
                timeout_error=None
            ))

        assert_operation(source_operation['op'])
        assert_operation(source_operation['op2'], True)
        assert_operation(target_operation['op'])

    def test_partial_path_to_script(self):
        imported_yaml = self.MINIMAL_BLUEPRINT + """
plugins:
    script:
        executor: central_deployment_agent
        install: false
node_types:
    test_type:
        properties:
            key:
                default: 'default'
node_templates:
  vm:
    type: cloudify.nodes.Compute
  http_web_server:
    type: test_type
    relationships:
      - type: cloudify.relationships.contained_in
        target: vm
    interfaces:
      cloudify.interfaces.lifecycle:
        start: scripts/start.sh
"""
        self.make_file_with_name(content='content',
                                 filename='start.sh',
                                 base_dir='scripts')
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
    -   {0}--{1}
""".format('test', import_file_name)
        main_yaml_path = self.make_file_with_name(content=main_yaml,
                                                  filename='blueprint.yaml')
        parsed_yaml = self.parse_from_path(main_yaml_path)
        http_node = parsed_yaml[constants.NODES][1]
        start_op = http_node['operations']['start']
        start_op_script_path = start_op['inputs']['script_path']
        self.assertEqual('test--scripts/start.sh', start_op_script_path)
