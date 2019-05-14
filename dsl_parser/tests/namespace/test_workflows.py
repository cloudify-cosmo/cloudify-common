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
from dsl_parser.tests.utils import ResolverWithBlueprintSupport as Resolver


def workflow_op_struct(plugin_name,
                       mapping,
                       parameters=None,
                       is_cascading=True):
    if not parameters:
        parameters = {}
    return {
        'plugin': plugin_name,
        'operation': mapping,
        'parameters': parameters,
        'is_cascading': is_cascading
    }


class TestNamespacedWorkflows(AbstractTestParser):

    def basic_blueprint(self):
        return self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
workflows:
    workflow1: test_plugin.workflow1
"""

    def test_basic_namespace_multi_import(self):
        imported_yaml = self.basic_blueprint()
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
    -   {2}--{1}
""".format('test', import_file_name, 'other_test')
        parsed = self.parse_1_3(main_yaml)
        workflows = parsed[constants.WORKFLOWS]
        self.assertEqual(2, len(workflows))
        self.assertEqual(workflow_op_struct('test--test_plugin', 'workflow1'),
                         workflows['test--workflow1'])
        self.assertEqual(
            workflow_op_struct('other_test--test_plugin', 'workflow1'),
            workflows['other_test--workflow1'])
        workflow_plugins_to_install =\
            parsed[constants.WORKFLOW_PLUGINS_TO_INSTALL]
        self.assertEqual(2, len(workflow_plugins_to_install))
        self.assertEqual('test--test_plugin',
                         workflow_plugins_to_install[1]['name'])
        self.assertEqual('other_test--test_plugin',
                         workflow_plugins_to_install[0]['name'])

    def test_workflow_collision(self):
        imported_yaml = self.basic_blueprint()
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
imports:
    -   {0}--{1}
workflows:
    workflow1: test_plugin.workflow1
""".format('test', import_file_name, 'other_test')
        parsed = self.parse_1_3(main_yaml)
        workflows = parsed[constants.WORKFLOWS]
        self.assertEqual(2, len(workflows))
        self.assertEqual(workflow_op_struct('test--test_plugin', 'workflow1'),
                         workflows['test--workflow1'])
        self.assertEqual(
            workflow_op_struct('test_plugin', 'workflow1'),
            workflows['workflow1'])
        workflow_plugins_to_install = \
            parsed[constants.WORKFLOW_PLUGINS_TO_INSTALL]
        self.assertEqual(2, len(workflow_plugins_to_install))
        self.assertItemsEqual(['test--test_plugin', 'test_plugin'],
                              [workflow_plugins_to_install[1]['name'],
                               workflow_plugins_to_install[0]['name']])

    def test_workflows_merging_with_no_collision(self):
        imported_yaml = self.basic_blueprint()
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
imports:
    -   {0}--{1}
workflows:
    workflow2: test_plugin.workflow2
""".format('test', import_file_name, 'other_test')
        parsed = self.parse_1_3(main_yaml)
        workflows = parsed[constants.WORKFLOWS]
        self.assertEqual(2, len(workflows))
        self.assertEqual(workflow_op_struct('test--test_plugin', 'workflow1'),
                         workflows['test--workflow1'])
        self.assertEqual(
            workflow_op_struct('test_plugin', 'workflow2'),
            workflows['workflow2'])
        workflow_plugins_to_install = \
            parsed[constants.WORKFLOW_PLUGINS_TO_INSTALL]
        self.assertEqual(2, len(workflow_plugins_to_install))
        self.assertEqual('test--test_plugin',
                         workflow_plugins_to_install[0]['name'])
        self.assertEqual('test_plugin',
                         workflow_plugins_to_install[1]['name'])

    def test_workflow_basic_mapping(self):
        imported_yaml = self.basic_blueprint()
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
""".format('test', import_file_name)

        parsed = self.parse(main_yaml)
        workflows = parsed[constants.WORKFLOWS]
        self.assertEqual(1, len(workflows))
        self.assertEqual(workflow_op_struct('test--test_plugin', 'workflow1',),
                         workflows['test--workflow1'])
        workflow_plugins_to_install =\
            parsed[constants.WORKFLOW_PLUGINS_TO_INSTALL]
        self.assertEqual(1, len(workflow_plugins_to_install))
        self.assertEqual('test--test_plugin',
                         workflow_plugins_to_install[0]['name'])

    def test_workflow_local_namespaced_operation(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_0 + """
plugins:
    script:
        executor: central_deployment_agent
        install: false

workflows:
    workflow: stub.py
    workflow2:
        mapping: stub.py
        parameters:
            key:
                default: value
        is_cascading: true
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
        parsed = self.parse_from_path(main_yaml_path)
        workflow = parsed[constants.WORKFLOWS]['test--workflow']
        workflow2 = parsed[constants.WORKFLOWS]['test--workflow2']
        namespaced_script_plugin = 'test--' + constants.SCRIPT_PLUGIN_NAME
        self.assertEqual(workflow['operation'],
                         constants.SCRIPT_PLUGIN_EXECUTE_WORKFLOW_TASK)
        self.assertEqual(1, len(workflow['parameters']))
        self.assertEqual(workflow['parameters']['script_path']['default'],
                         'test--stub.py')
        self.assertEqual(workflow['plugin'], namespaced_script_plugin)

        self.assertEqual(workflow2['operation'],
                         constants.SCRIPT_PLUGIN_EXECUTE_WORKFLOW_TASK)
        self.assertEqual(2, len(workflow2['parameters']))
        self.assertEqual(workflow2['parameters']['script_path']['default'],
                         'test--stub.py')
        self.assertEqual(workflow2['parameters']['key']['default'], 'value')
        self.assertEqual(workflow['plugin'], namespaced_script_plugin)
        self.assertEqual(True, workflow2['is_cascading'])
        self.assertEqual(True, workflow['is_cascading'])

    def test_workflow_blueprint_import_namespaced_operation(self):
        imported_yaml = self.BASIC_VERSION_SECTION_DSL_1_0 + """
plugins:
    script:
        executor: central_deployment_agent
        install: false

workflows:
    workflow: stub.py
    workflow2:
        mapping: stub.py
        parameters:
            key:
                default: value
        is_cascading: false
"""
        import_base_path = self._temp_dir + '/test'
        self.make_file_with_name(content='content',
                                 filename='stub.py',
                                 base_dir=import_base_path)
        import_path = self.make_file_with_name(content=imported_yaml,
                                               filename='test.yaml',
                                               base_dir=import_base_path)
        resolver = Resolver({'blueprint:test': (imported_yaml, import_path)},
                            import_base_path)
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_0 + """
imports:
- test--blueprint:test
"""
        main_base_path = self._temp_dir + '/main'
        main_yaml_path = self.make_file_with_name(content=main_yaml,
                                                  filename='blueprint.yaml',
                                                  base_dir=main_base_path)
        parsed = self.parse_from_path(main_yaml_path,
                                      resolver=resolver,
                                      resources_base_path=main_base_path)
        workflow = parsed[constants.WORKFLOWS]['test--workflow']
        workflow2 = parsed[constants.WORKFLOWS]['test--workflow2']
        namespaced_script_plugin = 'test--' + constants.SCRIPT_PLUGIN_NAME
        self.assertEqual(workflow['operation'],
                         constants.SCRIPT_PLUGIN_EXECUTE_WORKFLOW_TASK)
        self.assertEqual(1, len(workflow['parameters']))
        self.assertEqual(workflow['parameters']['script_path']['default'],
                         'test--stub.py')
        self.assertEqual(workflow['plugin'], namespaced_script_plugin)

        self.assertEqual(workflow2['operation'],
                         constants.SCRIPT_PLUGIN_EXECUTE_WORKFLOW_TASK)
        self.assertEqual(2, len(workflow2['parameters']))
        self.assertEqual(workflow2['parameters']['script_path']['default'],
                         'test--stub.py')
        self.assertEqual(workflow2['parameters']['key']['default'], 'value')
        self.assertEqual(workflow['plugin'], namespaced_script_plugin)
        self.assertEqual(False, workflow2['is_cascading'])
        self.assertEqual(True, workflow['is_cascading'])

    def test_workflow_advanced_mapping(self):
        imported_yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
workflows:
    workflow1:
        mapping: test_plugin.workflow1
        parameters:
            prop1:
                default: value1
            mandatory_prop: {}
            nested_prop:
                default:
                    nested_key: nested_value
                    nested_list:
                        - val1
                        - val2
"""
        import_file_name = self.make_yaml_file(imported_yaml)
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
""".format('test', import_file_name)

        parsed = self.parse(main_yaml)
        workflows = parsed[constants.WORKFLOWS]
        self.assertEqual(1, len(workflows))
        parameters = {
            'prop1': {'default': 'value1'},
            'mandatory_prop': {},
            'nested_prop': {
                'default': {
                    'nested_key': 'nested_value',
                    'nested_list': [
                        'val1',
                        'val2'
                    ]
                }
            }
        }
        self.assertEqual(workflow_op_struct('test--test_plugin',
                                            'workflow1',
                                            parameters),
                         workflows['test--workflow1'])
        workflow_plugins_to_install =\
            parsed[constants.WORKFLOW_PLUGINS_TO_INSTALL]
        self.assertEqual(1, len(workflow_plugins_to_install))
        self.assertEqual('test--test_plugin',
                         workflow_plugins_to_install[0]['name'])
