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


# TODO: this code already exists
def workflow_op_struct(plugin_name,
                       mapping,
                       parameters=None):

    if not parameters:
        parameters = {}
    return {
        'plugin': plugin_name,
        'operation': mapping,
        'parameters': parameters
    }


class TestGeneralNamespacedWorkflows(AbstractTestParser):
    def test_basic_namespace_multi_import(self):
        imported_yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
workflows:
    workflow1: test_plugin.workflow1"""

        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
    -   {2}->{1}
""".format('test', import_file_name, 'other_test')
        parsed = self.parse_1_3(main_yaml)
        workflows = parsed[constants.WORKFLOWS]
        self.assertEqual(2, len(workflows))
        self.assertEqual(workflow_op_struct('test->test_plugin', 'workflow1'),
                         workflows['test->workflow1'])
        self.assertEqual(
            workflow_op_struct('other_test->test_plugin', 'workflow1'),
            workflows['other_test->workflow1'])
        workflow_plugins_to_install =\
            parsed[constants.WORKFLOW_PLUGINS_TO_INSTALL]
        self.assertEqual(2, len(workflow_plugins_to_install))
        self.assertEqual('test->test_plugin',
                         workflow_plugins_to_install[0]['name'])
        self.assertEqual('other_test->test_plugin',
                         workflow_plugins_to_install[1]['name'])

    def test_workflow_collision(self):
        imported_yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
workflows:
    workflow1: test_plugin.workflow1"""

        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
imports:
    -   {0}->{1}
workflows:
    workflow1: test_plugin.workflow1
""".format('test', import_file_name, 'other_test')
        parsed = self.parse_1_3(main_yaml)
        workflows = parsed[constants.WORKFLOWS]
        self.assertEqual(2, len(workflows))
        self.assertEqual(workflow_op_struct('test->test_plugin', 'workflow1'),
                         workflows['test->workflow1'])
        self.assertEqual(
            workflow_op_struct('test_plugin', 'workflow1'),
            workflows['workflow1'])
        workflow_plugins_to_install = \
            parsed[constants.WORKFLOW_PLUGINS_TO_INSTALL]
        self.assertEqual(2, len(workflow_plugins_to_install))
        self.assertEqual('test->test_plugin',
                         workflow_plugins_to_install[0]['name'])
        self.assertEqual('test_plugin',
                         workflow_plugins_to_install[1]['name'])

    def test_imports_merging_with_no_collision(self):
        imported_yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
workflows:
    workflow1: test_plugin.workflow1"""

        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
imports:
    -   {0}->{1}
workflows:
    workflow2: test_plugin.workflow2
""".format('test', import_file_name, 'other_test')
        parsed = self.parse_1_3(main_yaml)
        workflows = parsed[constants.WORKFLOWS]
        self.assertEqual(2, len(workflows))
        self.assertEqual(workflow_op_struct('test->test_plugin', 'workflow1'),
                         workflows['test->workflow1'])
        self.assertEqual(
            workflow_op_struct('test_plugin', 'workflow2'),
            workflows['workflow2'])
        workflow_plugins_to_install = \
            parsed[constants.WORKFLOW_PLUGINS_TO_INSTALL]
        self.assertEqual(2, len(workflow_plugins_to_install))
        self.assertEqual('test->test_plugin',
                         workflow_plugins_to_install[0]['name'])
        self.assertEqual('test_plugin',
                         workflow_plugins_to_install[1]['name'])


class TestDetailNamespacedWorkflows(AbstractTestParser):
    def test_workflow_basic_mapping(self):
        imported_yaml = self.BLUEPRINT_WITH_INTERFACES_AND_PLUGINS + """
workflows:
    workflow1: test_plugin.workflow1"""

        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed = self.parse(main_yaml)
        workflows = parsed[constants.WORKFLOWS]
        self.assertEqual(1, len(workflows))
        self.assertEqual(workflow_op_struct('test->test_plugin', 'workflow1',),
                         workflows['test->workflow1'])
        workflow_plugins_to_install =\
            parsed[constants.WORKFLOW_PLUGINS_TO_INSTALL]
        self.assertEqual(1, len(workflow_plugins_to_install))
        self.assertEqual('test->test_plugin',
                         workflow_plugins_to_install[0]['name'])

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
    -   {0}->{1}
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
        self.assertEqual(workflow_op_struct('test->test_plugin',
                                            'workflow1',
                                            parameters),
                         workflows['test->workflow1'])
        workflow_plugins_to_install =\
            parsed[constants.WORKFLOW_PLUGINS_TO_INSTALL]
        self.assertEqual(1, len(workflow_plugins_to_install))
        self.assertEqual('test->test_plugin',
                         workflow_plugins_to_install[0]['name'])
