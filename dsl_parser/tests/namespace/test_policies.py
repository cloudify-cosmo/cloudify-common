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

from dsl_parser import constants, exceptions
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestNamespacedPolicies(AbstractTestParser):
    def _assert_policy(self,
                       policy,
                       targets,
                       policy_type,
                       min_instances,
                       max_instances,
                       default_instances):
        self.assertEqual(policy['targets'], targets)
        self.assertEqual(policy['type'], policy_type)

        policy_properties = policy['properties']
        self.assertEqual(min_instances, policy_properties['min_instances'])
        self.assertEqual(max_instances, policy_properties['max_instances'])
        self.assertEqual(default_instances,
                         policy_properties['current_instances'])
        self.assertEqual(default_instances,
                         policy_properties['planned_instances'])
        self.assertEqual(default_instances,
                         policy_properties['default_instances'])

    def test_basic_namespace_policies(self):
        imported_yaml = """
groups:
  group:
    members: [node]
node_templates:
  node: {type: type}
node_types:
  type: {}
policies:
  policy:
    targets: [group]
    type: cloudify.policies.scaling
    properties:
            default_instances: 2
            min_instances: 1
            max_instances: 10
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        policy = parsed_yaml[constants.POLICIES]['test::policy']
        self._assert_policy(policy,
                            targets= ['test::group'],
                            policy_type='test::cloudify.policies.scaling',
                            min_instances=1,
                            max_instances=10,
                            default_instances=2)

    def test_multi_layer_policies_with_targets(self):
        """
        Due to the patch for targets prop, we verify it
        still receives a namespace in another import.
        """
        top_level_yaml = """
groups:
  group:
    members: [node]
node_templates:
  node: {type: type}
node_types:
  type: {}
policies:
  policy:
    targets: [group]
    type: cloudify.policies.scaling
    properties:
            default_instances: 2
            min_instances: 1
            max_instances: 10
"""
        top_file_name = self.make_yaml_file(top_level_yaml)
        middle_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
""".format('middle_test', top_file_name)
        middle_file_name = self.make_yaml_file(middle_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
""".format('test', middle_file_name)
        parsed_yaml = self.parse(main_yaml)
        policy = parsed_yaml[constants.POLICIES]['test::middle_test::policy']
        self._assert_policy(policy,
                            targets= ['test::middle_test::group'],
                            policy_type='test::middle_test::cloudify.policies.scaling',
                            min_instances=1,
                            max_instances=10,
                            default_instances=2)


class TestNamespacedPoliciesTriggers(AbstractTestParser):
    def _assert_policy_trigger(self,
                               parsed_yaml,
                               name,
                               source,
                               prop_name,
                               description):
        triggers = parsed_yaml[constants.POLICY_TRIGGERS]
        self.assertIn(name, triggers)
        trigger = triggers[name]
        self.assertEqual(trigger['source'], source)
        trigger_parameters = trigger[constants.PARAMETERS]
        self.assertIn(prop_name, trigger_parameters)
        self.assertEqual(trigger_parameters[prop_name]['description'],
                         description)

    def test_basic_namespaced_trigger(self):
        imported_yaml = """
policy_triggers:
    trigger:
        source: source
        parameters:
            param1:
                description: the description
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        self._assert_policy_trigger(parsed_yaml,
                                    'test::trigger',
                                    'source',
                                    'param1',
                                    'the description')

    def test_basic_namespace_multi_import(self):
        imported_yaml = """
policy_triggers:
    trigger:
        source: source
        parameters:
            param1:
                description: the description
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
    -   {2}::{1}
""".format('test', import_file_name, 'other_test')
        parsed_yaml = self.parse(main_yaml)
        self._assert_policy_trigger(parsed_yaml,
                                    'test::trigger',
                                    'source',
                                    'param1',
                                    'the description')
        self._assert_policy_trigger(parsed_yaml,
                                    'other_test::trigger',
                                    'source',
                                    'param1',
                                    'the description')

    def test_policy_trigger_collision(self):
        imported_yaml = """
policy_triggers:
    trigger:
        source: source
        parameters:
            param1:
                description: the description
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
policy_triggers:
    trigger:
        source: source2
        parameters:
            param1:
                description: the description
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        self._assert_policy_trigger(parsed_yaml,
                                    'test::trigger',
                                    'source',
                                    'param1',
                                    'the description')
        self._assert_policy_trigger(parsed_yaml,
                                    'trigger',
                                    'source2',
                                    'param1',
                                    'the description')

    def test_imports_merging_with_no_collision(self):
        imported_yaml = """
policy_triggers:
    trigger:
        source: source
        parameters:
            param1:
                description: the description
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
policy_triggers:
    trigger2:
        source: source2
        parameters:
            param1:
                description: the description
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        self._assert_policy_trigger(parsed_yaml,
                                    'test::trigger',
                                    'source',
                                    'param1',
                                    'the description')
        self._assert_policy_trigger(parsed_yaml,
                                    'trigger2',
                                    'source2',
                                    'param1',
                                    'the description')


class TestNamespacedPoliciesTypes(AbstractTestParser):
    def _assert_policy_type(self,
                            parsed_yaml,
                            policy_name,
                            source,
                            prop_name,
                            prop_description):
        policy_types = parsed_yaml[constants.POLICY_TYPES]
        self.assertIn(policy_name, policy_types)
        policy_type = policy_types[policy_name]
        self.assertEqual(policy_type['source'], source)
        trigger_parameters = policy_type[constants.PROPERTIES]
        self.assertIn(prop_name, trigger_parameters)
        self.assertEqual(trigger_parameters[prop_name]['description'],
                         prop_description)

    def test_basic_namespaced_policy_type(self):
        imported_yaml = """
policy_types:
    type:
        source: source
        properties:
            param1:
                description: the description
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        self._assert_policy_type(parsed_yaml,
                                 'test::type',
                                 'source',
                                 'param1',
                                 'the description')

    def test_basic_namespace_multi_import(self):
        imported_yaml = """
policy_types:
    type:
        source: source
        properties:
            param1:
                description: the description
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
    -   {2}::{1}
""".format('test', import_file_name, 'other_test')
        parsed_yaml = self.parse(main_yaml)
        self._assert_policy_type(parsed_yaml,
                                 'test::type',
                                 'source',
                                 'param1',
                                 'the description')
        self._assert_policy_type(parsed_yaml,
                                 'other_test::type',
                                 'source',
                                 'param1',
                                 'the description')

    def test_policy_type_collision(self):
        imported_yaml = """
policy_types:
    type:
        source: source
        properties:
            param1:
                description: the description
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
policy_types:
    type:
        source: source2
        properties:
            param1:
                description: the description
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        self._assert_policy_type(parsed_yaml,
                                 'test::type',
                                 'source',
                                 'param1',
                                 'the description')
        self._assert_policy_type(parsed_yaml,
                                 'type',
                                 'source2',
                                 'param1',
                                 'the description')

    def test_imports_merging_with_no_collision(self):
        imported_yaml = """
policy_types:
    type:
        source: source
        properties:
            param1:
                description: the description
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
policy_types:
    type2:
        source: source
        properties:
            param1:
                description: the description
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        self._assert_policy_type(parsed_yaml,
                                 'test::type',
                                 'source',
                                 'param1',
                                 'the description')
        self._assert_policy_type(parsed_yaml,
                                 'type2',
                                 'source',
                                 'param1',
                                 'the description')
