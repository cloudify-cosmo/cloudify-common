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

    def test_basic_policies(self):
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
relationships:
  cloudify.relationships.contained_in: {}
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
                            min_instances= 1,
                            max_instances= 10,
                            default_instances= 2)

    def test_can_not_merge_namespace_policies(self):
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
relationships:
  cloudify.relationships.contained_in: {}
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
    -   {2}::{1}
""".format('test', import_file_name, 'other_test')
        self.assertRaises(
                exceptions.DSLParsingLogicException,
                self.parse,
                main_yaml)
