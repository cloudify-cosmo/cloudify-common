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


class TestNamespacedPolicies(AbstractTestParser):
    def test_basic_policiec(self):
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
relationships:
  cloudify.relationships.contained_in: {}
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}::{1}
""".format('test', import_file_name)
        parsed_yaml = self.parse(main_yaml)
        policies = parsed_yaml[constants.POLICIES]
        self.assertEqual(policies['test::policy']['targets'],
                         ['test::group'])
        self.assertEqual(policies['test::policy']['type'],
                         'test::cloudify.policies.scaling')

    def test_basic_namespace_multi_import(self):
        pass

    def test_policies_collision(self):
        pass

    def test_imports_merging_with_no_collision(self):
        pass
