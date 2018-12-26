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


class TestNamespacedGroups(AbstractTestParser):
    basic_group_blueprint = """
node_types:
    test_type: {}
node_templates:
    node1:
        type: test_type
groups:
    group:
        members: [node1]
"""

    def setUp(self):
        super(TestNamespacedGroups, self).setUp()
        self.import_file_name = self.make_yaml_file(self.basic_group_blueprint)

    def test_groups_definition(self):
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
""".format('test', self.import_file_name)
        parsed_yaml = self.parse(main_yaml)
        groups = parsed_yaml[constants.GROUPS]
        self.assertEqual(groups['test--group']['members'],
                         ['test--node1'])

    def test_basic_namespace_multi_import(self):
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
    -   {2}--{1}
""".format('test', self.import_file_name, 'other_test')

        parsed_yaml = self.parse(main_yaml)
        groups = parsed_yaml[constants.GROUPS]
        self.assertEqual(2, len(groups))
        self.assertEqual(groups['test--group']['members'],
                         ['test--node1'])
        self.assertEqual(groups['other_test--group']['members'],
                         ['other_test--node1'])

    def test_group_collision(self):
        main_yaml = """
imports:
  - {0}--{1}
groups:
    group:
        members: ["test--node1"]
""".format('test', self.import_file_name)
        parsed_yaml = self.parse_1_3(main_yaml)
        groups = parsed_yaml[constants.GROUPS]
        self.assertEqual(2, len(groups))
        self.assertEqual(groups['test--group']['members'],
                         ['test--node1'])
        self.assertEqual(groups['group']['members'],
                         ['test--node1'])

    def test_groups_merging_with_no_collision(self):
        main_yaml = """
imports:
  - {0}--{1}
groups:
    group2:
        members: ["test--node1"]
""".format('test', self.import_file_name)
        parsed_yaml = self.parse_1_3(main_yaml)
        groups = parsed_yaml[constants.GROUPS]
        self.assertEqual(2, len(groups))
        self.assertEqual(groups['test--group']['members'],
                         ['test--node1'])
        self.assertEqual(groups['group2']['members'],
                         ['test--node1'])
