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
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestCapabilities(AbstractTestParser):

    def test_capabilities_definition(self):
        yaml = """
capabilities: {}
"""
        parsed = self.parse(yaml)
        self.assertEqual(0, len(parsed[constants.CAPABILITIES]))

    def test_capability_definition(self):
        yaml = """
capabilities:
    cap1:
        description: cap1
        value: 1
    cap2:
        value: 2
"""
        parsed = self.parse(yaml)
        capabilities = parsed[constants.CAPABILITIES]
        self.assertEqual(2, len(capabilities))
        self.assertEqual(1, capabilities['cap1']['value'])
        self.assertEqual('cap1', capabilities['cap1']['description'])
        self.assertEqual(2, capabilities['cap2']['value'])
        self.assertNotIn('description', capabilities['cap2'])

    def test_capability_is_scanned(self):
        yaml = """
inputs:
    a:
        default: some_value
node_types:
    some_type: {}
node_templates:
    node1:
        type: some_type
capabilities:
    cap1:
        value: { get_input: a }
    cap2:
        value: { get_attribute: [ node1, really_shouldnt_matter ] }
"""
        plan = prepare_deployment_plan(self.parse(yaml))
        capabilities = plan[constants.CAPABILITIES]
        self.assertEqual('some_value', capabilities['cap1']['value'])
        self.assertDictEqual(
            {'get_attribute': ['node1', 'really_shouldnt_matter']},
            capabilities['cap2']['value'])
