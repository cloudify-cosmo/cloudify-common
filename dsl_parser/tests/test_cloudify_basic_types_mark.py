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
from dsl_parser.parser import resolve_blueprint_imports
from dsl_parser.import_resolver.default_import_resolver import \
    DefaultImportResolver


class TestCloudifyBasicTypesMark(AbstractTestParser):
    def test_marking(self):
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   http://www.getcloudify.org/spec/cloudify/4.5/types.yaml

node_types:
  test_type:
    properties:
      prop1:
        default: value
"""
        resolver = DefaultImportResolver()
        _, merged_blueprint, _ = resolve_blueprint_imports(
            dsl_location=None,
            dsl_string=main_yaml,
            resolver=resolver,
            resources_base_path=None,
            validate_version=True)
        _, node_types = merged_blueprint.get_item(constants.NODE_TYPES)
        _, root_type = node_types.get_item('cloudify.nodes.Root')
        _, test_type = node_types.get_item('test_type')
        self.assertEqual(root_type.is_cloudify_type, True)
        self.assertEqual(test_type.is_cloudify_type, False)
