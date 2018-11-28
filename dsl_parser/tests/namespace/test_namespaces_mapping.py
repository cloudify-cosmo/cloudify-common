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

from dsl_parser import constants, utils
from dsl_parser.tests.abstract_test_parser import AbstractTestParser
from ...import_resolver.default_import_resolver import DefaultImportResolver


class ResolverWithBlueprintSupport(DefaultImportResolver):
    BLUEPRINT_PREFIX = 'blueprint:'

    def __init__(self, blueprint):
        super(ResolverWithBlueprintSupport, self).__init__()
        self.blueprint_yaml = utils.load_yaml(
            raw_yaml=blueprint,
            error_message="Failed to parse blueprint import'")

    def _is_blueprint_url(self, import_url):
        return import_url.startswith(self.BLUEPRINT_PREFIX)

    def fetch_import(self, import_url):
        if self._is_blueprint_url(import_url):
            return self._fetch_blueprint_import()
        return super(ResolverWithBlueprintSupport,
                     self).fetch_import(import_url)

    def _fetch_blueprint_import(self):
        return self.blueprint_yaml


class TestNamespacesMapping(AbstractTestParser):
    @staticmethod
    def _default_input(description, default, name='port'):
        return """
inputs:
    {2}:
        description: {0}
        default: {1}
""".format(description, default, name)

    blueprint_imported = """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
"""
    resolver = ResolverWithBlueprintSupport(blueprint_imported)

    def test_multi_layer_import_collision(self):
        layer1 = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - blueprint:test
"""
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - {0}->{1}
""".format('test1', layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - first->blueprint:test
  - {0}->{1}
  - {2}->{1}
""".format('test', layer2_import_path, 'other_test')
        parsed_yaml = self.parse(main_yaml, resolver=self.resolver)
        namespaces_mapping = parsed_yaml[constants.NAMESPACES_MAPPING]
        self.assertEqual({'other_test->test1': 'test',
                          'test->test1': 'test',
                          'first': 'test'},
                         namespaces_mapping)
