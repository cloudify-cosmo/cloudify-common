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
from dsl_parser.exceptions import DSLParsingLogicException
from dsl_parser.tests.abstract_test_parser import AbstractTestParser
from ..import_resolver.default_import_resolver import DefaultImportResolver


class ResolverWithBlueprintSupport(DefaultImportResolver):
    def __init__(self, blueprint):
        super(ResolverWithBlueprintSupport, self).__init__()
        self.blueprint_yaml = utils.load_yaml(
            raw_yaml=blueprint,
            error_message="Failed to parse blueprint import'")

    def _is_blueprint_url(self, import_url):
        return import_url.startswith(constants.BLUEPRINT_IMPORT)

    def fetch_import(self, import_url):
        if self._is_blueprint_url(import_url):
            return self._fetch_blueprint_import()
        return super(ResolverWithBlueprintSupport,
                     self).fetch_import(import_url)

    def _fetch_blueprint_import(self):
        return self.blueprint_yaml


class TestImportedBlueprints(AbstractTestParser):
    basic_blueprint = """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
"""
    blueprint_with_blueprint_import = """
imported_blueprints:
    - other_test
"""

    def test_with_no_blueprints_imports(self):
        imported_blueprint_yaml = self.MINIMAL_BLUEPRINT

        imported_blueprint = self.make_yaml_file(imported_blueprint_yaml, True)

        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}
    -   http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
""".format(imported_blueprint)

        parsed_yaml = self.parse(yaml)
        imported_blueprints = parsed_yaml[constants.IMPORTED_BLUEPRINTS]
        self.assertEqual(len(imported_blueprints), 0)

    def test_imported_list_with_namespace(self):
        layer1 = """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
"""
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
  - {0}->{1}
""".format('test1', layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
  - {0}->{1}
""".format('test', layer2_import_path)
        parsed_yaml = self.parse_1_3(main_yaml)
        imported_blueprints = parsed_yaml[constants.IMPORTED_BLUEPRINTS]
        self.assertEqual(len(imported_blueprints), 0)

    def test_basic_blueprint_import(self):
        resolver = ResolverWithBlueprintSupport(self.basic_blueprint)
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   ns->blueprint:test
"""
        parsed_yaml = self.parse(yaml, resolver=resolver)
        imported_blueprints = parsed_yaml[constants.IMPORTED_BLUEPRINTS]
        self.assertEqual(len(imported_blueprints), 1)
        self.assertEqual(
            ['test'],
            imported_blueprints)

    def test_merge_imported_lists(self):
        resolver =\
            ResolverWithBlueprintSupport(self.blueprint_with_blueprint_import)
        layer1 = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - ns->blueprint:test
"""
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
  - {0}->{1}
  - ns->blueprint:another_test
""".format('test1', layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
  - {0}->{1}
""".format('test', layer2_import_path)
        parsed_yaml = self.parse(main_yaml, resolver=resolver)
        imported_blueprints = parsed_yaml[constants.IMPORTED_BLUEPRINTS]
        self.assertEqual(len(imported_blueprints), 3)
        self.assertItemsEqual(
            ['other_test',
             'test',
             'another_test'],
            imported_blueprints)

    def test_not_valid_blueprint_import(self):
        yaml = """
imports:
    -   blueprint:test
"""
        self._assert_dsl_parsing_exception_error_code(
            yaml, 213, DSLParsingLogicException)
