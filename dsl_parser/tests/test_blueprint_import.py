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
from dsl_parser.tests.utils import ResolverWithBlueprintSupport
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestImportedBlueprints(AbstractTestParser):
    basic_blueprint = """
tosca_definitions_version: cloudify_dsl_1_3
imports:
  - http://local-test-resolver/types.yaml
"""
    blueprint_with_blueprint_import = """
tosca_definitions_version: cloudify_dsl_1_3
imported_blueprints:
    - other_test
"""

    def test_with_no_blueprints_imports(self):
        imported_blueprint_yaml = self.MINIMAL_BLUEPRINT

        imported_blueprint = self.make_yaml_file(imported_blueprint_yaml, True)

        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}
    - http://local-test-resolver/types.yaml
""".format(imported_blueprint)

        parsed_yaml = self.parse(yaml)
        imported_blueprints = parsed_yaml[constants.IMPORTED_BLUEPRINTS]
        self.assertEqual(len(imported_blueprints), 0)

    def test_imported_list_with_namespace(self):
        layer1 = """
imports:
  - http://local-test-resolver/types.yaml
"""
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = """
imports:
  - http://local-test-resolver/types.yaml
  - {0}--{1}
""".format('test1', layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = """
imports:
  - http://local-test-resolver/types.yaml
  - {0}--{1}
""".format('test', layer2_import_path)
        parsed_yaml = self.parse_1_3(main_yaml)
        imported_blueprints = parsed_yaml[constants.IMPORTED_BLUEPRINTS]
        self.assertEqual(len(imported_blueprints), 0)

    def test_basic_blueprint_import(self):
        resolver = ResolverWithBlueprintSupport({
            'blueprint:test': self.basic_blueprint
        }, rules=self._local_resolver_rules())
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   ns--blueprint:test
"""
        parsed_yaml = self.parse(yaml, resolver=resolver)
        imported_blueprints = parsed_yaml[constants.IMPORTED_BLUEPRINTS]
        self.assertEqual(len(imported_blueprints), 1)
        self.assertEqual(
            ['test'],
            imported_blueprints)

    def test_merge_imported_lists(self):
        resolver = ResolverWithBlueprintSupport({
            'blueprint:test': self.blueprint_with_blueprint_import,
            'blueprint:another_test': self.blueprint_with_blueprint_import
        }, rules=self._local_resolver_rules())
        layer1 = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - ns--blueprint:test
"""
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://local-test-resolver/types.yaml
  - {0}--{1}
  - ns--blueprint:another_test
""".format('test1', layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://local-test-resolver/types.yaml
  - {0}--{1}
""".format('test', layer2_import_path)
        parsed_yaml = self.parse(main_yaml, resolver=resolver)
        imported_blueprints = parsed_yaml[constants.IMPORTED_BLUEPRINTS]
        self.assertEqual(len(imported_blueprints), 3)
        self.assertEqual(
            {'other_test', 'test', 'another_test'},
            set(imported_blueprints))

    def test_not_valid_blueprint_import(self):
        yaml = """
imports:
    -   blueprint:test
"""
        self._assert_dsl_parsing_exception_error_code(
            yaml, 213, exceptions.DSLParsingLogicException)


class TestNamespacesMapping(AbstractTestParser):
    blueprint_imported = """
tosca_definitions_version: cloudify_dsl_1_3
namespaces_mapping:
  deepns: blueprint
"""

    def test_merging_namespaces_mapping(self):
        resolver = ResolverWithBlueprintSupport({
            'blueprint:testbp': self.blueprint_imported
        }, rules=self._local_resolver_rules())
        layer1 = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - collision--blueprint:testbp
"""
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - nested1--{bp_path}
  - collision--blueprint:testbp
""".format(bp_path=layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - mainns1--{bp_path}
  - mainns2--{bp_path}
""".format(bp_path=layer2_import_path)
        parsed_yaml = self.parse(main_yaml, resolver=resolver)
        namespaces_mapping = parsed_yaml[constants.NAMESPACES_MAPPING]
        # If the nested namespace refers to a path to a blueprint, the value
        # is expected to be: 'blueprint'
        # If the nested namespace refers to a blueprint:<blueprint_name> then
        # the value is expected to be '<blueprint_name>'
        self.assertEqual({
            'mainns2--nested1--collision': 'testbp',
            'mainns1--nested1--collision': 'testbp',
            'mainns2--nested1--collision--deepns': 'blueprint',
            'mainns1--nested1--collision--deepns': 'blueprint',
            'mainns2--collision--deepns': 'blueprint',
            'mainns1--collision': 'testbp',
            'mainns2--collision': 'testbp',
            'mainns1--collision--deepns': 'blueprint'
          }, namespaces_mapping)

    def test_blueprints_imports_with_the_same_import(self):
        resolver = ResolverWithBlueprintSupport({
            'blueprint:test': self.blueprint_imported,
            'blueprint:other': self.blueprint_imported
        }, rules=self._local_resolver_rules())
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   same--blueprint:test
    -   same--blueprint:other
"""
        self.assertRaises(exceptions.DSLParsingLogicException,
                          self.parse, yaml, resolver=resolver)


class TestCloudifyBasicTypes(AbstractTestParser):
    basic_blueprint = """
tosca_definitions_version: cloudify_dsl_1_3
node_types:
  cloudify.nodes.Root:
    interfaces:
      cloudify.interfaces.lifecycle:
        create: {}
        configure: {}
        start: {}
        stop: {}
        delete: {}
      cloudify.interfaces.validation:
        create: {}
        delete: {}
      cloudify.interfaces.monitoring:
        start: {}
        stop: {}
"""

    def test_local_cloudify_types(self):
        local_types_path = self.make_yaml_file(self.basic_blueprint)
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://local-test-resolver/types.yaml
  - {0}
""".format(local_types_path)
        self.parse(main_yaml)

    def test_cloudify_basic_types_blueprint_import(self):
        imported_yaml = """
tosca_definitions_version: cloudify_dsl_1_3
imports:
  - http://local-test-resolver/types.yaml
inputs:
    port:
        default: 90
"""
        resolver = ResolverWithBlueprintSupport({
            'blueprint:test': imported_yaml
        }, rules=self._local_resolver_rules())
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - http://local-test-resolver/types.yaml
    - ns--blueprint:test
"""
        self.parse(yaml, resolver=resolver)
