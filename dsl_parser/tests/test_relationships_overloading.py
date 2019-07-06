########
# Copyright (c) 2019 Cloudify Platform Ltd. All rights reserved
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


class TestRelationshipsOverloading(AbstractTestParser):
    def verify_relationships(self, relationships, expected_rels):
        source_rels = [(rel['target_id'], rel['type'])
                       for rel in relationships]
        self.assertItemsEqual(source_rels, expected_rels)

    def validate_expected_relationships(self, main_yaml,
                                        test_node_name,
                                        expected_relationships,
                                        resolver=None):
        parsed = self.parse_1_3(main_yaml, resolver=resolver)
        test_node = self.get_node_by_name(parsed, test_node_name)
        self.assertEqual(len(test_node[constants.RELATIONSHIPS]),
                         len(expected_relationships))
        self.verify_relationships(test_node[constants.RELATIONSHIPS],
                                  expected_relationships)

    def test_relationships_overloading_local_import_with_namespace(self):
        node_blueprint = """
node_types:
  test_type:
    properties:
      prop1:
        default: value2
node_templates:
    test_node:
        type: test_type
"""
        import_file_name = self.make_yaml_file(node_blueprint)

        main_yaml = """
relationships:
    cloudify.relationships.depends_on:
        properties:
            connection_type:
                default: 'all_to_all'
node_templates:
    other_node:
        type: test--test_type
    test--test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: other_node
imports:
    -   test--{0}
""".format(import_file_name)
        self.validate_expected_relationships(
            main_yaml,
            'test--test_node',
            [('other_node', 'cloudify.relationships.depends_on')])

    def test_relationships_overloading_local_import_without_namespace(self):
        node_blueprint = """
node_types:
  test_type:
    properties:
      prop1:
        default: value2
node_templates:
    test_node:
        type: test_type
"""
        import_file_name = self.make_yaml_file(node_blueprint)

        main_yaml = """
relationships:
    cloudify.relationships.depends_on:
        properties:
            connection_type:
                default: 'all_to_all'
node_templates:
    other_node:
        type: test_type
    test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: other_node
imports:
    -   {0}
""".format(import_file_name)

        self.validate_expected_relationships(
            main_yaml,
            'test_node',
            [('other_node', 'cloudify.relationships.depends_on')])

    def test_relationships_overloading_blueprint_import(self):
        node_blueprint = """
tosca_definitions_version: cloudify_dsl_1_3

node_types:
  test_type:
    properties:
      prop1:
        default: value2
node_templates:
    test_node:
        type: test_type
"""
        resolver = ResolverWithBlueprintSupport(
            {'blueprint:node': node_blueprint})

        main_yaml = """
tosca_definitions_version: cloudify_dsl_1_3

relationships:
    cloudify.relationships.depends_on:
        properties:
            connection_type:
                default: 'all_to_all'

node_templates:
    other_node:
        type: test--test_type
    test--test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: other_node
imports:
    -   test--blueprint:node
"""
        self.validate_expected_relationships(
            main_yaml,
            'test--test_node',
            [('other_node', 'cloudify.relationships.depends_on')],
            resolver)

    def test_extending_relationships(self):
        node_blueprint = """
relationships:
    cloudify.relationships.depends_on:
        properties:
            connection_type:
                default: 'all_to_all'

node_types:
  test_type:
    properties:
      prop1:
        default: value2

node_templates:
    other_node:
        type: test_type

    test_node:
        type: test_type
        relationships:
            - type: cloudify.relationships.depends_on
              target: other_node
"""
        import_file_name = self.make_yaml_file(node_blueprint)

        main_yaml = """
node_templates:
    test--test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: test--other_node
imports:
    -   test--{0}
""".format(import_file_name)

        self.validate_expected_relationships(
            main_yaml,
            'test--test_node',
            [('test--other_node', 'cloudify.relationships.depends_on'),
             ('test--other_node', 'cloudify.relationships.depends_on')])

    def test_not_existing_overloading_target(self):
        node_blueprint = """
node_types:
  test_type:
    properties:
      prop1:
        default: value2

node_templates:
    test_node:
        type: test_type
"""
        import_file_name = self.make_yaml_file(node_blueprint)

        main_yaml = """
relationships:
    cloudify.relationships.depends_on:
        properties:
            connection_type:
                default: 'all_to_all'

node_templates:
    other_node:
        type: test--test_type

    test--test_node1:
        relationships:
            - type: cloudify.relationships.depends_on
              target: other_node
imports:
    -   test--{0}
""".format(import_file_name)
        self.assertRaises(exceptions.DSLParsingFormatException,
                          self.parse_1_3,
                          main_yaml)

    def test_multi_levels_extend(self):
        node_blueprint = """
node_types:
  test_type:
    properties:
      prop1:
        default: value2

relationships:
    cloudify.relationships.depends_on:
        properties:
            connection_type:
                default: 'all_to_all'

node_templates:
    other_node:
        type: test_type
    test_node:
        type: test_type
"""
        node_file_name = self.make_yaml_file(node_blueprint)

        middle_extender_blueprint = """
node_templates:
    test--test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: test--other_node
imports:
    -   test--{0}
""".format(node_file_name)

        extender_file_name = self.make_yaml_file(middle_extender_blueprint)

        main_yaml = """
node_templates:
    test--test--test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: test--test--other_node
imports:
    -   test--{0}
""".format(extender_file_name)

        self.validate_expected_relationships(
            main_yaml,
            'test--test--test_node',
            [('test--test--other_node', 'cloudify.relationships.depends_on'),
             ('test--test--other_node', 'cloudify.relationships.depends_on')])

    def test_multi_levels_extend_with_blueprint_import(self):
        node_blueprint = """
tosca_definitions_version: cloudify_dsl_1_3

node_types:
  test_type:
    properties:
      prop1:
        default: value2
relationships:
    cloudify.relationships.depends_on:
        properties:
            connection_type:
                default: 'all_to_all'
node_templates:
    other_node:
        type: test_type
    test_node:
        type: test_type
"""
        middle_extender_blueprint = """
tosca_definitions_version: cloudify_dsl_1_3

node_templates:
    test--test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: test--other_node
imports:
    -   test--blueprint:node
"""
        resolver = ResolverWithBlueprintSupport(
            {'blueprint:node': node_blueprint,
             'blueprint:middle': middle_extender_blueprint})
        main_yaml = """
tosca_definitions_version: cloudify_dsl_1_3

node_templates:
    test--test--test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: test--test--other_node
imports:
    -   test--blueprint:middle
"""

        self.validate_expected_relationships(
            main_yaml,
            'test--test--test_node',
            [('test--test--other_node', 'cloudify.relationships.depends_on'),
             ('test--test--other_node', 'cloudify.relationships.depends_on')],
            resolver)

    def test_middle_layer_disrupt(self):
        node_blueprint = """
node_types:
  test_type:
    properties:
      prop1:
        default: value2

relationships:
    cloudify.relationships.depends_on:
        properties:
            connection_type:
                default: 'all_to_all'

node_templates:
    other_node:
        type: test_type
    test_node:
        type: test_type
"""
        node_file_name = self.make_yaml_file(node_blueprint)

        extender_blueprint = """
node_templates:
    test--test_node:
        type: test--test_type
imports:
    -   test--{0}
""".format(node_file_name)

        extender_file_name = self.make_yaml_file(extender_blueprint)

        main_yaml = """
node_templates:
    test--test--test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: test--test--other_node
imports:
    -   test--{0}
""".format(extender_file_name)

        self.assertRaises(exceptions.DSLParsingLogicException,
                          self.parse_1_3,
                          main_yaml)

    def test_not_extending_at_main_blueprint_failure(self):
        node_blueprint = """
node_types:
  test_type:
    properties:
      prop1:
        default: value2

relationships:
    cloudify.relationships.depends_on:
        properties:
            connection_type:
                default: 'all_to_all'

node_templates:
    other_node:
        type: test_type
    test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: test--other_node
"""
        node_file_name = self.make_yaml_file(node_blueprint)

        middle_extender_blueprint = """
node_templates:
    test--test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: test--other_node
imports:
    -   test--{0}
""".format(node_file_name)

        extender_file_name = self.make_yaml_file(middle_extender_blueprint)

        main_yaml = """
node_templates:
    test--test--test_node:
        type: test_type
imports:
    -   test--{0}
""".format(extender_file_name)

        self.assertRaises(exceptions.DSLParsingLogicException,
                          self.parse_1_3,
                          main_yaml)

    def test_extending_other_fields_failure(self):
        node_blueprint = """
node_types:
  test_type:
    properties:
      prop1:
        default: value2

relationships:
    cloudify.relationships.depends_on:
        properties:
            connection_type:
                default: 'all_to_all'

node_templates:
    other_node:
        type: test_type
    test_node:
        type: test_type
"""
        node_file_name = self.make_yaml_file(node_blueprint)

        extender_blueprint = """
node_templates:
    test--test_node:
        properties:
            prop1: e
imports:
    -   test--{0}
""".format(node_file_name)

        extender_file_name = self.make_yaml_file(extender_blueprint)

        main_yaml = """
node_templates:
    test--test--test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: test--test--other_node
imports:
    -   test--{0}
""".format(extender_file_name)

        self.assertRaises(exceptions.DSLParsingLogicException,
                          self.parse_1_3,
                          main_yaml)

    def test_extending_only_other_fields_failure(self):
        node_blueprint = """
node_types:
  test_type:
    properties:
      prop1:
        default: value2

relationships:
    cloudify.relationships.depends_on:
        properties:
            connection_type:
                default: 'all_to_all'

node_templates:
    other_node:
        type: test_type
    test_node:
        type: test_type
"""
        node_file_name = self.make_yaml_file(node_blueprint)

        extender_blueprint = """
node_templates:
    test--test_node:
        properties:
            prop1: e
imports:
    -   test--{0}
""".format(node_file_name)

        extender_file_name = self.make_yaml_file(extender_blueprint)

        main_yaml = """
node_templates:
    test--test--test_node:
        properties:
                prop1: e
imports:
    -   test--{0}
""".format(extender_file_name)

        self.assertRaises(exceptions.DSLParsingLogicException,
                          self.parse_1_3,
                          main_yaml)

    def test_side_import_expending_relationships(self):
        side_blueprint = """
imports:
    - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml

node_types:
  test_other_type:
    properties:
      prop1:
        default: value2

node_templates:
    some_node:
        type: test_other_type

    test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: some_node
"""
        side_import_file_name = self.make_yaml_file(side_blueprint)
        node_blueprint = """
imports:
    - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml

node_types:
  test_type:
    properties:
      prop1:
        default: value2

node_templates:
    other_node:
        type: test_type

    test_node:
        type: test_type
        relationships:
            - type: cloudify.relationships.depends_on
              target: other_node
"""
        import_file_name = self.make_yaml_file(node_blueprint)

        main_yaml = """
node_templates:
    test--test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: test--other_node
imports:
    -   test--{0}
    -   test--{1}
""".format(import_file_name, side_import_file_name)

        self.validate_expected_relationships(
            main_yaml,
            'test--test_node',
            [('test--other_node', 'cloudify.relationships.depends_on'),
             ('test--other_node', 'cloudify.relationships.depends_on'),
             ('test--some_node', 'cloudify.relationships.depends_on')])

    def test_side_import_not_expending_relationships_failure(self):
        side_blueprint = """
imports:
    - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml

node_types:
  test_other_type:
    properties:
      prop1:
        default: value2

node_templates:
    some_node:
        type: test_other_type

    test_node:
        properties:
            prop1: value2

"""
        side_import_file_name = self.make_yaml_file(side_blueprint)
        node_blueprint = """
imports:
    - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml

node_types:
  test_type:
    properties:
      prop1:
        default: value2

node_templates:
    other_node:
        type: test_type

    test_node:
        type: test_type
        relationships:
            - type: cloudify.relationships.depends_on
              target: other_node
"""
        import_file_name = self.make_yaml_file(node_blueprint)

        main_yaml = """
node_templates:
    test--test_node:
        relationships:
            - type: cloudify.relationships.depends_on
              target: test--other_node
imports:
    -   test--{0}
    -   test--{1}
""".format(import_file_name, side_import_file_name)

        self.assertRaises(exceptions.DSLParsingLogicException,
                          self.parse_1_3,
                          main_yaml)
