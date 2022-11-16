import random

import pytest

from dsl_parser import constants, exceptions
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TypesBasedOnDatabaseTest(AbstractTestParser):
    YAML_with_dep_id = """
inputs:
  {data_type}:
    type: {data_type}
    constraints:
      - deployment_id: d1
    """

    YAML_without_dep_id = """
inputs:
  {data_type}:
    type: {data_type}
    """

    YAML_for_node_props = """
node_types:
  t1:
    properties:
      {data_type}:
        type: {data_type}
node_templates:
  x:
    type: t1
    properties:
      {data_type}: foobar
        """

    def test_fuzzy_with_deployment_id_constraint_1_3(self):
        data_type = random.choice(
            constants.TYPES_WHICH_REQUIRE_DEPLOYMENT_ID_CONSTRAINT)
        with pytest.raises(exceptions.DSLParsingLogicException,
                           match="^type '{data_type}' not.*cloudify_dsl_1_3"
                                 .format(data_type=data_type)):
            self.parse_1_3(TypesBasedOnDatabaseTest.YAML_with_dep_id
                           .format(data_type=data_type))

    def test_fuzzy_with_deployment_id_constraint_1_4(self):
        data_type = random.choice(
            constants.TYPES_WHICH_REQUIRE_DEPLOYMENT_ID_CONSTRAINT)
        parsed = self.parse_1_4(TypesBasedOnDatabaseTest.YAML_with_dep_id
                                .format(data_type=data_type))
        assert parsed['inputs'][data_type]['type'] == data_type

    def test_fuzzy_without_deployment_id_constraint_1_3(self):
        data_type = random.choice(
            list(set(constants.OBJECT_BASED_TYPES) -
                 set(constants.TYPES_WHICH_REQUIRE_DEPLOYMENT_ID_CONSTRAINT)))
        with pytest.raises(exceptions.DSLParsingLogicException,
                           match="^type '{data_type}' not.*cloudify_dsl_1_3"
                                 .format(data_type=data_type)):
            self.parse_1_3(TypesBasedOnDatabaseTest.YAML_without_dep_id
                           .format(data_type=data_type))

    def test_fuzzy_without_deployment_id_constraint_1_4(self):
        data_type = random.choice(
            list(set(constants.OBJECT_BASED_TYPES) -
                 set(constants.TYPES_WHICH_REQUIRE_DEPLOYMENT_ID_CONSTRAINT)))
        parsed = self.parse_1_4(TypesBasedOnDatabaseTest.YAML_without_dep_id
                                .format(data_type=data_type))
        assert parsed['inputs'][data_type]['type'] == data_type

    def test_fuzzy_node_property_1_3(self):
        data_type = random.choice(
            list(set(constants.OBJECT_BASED_TYPES) -
                 set(constants.TYPES_WHICH_REQUIRE_DEPLOYMENT_ID_CONSTRAINT)))
        with pytest.raises(exceptions.DSLParsingLogicException,
                           match="^type '{data_type}' not.*cloudify_dsl_1_3"
                                 .format(data_type=data_type)):
            self.parse_1_3(TypesBasedOnDatabaseTest.YAML_for_node_props
                           .format(data_type=data_type))

    def test_fuzzy_node_property_1_4(self):
        data_type = random.choice(
            list(set(constants.OBJECT_BASED_TYPES) -
                 set(constants.TYPES_WHICH_REQUIRE_DEPLOYMENT_ID_CONSTRAINT)))
        parsed = self.parse_1_4(TypesBasedOnDatabaseTest.YAML_for_node_props
                                .format(data_type=data_type))
        assert parsed['nodes'][0]['properties'][data_type] == 'foobar'
