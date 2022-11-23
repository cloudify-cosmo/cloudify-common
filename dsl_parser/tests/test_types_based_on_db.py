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

    def test_with_deployment_id_constraint_1_3(self):
        data_types = constants.BLUEPRINT_OR_DEPLOYMENT_ID_CONSTRAINT_TYPES + \
            constants.DEPLOYMENT_ID_CONSTRAINT_TYPES
        for data_type in data_types:
            with pytest.raises(exceptions.DSLParsingLogicException,
                               match="^type '{data_type}' not.*cloudify_"
                                     "dsl_1_3".format(data_type=data_type)):
                self.parse_1_3(self.YAML_with_dep_id
                               .format(data_type=data_type))

    def test_with_deployment_id_constraint_1_5(self):
        data_types = constants.BLUEPRINT_OR_DEPLOYMENT_ID_CONSTRAINT_TYPES + \
                     constants.DEPLOYMENT_ID_CONSTRAINT_TYPES
        for data_type in data_types:
            parsed = self.parse_1_5(self.YAML_with_dep_id
                                    .format(data_type=data_type))
            assert parsed['inputs'][data_type]['type'] == data_type

    def test_without_deployment_id_constraint_1_3(self):
        data_types = set(constants.OBJECT_BASED_TYPES) - \
            set(constants.BLUEPRINT_OR_DEPLOYMENT_ID_CONSTRAINT_TYPES +
                constants.DEPLOYMENT_ID_CONSTRAINT_TYPES)
        for data_type in data_types:
            with pytest.raises(exceptions.DSLParsingLogicException,
                               match="^type '{data_type}' not.*cloudify_"
                                     "dsl_1_3".format(data_type=data_type)):
                self.parse_1_3(self.YAML_without_dep_id
                               .format(data_type=data_type))

    def test_without_deployment_id_constraint_1_5(self):
        data_types = set(constants.OBJECT_BASED_TYPES) - \
            set(constants.BLUEPRINT_OR_DEPLOYMENT_ID_CONSTRAINT_TYPES +
                constants.DEPLOYMENT_ID_CONSTRAINT_TYPES)
        for data_type in data_types:
            parsed = self.parse_1_5(self.YAML_without_dep_id
                                    .format(data_type=data_type))
            assert parsed['inputs'][data_type]['type'] == data_type

    def test_node_property_1_3(self):
        data_types = set(constants.OBJECT_BASED_TYPES) - \
            set(constants.BLUEPRINT_OR_DEPLOYMENT_ID_CONSTRAINT_TYPES +
                constants.DEPLOYMENT_ID_CONSTRAINT_TYPES)
        for data_type in data_types:
            with pytest.raises(exceptions.DSLParsingLogicException,
                               match="^type '{data_type}' not.*cloudify_"
                                     "dsl_1_3".format(data_type=data_type)):
                self.parse_1_3(self.YAML_for_node_props
                               .format(data_type=data_type))

    def test_node_property_1_5(self):
        data_types = set(constants.OBJECT_BASED_TYPES) - \
            set(constants.BLUEPRINT_OR_DEPLOYMENT_ID_CONSTRAINT_TYPES +
                constants.DEPLOYMENT_ID_CONSTRAINT_TYPES)
        for data_type in data_types:
            parsed = self.parse_1_5(self.YAML_for_node_props
                                    .format(data_type=data_type))
            assert parsed['nodes'][0]['properties'][data_type] == 'foobar'
