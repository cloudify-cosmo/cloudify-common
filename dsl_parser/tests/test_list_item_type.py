import pytest

from dsl_parser import exceptions
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class ListItemTypeTest(AbstractTestParser):
    YAML = """
inputs:
    integers_list:
        type: list
        item_type: integer
    booleans_list:
        type: list
        item_type: boolean
    """

    def test_parse_1_3(self):
        with pytest.raises(exceptions.DSLParsingLogicException,
                           match='^item_type not.*cloudify_dsl_1_3'):
            self.parse_1_3(ListItemTypeTest.YAML)

    def test_parse_1_4(self):
        parsed = self.parse_1_4(ListItemTypeTest.YAML)
        assert parsed['inputs']['integers_list'] == \
               {'item_type': 'integer', 'type': 'list'}
