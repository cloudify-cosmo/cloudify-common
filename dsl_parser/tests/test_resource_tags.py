import pytest

from dsl_parser import exceptions
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class ResourceTagsTest(AbstractTestParser):
    YAML = """
resource_tags:
    foo: bar
    """

    def test_parse_1_3(self):
        with pytest.raises(exceptions.DSLParsingLogicException,
                           match='^resource_tags not.*cloudify_dsl_1_3'):
            self.parse_1_3(ResourceTagsTest.YAML)

    def test_parse_1_4(self):
        parsed = self.parse_1_4(ResourceTagsTest.YAML)
        assert parsed['resource_tags'] == {'foo': 'bar'}
