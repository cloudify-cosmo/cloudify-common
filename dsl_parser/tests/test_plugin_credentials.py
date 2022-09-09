import pytest

from dsl_parser.exceptions import DSLParsingLogicException
from dsl_parser.tests.abstract_test_parser import AbstractTestParser

CREDENTIALS_YAML = """
credentials:
  description: |
    Description regarding the credentials and
    the link to AWS documentation how to generate
    or the link to cloudify documentation on
    different types of authentication methods
  properties:
    aws_access_key_id:
      type: string
      description: This is a AWS Access Key ID
      display_label: AWS Access Key ID
    aws_secret_access_key:
      type: string
      description: This is a AWS Secret Access Key
      display_label: AWS Secret Access Key
    aws_region:
      type: string
      display_label: AWS Region
"""


class ResourceTagsTest(AbstractTestParser):
    def test_parse_1_4(self):
        with pytest.raises(DSLParsingLogicException,
                           match='not supported in version cloudify_dsl_1_4'):
            self.parse_1_4(
                self.BASIC_VERSION_SECTION_DSL_1_4 + CREDENTIALS_YAML
            )

    def test_parse_1_5(self):
        plan = self.parse_1_5(
            self.BASIC_VERSION_SECTION_DSL_1_5 + CREDENTIALS_YAML
        )
        creds = plan['credentials']
        assert creds['description'].rstrip().endswith('authentication methods')
        assert all(p['type'] == 'string' for p in creds['properties'].values())

    def test_no_properties(self):
        yaml = """
credentials:
  description: Empty credentials
"""
        plan = self.parse_1_5(self.BASIC_VERSION_SECTION_DSL_1_5 + yaml)
        assert plan['credentials']['properties'] == {}
