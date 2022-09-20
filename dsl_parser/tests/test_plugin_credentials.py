import pytest

from dsl_parser.exceptions import DSLParsingLogicException
from dsl_parser.tests.abstract_test_parser import AbstractTestParser

PLUGINS_YAML = """
plugins:
  dummy:
    executor: central_deployment_agent
    package_name: testing
    source: dummy
    properties_description: |
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

node_types:
  type: {}

node_templates:
  dummy_node:
    type: type
    interfaces:
      dummy_install:
        install: dummy.install
"""


class ResourceTagsTest(AbstractTestParser):
    def test_parse_1_4(self):
        with pytest.raises(DSLParsingLogicException,
                           match='not supported in version cloudify_dsl_1_4'):
            self.parse_1_4(
                self.BASIC_VERSION_SECTION_DSL_1_4 + PLUGINS_YAML
            )

    def test_parse_1_5(self):
        plan = self.parse_1_5(
            self.BASIC_VERSION_SECTION_DSL_1_5 + PLUGINS_YAML
        )
        dummy = plan['deployment_plugins_to_install'][0]
        assert dummy['properties_description'].rstrip()\
            .endswith('authentication methods')
        assert all(p['type'] == 'string' for p in dummy['properties'].values())

    def test_no_properties(self):
        yaml = """
plugins:
  dummy:
    executor: central_deployment_agent
    package_name: testing
    source: dummy
    properties_description: Empty credentials

node_types:
  type: {}

node_templates:
  dummy_node:
    type: type
    interfaces:
      dummy_install:
        install: dummy.install
"""
        plan = self.parse_1_5(self.BASIC_VERSION_SECTION_DSL_1_5 + yaml)
        assert plan['deployment_plugins_to_install'][0]['properties'] == {}
