import pytest
from mock import patch

from dsl_parser.exceptions import (DSLParsingFormatException,
                                   DSLParsingLogicException)
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
"""

NODES_YAML = """
node_types:
  type: {}

node_templates:
  dummy_node:
    type: type
    interfaces:
      dummy_install:
        install: dummy.install
"""


class PluginPropertiesTest(AbstractTestParser):
    def test_parse_1_4(self):
        with pytest.raises(DSLParsingLogicException,
                           match='not supported in version cloudify_dsl_1_4'):
            self.parse_1_4(PLUGINS_YAML + NODES_YAML)

    def test_parse_1_5(self):
        plan = self.parse_1_5(PLUGINS_YAML + NODES_YAML)
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
        plan = self.parse_1_5(yaml)
        assert plan['deployment_plugins_to_install'][0]['properties'] == {}


class PluginPropertiesImporterTest(AbstractTestParser):

    def test_no_properties(self):
        yaml = """
imports:
  - cloudify/types/types.yaml
  - plugin:dummy
"""
        with patch('dsl_parser.elements.imports._build_ordered_imports',
                   return_value=({}, set(), {})) as p:
            self.parse_1_4(yaml)
        assert p.call_count == 1

    def test_basic(self):
        yaml = """
imports:
  - plugin:dummy:
      prop1: foo
      prop2: bar
"""
        with patch('dsl_parser.elements.imports._build_ordered_imports',
                   return_value=({}, set(), {})) as p:
            self.parse_1_5(yaml)
        assert p.call_count == 1

    def test_valid(self):
        yaml = """
imports:
  - cloudify/types/types.yaml
  - plugin:dummy:
      aws_access_key_id: foobar
      aws_secret_access_key: {get_secret: my_secret}
"""
        with patch('dsl_parser.elements.imports._build_ordered_imports',
                   return_value=({}, set(), {})) as p:
            self.parse_1_5(yaml)
        assert p.call_count == 1

    def test_invalid_old_dsl(self):
        yaml = """
imports:
  - cloudify/types/types.yaml
  - plugin:dummy:
      aws_access_key_id: foobar
      aws_secret_access_key: {get_secret: my_secret}
"""
        with patch('dsl_parser.elements.imports._build_ordered_imports',
                   return_value=({}, set(), {})):
            with pytest.raises(DSLParsingLogicException):
                self.parse_1_4(yaml)

    def test_invalid_too_large_dict(self):
        yaml = """
imports:
  - cloudify/types/types.yaml
  - plugin:dummy:
      aws_access_key_id: foobar
      aws_secret_access_key: {get_secret: my_secret}
    plugin:foobar: {}
"""
        with patch('dsl_parser.elements.imports._build_ordered_imports',
                   return_value=({}, set(), {})):
            with pytest.raises(DSLParsingFormatException,
                               match='single-entry dictionary'):
                self.parse_1_5(yaml)

    def test_parse_invalid_schema(self):
        yaml = """
imports:
  - plugin:dummy:
      - foo
      - bar
"""
        with patch('dsl_parser.elements.imports._build_ordered_imports',
                   return_value=({}, set(), {})):
            with pytest.raises(DSLParsingFormatException):
                self.parse_1_5(yaml)

    def test_valid_properties(self):
        yaml = """
imports:
  - cloudify/types/types.yaml
  - plugin:dummy:
      list_property: [1, 2, 3]
      str_property: "lorem ipsum"
      int_property: 1
      float_property: 3.14
      fn_property: {get_secret: something}
"""
        with patch('dsl_parser.elements.imports._build_ordered_imports',
                   return_value=({}, set(), {})):
            self.parse_1_5(yaml)

    def test_invalid_properties(self):
        yaml = """
imports:
  - cloudify/types/types.yaml
  - plugin:dummy:
      property: { not_a_intrinsic_function: something }
"""
        with patch('dsl_parser.elements.imports._build_ordered_imports',
                   return_value=({}, set(), {})):
            with pytest.raises(DSLParsingFormatException):
                self.parse_1_5(yaml)
