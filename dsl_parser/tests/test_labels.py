from dsl_parser import constants, exceptions
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestLabels(AbstractTestParser):

    def test_labels_definition(self):
        yaml = """
labels: {}
"""
        parsed = self.parse(yaml)
        self.assertEqual(0, len(parsed[constants.LABELS]))

    def test_label_definition(self):
        yaml = """
labels:
    key1:
        value: key1_val1
    key2:
        value:
          - key2_val1
          - key2_val2
"""
        parsed = self.parse(yaml)
        labels = parsed[constants.LABELS]
        self.assertEqual(2, len(labels))
        self.assertEqual('key1_val1', labels['key1']['value'])
        self.assertEqual(['key2_val1', 'key2_val2'], labels['key2']['value'])

    def test_label_is_scanned(self):
        yaml = """
tosca_definitions_version: cloudify_dsl_1_3

inputs:
    a:
        default: some_value

labels:
    concat:
        value: { concat: ['a', 'b'] }
    get_input:
        value: { get_input: a }
"""
        plan = prepare_deployment_plan(self.parse(yaml))
        labels = plan[constants.LABELS]
        self.assertEqual('ab', labels['concat']['value'])
        self.assertEqual('some_value', labels['get_input']['value'])

    def test_label_value_get_attribute_fail(self):
        yaml_1 = """
tosca_definitions_version: cloudify_dsl_1_3

labels:
    get_attribute:
        value: { get_attribute: [ node, attr ] }
"""
        yaml_2 = """
tosca_definitions_version: cloudify_dsl_1_3

labels:
    get_attribute:
        value:
          - val1
          - { get_attribute: [ node, attr ] }
"""
        message_regex = '.*cannot be a runtime property.*'
        self.assertRaisesRegex(
            exceptions.DSLParsingException, message_regex, self.parse, yaml_1)
        self.assertRaisesRegex(
            exceptions.DSLParsingException, message_regex, self.parse, yaml_2)
