from dsl_parser import constants, exceptions
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestLabels(AbstractTestParser):

    def test_labels_definition(self):
        yaml_1 = """
labels: {}
"""
        yaml_2 = """
blueprint_labels: {}
"""
        for yaml, labels_type in [(yaml_1, constants.LABELS),
                                  (yaml_2, constants.BLUEPRINT_LABELS)]:
            parsed = self.parse(yaml)
            self.assertEqual(0, len(parsed[labels_type]))

    def test_label_definition(self):
        yaml_1 = """
labels:
    key1:
        value:
            - key1_val1
    key2:
        value:
            - key2_val1
            - key2_val2
"""
        yaml_2 = """
blueprint_labels:
    key1:
        value:
            - key1_val1
    key2:
        value:
            - key2_val1
            - key2_val2
"""
        for yaml, labels_type in [(yaml_1, constants.LABELS),
                                  (yaml_2, constants.BLUEPRINT_LABELS)]:
            parsed = self.parse(yaml)
            labels = parsed[labels_type]
            self.assertEqual(2, len(labels))
            self.assertEqual(['key1_val1'], labels['key1']['value'])
            self.assertEqual(['key2_val1', 'key2_val2'],
                             labels['key2']['value'])

    def test_label_is_scanned(self):
        yaml_1 = """
tosca_definitions_version: cloudify_dsl_1_3

inputs:
    a:
        default: some_value

labels:
    concat:
        value:
            - { concat: ['a', 'b'] }
    get_input:
        value:
            - { get_input: a }
"""
        yaml_2 = """
tosca_definitions_version: cloudify_dsl_1_3

inputs:
    a:
        default: some_value

blueprint_labels:
    concat:
        value:
            - { concat: ['a', 'b'] }
    get_input:
        value:
            - { get_input: a }
"""
        for yaml, labels_type in [(yaml_1, constants.LABELS),
                                  (yaml_2, constants.BLUEPRINT_LABELS)]:
            plan = prepare_deployment_plan(self.parse(yaml))
            labels = plan[labels_type]
            self.assertEqual(['ab'], labels['concat']['value'])
            self.assertEqual(['some_value'], labels['get_input']['value'])

    def test_label_value_get_attribute_fail(self):
        yaml_1 = """
tosca_definitions_version: cloudify_dsl_1_3

labels:
    get_attribute:
        value:
          - val1
          - { get_attribute: [ node, attr ] }
"""
        yaml_2 = """
tosca_definitions_version: cloudify_dsl_1_3

blueprint_labels:
    get_attribute:
        value:
          - val1
          - { get_attribute: [ node, attr ] }
"""
        message_regex = '.*cannot be a runtime property.*'
        for yaml in yaml_1, yaml_2:
            self.assertRaisesRegex(exceptions.DSLParsingException,
                                   message_regex, self.parse, yaml)
