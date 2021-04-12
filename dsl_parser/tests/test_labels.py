from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser import constants, exceptions, functions
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
        values:
            - key1_val1
    key2:
        values:
            - key2_val1
            - key2_val2
"""
        yaml_2 = """
blueprint_labels:
    key1:
        values:
            - key1_val1
    key2:
        values:
            - key2_val1
            - key2_val2
"""
        for yaml, labels_type in [(yaml_1, constants.LABELS),
                                  (yaml_2, constants.BLUEPRINT_LABELS)]:
            parsed = self.parse(yaml)
            labels = parsed[labels_type]
            self.assertEqual(2, len(labels))
            self.assertEqual(['key1_val1'], labels['key1']['values'])
            self.assertEqual(['key2_val1', 'key2_val2'],
                             labels['key2']['values'])

    def test_label_is_scanned(self):
        yaml = """
tosca_definitions_version: cloudify_dsl_1_3

inputs:
    a:
        default: some_value

labels:
    concat:
        values:
            - { concat: ['a', 'b'] }
    get_input:
        values:
            - { get_input: a }
"""
        plan = prepare_deployment_plan(self.parse(yaml))
        labels = plan['labels']
        self.assertEqual(['ab'], labels['concat']['values'])
        self.assertEqual(['some_value'], labels['get_input']['values'])

    def test_label_value_get_attribute_fail(self):
        yaml = """
tosca_definitions_version: cloudify_dsl_1_3

labels:
    get_attribute:
        values:
          - val1
          - { get_attribute: [ node, attr ] }
"""

        message_regex = '.*cannot be a runtime property.*'
        self.assertRaisesRegex(exceptions.DSLParsingException,
                               message_regex, self.parse, yaml)

    def test_label_value_type_fail(self):
        yaml = """
tosca_definitions_version: cloudify_dsl_1_3

labels:
    get_attribute:
        values:
          - [val1, val2]
"""
        message_regex = '.*must be a string or an intrinsic function.*'
        self.assertRaisesRegex(exceptions.DSLParsingException,
                               message_regex, self.parse, yaml)

    def test_blueprint_label_value_intrinsic_function_fails(self):
        yaml = """
tosca_definitions_version: cloudify_dsl_1_3

blueprint_labels:
    get_attribute:
        values:
          - val1
          - { get_attribute: [ node, attr ] }
"""
        message_regex = "blueprint label's value must be a string."
        self.assertRaisesRegex(exceptions.DSLParsingException, message_regex,
                               self.parse, yaml)


class TestGetLabel(AbstractTestParser):
    def test_has_intrinsic_functions_property(self):
        yaml = """
relationships:
    cloudify.relationships.contained_in: {}
plugins:
    p:
        executor: central_deployment_agent
        install: false
node_types:
    webserver_type: {}
node_templates:
    node:
        type: webserver_type
    webserver:
        type: webserver_type
        interfaces:
            test:
                op_with_no_get_label:
                    implementation: p.p
                    inputs:
                        a: 1
                op_with_get_label:
                    implementation: p.p
                    inputs:
                        a: { get_label: node_template_key }
        relationships:
            -   type: cloudify.relationships.contained_in
                target: node
                source_interfaces:
                    test:
                        op_with_no_get_label:
                            implementation: p.p
                            inputs:
                                a: 1
                        op_with_get_label:
                            implementation: p.p
                            inputs:
                                a: { get_label: source_op_key }
                target_interfaces:
                    test:
                        op_with_no_get_label:
                            implementation: p.p
                            inputs:
                                a: 1
                        op_with_get_label:
                            implementation: p.p
                            inputs:
                                a: { get_label: target_op_key }
"""
        parsed = prepare_deployment_plan(self.parse(yaml))
        webserver_node = None
        for node in parsed.node_templates:
            if node['id'] == 'webserver':
                webserver_node = node
                break
        self.assertIsNotNone(webserver_node)

        def assertion(operations):
            op = operations['test.op_with_no_get_label']
            self.assertIs(False, op.get('has_intrinsic_functions'))
            op = operations['test.op_with_get_label']
            self.assertIs(True, op.get('has_intrinsic_functions'))

        assertion(webserver_node['operations'])
        assertion(webserver_node['relationships'][0]['source_operations'])
        assertion(webserver_node['relationships'][0]['target_operations'])

    def test_evaluate_functions(self):

        payload = {
            'a': {'get_label': 'key1'},
            'b': {'get_label': ['key1', 0]},
            'c': {'concat': [
                {'get_label': ['key1', 1]},
                {'get_label': ['key2', 1]},
            ]}
        }

        functions.evaluate_functions(
            payload, {}, self._mock_evaluation_storage())

        self.assertEqual(payload['a'], ['val1', 'val2'])
        self.assertEqual(payload['b'], 'val1')
        self.assertEqual(payload['c'], 'val2val4')

    def test_get_label_in_properties(self):
        yaml = """
node_types:
    type:
        properties:
            property_1: {}
            property_2: {}
node_templates:
    node:
        type: type
        properties:
            property_1: { get_label: key1 }
            property_2: { get_label: [key2, 0] }
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        node = self.get_node_by_name(parsed, 'node')
        self.assertEqual({'get_label': 'key1'},
                         node['properties']['property_1'])
        self.assertEqual({'get_label': ['key2', 0]},
                         node['properties']['property_2'])

        functions.evaluate_functions(
            parsed, {}, self._mock_evaluation_storage())
        self.assertEqual(node['properties']['property_1'], ['val1', 'val2'])
        self.assertEqual(node['properties']['property_2'], 'val3')

    def test_get_label_in_outputs(self):
        yaml = """
inputs:
    label_key:
        default: key1
node_types:
    type: {}
node_templates:
    node:
        type: type
outputs:
    output_1:
      value: { get_label: { get_input: label_key } }
    output_2:
      value: { get_label: [key2, 0] }
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        outputs = parsed.outputs
        self.assertEqual({'get_label': 'key1'}, outputs['output_1']['value'])
        self.assertEqual({'get_label': ['key2', 0]},
                         outputs['output_2']['value'])

        functions.evaluate_functions(
            parsed, {}, self._mock_evaluation_storage())
        self.assertEqual(outputs['output_1']['value'], ['val1', 'val2'])
        self.assertEqual(outputs['output_2']['value'], 'val3')

    def test_get_label_in_inputs(self):
        yaml = """
inputs:
    input_1:
        default: { get_label: [key1, 0] }
    input_2:
        default: { get_label: key2 }
node_types:
    type: {}
node_templates:
    node:
        type: type
outputs:
    output_1:
      value: { get_input: input_1 }

    output_2:
      value: { get_input: input_2 }
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        outputs = parsed.outputs

        # `get_input` is evaluated at parse time, so we expect to see it
        # replaced here with the `get_label` function
        self.assertEqual({'get_label': ['key1', 0]},
                         outputs['output_1']['value'])
        self.assertEqual({'get_label': 'key2'},
                         outputs['output_2']['value'])

        functions.evaluate_functions(
            parsed, {}, self._mock_evaluation_storage())
        self.assertEqual(outputs['output_1']['value'], 'val1')
        self.assertEqual(outputs['output_2']['value'], ['val3', 'val4'])

    def test_get_label_in_get_capability(self):
        yaml = """
node_types:
    type: {}
node_templates:
    node:
        type: type
outputs:
    output_1:
      value: { get_capability: [ { get_label: [key3, 0] }, cap_a ] }
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        outputs = parsed.outputs

        self.assertEqual({'get_capability': [{'get_label': ['key3', 0]},
                                             'cap_a']},
                         outputs['output_1']['value'])

        functions.evaluate_functions(
            parsed, {}, self._mock_evaluation_storage())
        self.assertEqual(outputs['output_1']['value'], 'value_a_1')

    def test_get_label_short_list(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_label: [ only_one_item ] }
"""
        self.assertRaisesRegex(
            exceptions.FunctionValidationError,
            '`get_label` function argument should be',
            self.parse_1_3,
            yaml)

    def test_get_label_invalid_label_key(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_label: [ [list] , value ] }
"""
        self.assertRaisesRegex(
            exceptions.FunctionValidationError,
            '<label-key> should be',
            self.parse_1_3,
            yaml)

    def test_get_label_invalid_label_values_index(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_label: [ key1 , val ] }
"""
        self.assertRaisesRegex(
            exceptions.FunctionValidationError,
            '<label-values list index> should be',
            self.parse_1_3,
            yaml)
