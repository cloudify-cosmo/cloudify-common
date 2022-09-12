from dsl_parser import functions
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestGetConsumers(AbstractTestParser):
    def setUp(self):
        super(TestGetConsumers, self).setUp()
        self.mock_storage = self.mock_evaluation_storage(
            consumers={
                'app1': 'App1',
                'app2': 'My Second App',
                'app3': 'App #3'
            })

    def test_node_template_properties_simple(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_consumers: ids }
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        node = self.get_node_by_name(parsed, 'node')
        self.assertEqual({'get_consumers': 'ids'},
                         node['properties']['property'])

        functions.evaluate_functions(parsed, {}, self.mock_storage)
        self.assertEqual(set(node['properties']['property']),
                         {'app1', 'app2', 'app3'})

    def test_consumers_in_outputs(self):
        yaml = """
node_types:
    type: {}
node_templates:
    node:
        type: type
outputs:
    consumers:
        value: { get_consumers: ids }
    consumer_count:
        value: { get_consumers: count }
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        outputs = parsed.outputs
        self.assertEqual({'get_consumers': 'ids'},
                         outputs['consumers']['value'])

        functions.evaluate_functions(parsed, {}, self.mock_storage)
        self.assertEqual(set(outputs['consumers']['value']),
                         {'app1', 'app2', 'app3'})
        self.assertEqual(outputs['consumer_count']['value'], 3)

    def test_consumers_in_inputs(self):
        yaml = """
inputs:
    consumer_count:
        default: { get_consumers: count }
    consumer_names:
        default: { get_consumers: names }
node_types:
    type: {}
node_templates:
    node:
        type: type
outputs:
    consumers:
        value: { get_input: consumer_names }
    consumer_count:
        value: { get_input: consumer_count }
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        outputs = parsed.outputs

        # `get_input` is evaluated at parse time, so we expect to see it
        # replaced here with the `get_consumers_count` function
        self.assertEqual({'get_consumers': 'count'},
                         outputs['consumer_count']['value'])

        functions.evaluate_functions(parsed, {}, self.mock_storage)
        self.assertEqual(outputs['consumer_count']['value'], 3)
        self.assertEqual(set(outputs['consumers']['value']),
                         {'App1', 'My Second App', 'App #3'})

    def test_illegal_arguments(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_consumers: [a, b] }
"""
        self.assertRaisesRegex(
            ValueError,
            "Illegal argument passed to get_consumers",
            self.parse_1_3,
            yaml)
