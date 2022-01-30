from dsl_parser import functions
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.exceptions import (UnknownSysEntityError,
                                   UnknownSysPropertyError)
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestGetSys(AbstractTestParser):
    def setUp(self):
        super(TestGetSys, self).setUp()
        self.mock_storage = self.mock_evaluation_storage(id='dep1')

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
            property: { get_sys: [deployment, id] }
"""
        parsed = prepare_deployment_plan(self.parse_1_3(yaml))
        node = self.get_node_by_name(parsed, 'node')
        self.assertEqual({'get_sys': ['deployment', 'id']},
                         node['properties']['property'])

        functions.evaluate_functions(parsed, {}, self.mock_storage)
        self.assertEqual(node['properties']['property'], 'dep1')

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
            property: { get_sys: {} }
"""
        self.assertRaisesRegex(
            ValueError,
            "Illegal argument.* passed to get_sys",
            self.parse_1_3,
            yaml)

    def test_invalid_entity(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_sys: [a, b] }
"""
        self.assertRaisesRegex(
            UnknownSysEntityError,
            "get_sys function unable to determine entity: a",
            self.parse_1_3,
            yaml)

    def test_invalid_property(self):
        yaml = """
node_types:
    type:
        properties:
            property: {}
node_templates:
    node:
        type: type
        properties:
            property: { get_sys: [deployment, b] }
"""
        self.assertRaisesRegex(
            UnknownSysPropertyError,
            "get_sys function unable to determine property: deployment b",
            self.parse_1_3,
            yaml)
