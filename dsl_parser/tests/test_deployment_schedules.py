from datetime import datetime

from dsl_parser import exceptions
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.tests.abstract_test_parser import AbstractTestParser
from dsl_parser.constants import DEPLOYMENT_SETTINGS, DEFAULT_SCHEDULES
from cloudify.exceptions import NonRecoverableError

time_fmt = '%Y-%m-%d %H:%M:%S'


class TestDeploymentSchedules(AbstractTestParser):

    def test_deployment_schedules_schema(self):
        yaml_1 = """
deployment_settings:
    default_schedules:
        sc1:
            workflow: install
            since: '+4h'
            until: '+1y'
            recurring: 1w
            weekdays: [mo, TU]
            count: 5
        sc2:
            workflow: backup
            since: '12:00'
            until: '2035-1-1 19:30'
            timezone: EST
            recurring: '3 days'
            execution_arguments:
                allow_custom_parameters: False
"""
        parsed = self.parse(yaml_1)
        deployment_schedules = parsed[DEPLOYMENT_SETTINGS][DEFAULT_SCHEDULES]
        self.assertEqual(2, len(deployment_schedules))
        for schedule in deployment_schedules.values():
            datetime.strptime(schedule['since'], time_fmt)
            datetime.strptime(schedule['until'], time_fmt)

    def test_deployment_schedules_invalid_schema_field(self):
        yaml_1 = """
deployment_settings:
    default_schedules:
        sc1:
            workflow: some_workflow
            nonsuch: field
            since: '2000-1-1 12:00'
"""
        self.assertRaisesRegex(
            exceptions.DSLParsingFormatException,
            "'nonsuch' is not in schema",
            self.parse, yaml_1)

    def test_deployment_schedules_missing_workflow_id(self):
        yaml_1 = """
deployment_settings:
    default_schedules:
        sc1:
            since: '2000-1-1 12:00'
"""
        self.assertRaisesRegex(
            exceptions.DSLParsingFormatException,
            "'workflow' key is required but it is currently missing",
            self.parse, yaml_1)

    def test_deployment_schedules_missing_since(self):
        yaml_1 = """
deployment_settings:
    default_schedules:
        sc1:
            workflow: some_workflow
"""
        self.assertRaisesRegex(
            exceptions.DSLParsingFormatException,
            "'since' key is required but it is currently missing",
            self.parse, yaml_1)

    def test_deployment_schedules_bad_weekday(self):
        yaml_1 = """
deployment_settings:
    default_schedules:
        sc1:
            workflow: some_workflow
            since: '2000-1-1 12:00'
            weekdays: ['Fr', 'xy']
"""
        self.assertRaisesRegex(
            exceptions.DSLParsingException,
            "xy is not a valid weekday value. Accepted values:",
            self.parse, yaml_1)

    def test_deployment_schedules_bad_recurrence_string(self):
        yaml_1 = """
deployment_settings:
    default_schedules:
        sc1:
            workflow: install
            since: '+1d +1h'
            recurring: blah
"""
        self.assertRaisesRegex(
            NonRecoverableError,
            "blah is not a legal time delta",
            self.parse, yaml_1)

    def test_deployment_schedules_is_scanned(self):
        yaml_1 = """
tosca_definitions_version: cloudify_dsl_1_3
inputs:
    a:
        default: sample
deployment_settings:
    default_schedules:
        sc1:
            workflow: install
            since: '2020-1-1 7:40'
            workflow_parameters:
                key1: { get_input: a }
"""
        plan = prepare_deployment_plan(self.parse(yaml_1))
        schedule = plan[DEPLOYMENT_SETTINGS][DEFAULT_SCHEDULES]['sc1']
        self.assertEqual('sample', schedule['workflow_parameters']['key1'])

    def test_deployment_schedules_invalid_workflow_parameters(self):
        yaml_1 = """
deployment_settings:
    default_schedules:
        sc1:
            workflow: install
            since: '2020-1-1 7:40'
            workflow_parameters:
                get_attribute: [a, b, c]
"""
        yaml_2 = """
deployment_settings:
    default_schedules:
        sc1:
            workflow: install
            since: '2020-1-1 7:40'
            workflow_parameters:
                key1: val1
                key2: {get_attribute: [a, b, c]}
        """
        self.assertRaisesRegex(
            exceptions.DSLParsingException,
            "`workflow_parameters` cannot be a runtime property.",
            self.parse, yaml_1)
        self.assertRaisesRegex(
            exceptions.DSLParsingException,
            "`workflow_parameters` cannot contain a runtime property. "
            "Please remove the `get_attribute` function from the value "
            "of key2",
            self.parse, yaml_2)
