import re
from datetime import datetime

from dsl_parser import exceptions
from dsl_parser.framework.elements import (DictElement,
                                           DictNoDefaultElement,
                                           Element,
                                           Leaf,
                                           List,
                                           Dict)
from cloudify.utils import parse_utc_datetime_absolute, unpack_timedelta_string


class ScheduleEnabled(Element):
    schema = Leaf(type=bool)


class ScheduleStopAfterFail(Element):
    schema = Leaf(type=bool)


class ScheduleCount(Element):
    schema = Leaf(type=int)


class ScheduleName(Element):
    required = True
    schema = Leaf(type=str)


class DateTimeExpression(Element):
    schema = Leaf(type=str)


class RequiredDateTimeExpression(DateTimeExpression):
    required = True


class TimeDelta(Element):
    schema = Leaf(type=str)

    def validate(self):
        if self.initial_value:
            unpack_timedelta_string(self.initial_value)


class TimeZoneExpression(Element):
    schema = Leaf(type=str)


class ScheduledWorkflowId(Element):
    required = True
    schema = Leaf(type=str)


class ScheduledWorkflowForce(Element):
    schema = Leaf(type=bool)


class ScheduledWorkflowDryRun(Element):
    schema = Leaf(type=bool)


class ScheduledWorkflowWaitAfterFail(Element):
    schema = Leaf(type=int)


class ScheduledWorkflowAllowCustomParameters(Element):
    schema = Leaf(type=bool)


class ScheduledWorkflowArguments(DictNoDefaultElement):
    schema = {
        'force': ScheduledWorkflowForce,
        'is_dry_run': ScheduledWorkflowDryRun,
        'wait_after_fail': ScheduledWorkflowWaitAfterFail,
        'allow_custom_parameters': ScheduledWorkflowAllowCustomParameters
    }


class ScheduledWorkflowParameters(Element):
    schema = Leaf(type=dict)

    def validate(self, **kwargs):
        """
        Deployment schedules are assigned to a deployment during its creation,
        so they cannot cannot contain runtime properties.
        """
        err_msg = "`workflow_parameters` cannot {0} a runtime property." \
                  " Please remove the `get_attribute` function{1}"

        if self.initial_value:
            if 'get_attribute' in self.initial_value:
                raise exceptions.DSLParsingException(
                    1, err_msg.format('be', ''))
            for key, value in self.initial_value.items():
                if isinstance(value, dict) and 'get_attribute' in value:
                    value_msg = ' from the value of {}'.format(key)
                    raise exceptions.DSLParsingException(
                        1, err_msg.format('contain', value_msg))


class Weekday(Element):
    schema = Leaf(type=str)

    def validate(self):
        weekdays_list = ['su', 'mo', 'tu', 'we', 'th', 'fr', 'sa']
        err_msg = "{0} is not a valid weekday value. Accepted values: {1}"
        weekday_value = self.initial_value.lower()[-2:]
        if weekday_value not in weekdays_list:
            raise exceptions.DSLParsingException(
                1, err_msg.format(weekday_value, weekdays_list))


class ScheduleWeekdays(Element):
    schema = List(type=Weekday)


class ScheduleSlip(Element):
    schema = Leaf(type=int)


class DeploymentSchedule(DictNoDefaultElement):
    schema = {
        'workflow': ScheduledWorkflowId,
        'execution_arguments': ScheduledWorkflowArguments,
        'workflow_parameters': ScheduledWorkflowParameters,
        'since': RequiredDateTimeExpression,
        'until': DateTimeExpression,
        'recurrence': TimeDelta,
        'timezone': TimeZoneExpression,
        'weekdays': ScheduleWeekdays,
        'count': ScheduleCount,
        'default_enabled': ScheduleEnabled,
        'stop_after_fail': ScheduleStopAfterFail,
        'slip': ScheduleSlip
    }

    @staticmethod
    def parse_time_expression(time_expression, timezone):
        if time_expression.startswith('+'):
            deltas = re.findall(r"(\+\d+\ ?[a-z]+\ ?)", time_expression)
            for delta in deltas:
                unpack_timedelta_string(delta[1:])
            return time_expression
        return datetime.strftime(
            parse_utc_datetime_absolute(time_expression, timezone),
            '%Y-%m-%d %H:%M:%S')

    def parse(self):
        timezone = self.initial_value.get('timezone')
        since = self.initial_value['since']
        until = self.initial_value.get('until')
        return_dict = self.initial_value.copy()

        # calculate naive UTC datetime from time expressions in since and until
        return_dict['since'] = self.parse_time_expression(since, timezone)
        if until:
            return_dict['until'] = self.parse_time_expression(until, timezone)
        if 'timezone' in return_dict:
            del return_dict['timezone']
        return return_dict


class DeploymentSchedules(DictElement):
    schema = Dict(type=DeploymentSchedule)
