########
# Copyright (c) 2019 Cloudify Platform Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#    * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    * See the License for the specific language governing permissions and
#    * limitations under the License.

import re
import numbers

from dsl_parser import exceptions, utils
from dsl_parser.constants import (
    INPUTS,
    DEFAULT,
    CONSTRAINTS as CONSTRAINT_CONST,
    TYPE,
    ITEM_TYPE,
    OBJECT_BASED_TYPES,
    BLUEPRINT_OR_DEPLOYMENT_ID_CONSTRAINT_TYPES,
    DEPLOYMENT_ID_CONSTRAINT_TYPES,
    ID_CONSTRAINT_TYPES,
)

_NOT_COMPARABLE_ERROR_MSG = "Value is not comparable, the Constraint " \
                            "argument type  is '{0}' but value type is '{1}'."
_NO_LENGTH_ERROR_MSG = "Value's length could not be computed. Value " \
                       "type is '{0}'."

# Constraint types:
#  scalar (int/float/long), dual scalar (list/tuple), sequence (list/tuple),
#  regex (string), string (string), dict (dictionary/hashmap)
# A few examples:
# - dual scalar: [ -0.5, 2 ] or it could be ( 1, 10 ) or any sequence of two
#                scalars
# - sequence: ( 1, 0, 'something', False ) or it could
#             be [ 1, 0, 'something', False ]
# - string: 'Quick brown fox jumps over the lazy dog'
# - dict: {'one': 1, 'two': [2, 22], 'three': 3.14}
_SCALAR, _DUAL_SCALAR, _SEQUENCE, _REGEX, _STRING, _DICT = [0, 1, 2, 3, 4, 5]

CONSTRAINTS = {}
VALIDATION_FUNCTIONS = {}


def register_constraint(constraint_data_type, name, cls=None):
    if cls is None:
        def partial(_cls):
            return register_constraint(
                constraint_data_type=constraint_data_type, cls=_cls, name=name)

        return partial
    CONSTRAINTS[name] = cls
    cls.name = name
    cls.constraint_data_type = constraint_data_type
    return cls


def register_validation_func(constraint_data_type, f=None):
    if f is None:
        def partial(_f):
            return register_validation_func(constraint_data_type, _f)
        return partial
    VALIDATION_FUNCTIONS[constraint_data_type] = f
    return f


def _try_predicate_func(predicate_function, err_msg):
    """Tries to return the value of the predicate func `p_func`, if it catches
     a TypeError it raises a ConstraintException instead.

    :param predicate_function: the actual predicate function of a constraint,
     the one that checks if a given value complies with the constraint. E.g.
     value == 2 for an Equal(2) constraint.
    :param err_msg: an error message to initialize the ConstraintException
     with, in case TypeError is raised during the `p_func` runtime.
    :return: whatever `p_func` returns.
    """
    try:
        return predicate_function()
    except TypeError:
        raise exceptions.ConstraintException(err_msg)


class Constraint(object):
    """An constraint operator base class, all classes that implement this
    one must be registered as the other classes below.
    """

    # The name of the operator as it appears in the YAML file.
    name = None
    # The data type of this constraint, what type of arguments it
    # accepts at it's initialization/definition.
    constraint_data_type = None

    def __init__(self, args, error_message=None):
        """
        :param args: the constraint arguments.
        """
        self.args = args
        self.error_message = error_message

    def predicate(self, value):
        """Value compliance hook.

        :param value: value to check the constraint with.
        :return: whether the value complies with this constraint.
        """
        raise NotImplementedError()

    def __str__(self):
        return "{0}({1}) operator".format(
            self.name,
            ', '.join(str(arg) for arg in self.args)
            if isinstance(self.args, (list, tuple)) else self.args)

    def data_based(self, data_type=None):
        return False


class DataBasedConstraint(Constraint):
    SUPPORTED_DATA_TYPES = []

    def predicate(self, value):
        raise NotImplementedError()

    def data_based(self, data_type=None):
        return True

    def type_check(self, data_type):
        if data_type not in self.SUPPORTED_DATA_TYPES:
            raise exceptions.ConstraintException(
                "'{0}' constraint is not defined for "
                "'{1}' data types".format(self.name, data_type))

    def query_param(self, data_type=None):
        return self.name


@register_constraint(name='equal', constraint_data_type=_SCALAR)
class Equal(Constraint):
    def predicate(self, value):
        return _try_predicate_func(
            lambda: value == self.args,
            _NOT_COMPARABLE_ERROR_MSG.format(
                type(self.args).__name__, type(value).__name__))


@register_constraint(name='greater_than', constraint_data_type=_SCALAR)
class GreaterThan(Constraint):

    def predicate(self, value):
        return _try_predicate_func(
            lambda: value > self.args,
            _NOT_COMPARABLE_ERROR_MSG.format(
                type(self.args).__name__, type(value).__name__))


@register_constraint(name='greater_or_equal', constraint_data_type=_SCALAR)
class GreaterOrEqual(Constraint):

    def predicate(self, value):
        return _try_predicate_func(
            lambda: value >= self.args,
            _NOT_COMPARABLE_ERROR_MSG.format(
                type(self.args).__name__, type(value).__name__))


@register_constraint(name='less_than', constraint_data_type=_SCALAR)
class LessThan(Constraint):

    def predicate(self, value):
        return _try_predicate_func(
            lambda: value < self.args,
            _NOT_COMPARABLE_ERROR_MSG.format(
                type(self.args).__name__, type(value).__name__))


@register_constraint(name='less_or_equal', constraint_data_type=_SCALAR)
class LessOrEqual(Constraint):

    def predicate(self, value):
        return _try_predicate_func(
            lambda: value <= self.args,
            _NOT_COMPARABLE_ERROR_MSG.format(
                type(self.args).__name__, type(value).__name__))


@register_constraint(name='in_range', constraint_data_type=_DUAL_SCALAR)
class InRange(Constraint):

    def predicate(self, value):
        return _try_predicate_func(
            lambda: self.args[0] <= value <= self.args[1],
            _NOT_COMPARABLE_ERROR_MSG.format(
                type(self.args).__name__, type(value).__name__))


@register_constraint(name='valid_values', constraint_data_type=_SEQUENCE)
class ValidValues(DataBasedConstraint):
    SUPPORTED_DATA_TYPES = ['capability_value', 'scaling_group',
                            'node_id', 'node_type', 'node_instance',
                            'operation_name', ]

    def predicate(self, value):
        return _try_predicate_func(
            lambda: value in self.args,
            "Value cannot be searched in the given "
            "valid values. Constraint argument type "
            "is '{0}' but value type is "
            "'{1}'.".format(type(self.args).__name__, type(value).__name__))

    def data_based(self, data_type=None):
        if data_type in self.SUPPORTED_DATA_TYPES:
            return True
        return False

    def type_check(self, data_type):
        if data_type not in self.SUPPORTED_DATA_TYPES:
            raise exceptions.ConstraintException(
                "'{0}' constraint is not defined for "
                "'{1}' data types".format(self.name, data_type))

    def query_param(self, data_type=None):
        return self.name


@register_constraint(name='length', constraint_data_type=_SCALAR)
class Length(Constraint):

    def predicate(self, value):
        return _try_predicate_func(
            lambda: len(value) == self.args,
            _NO_LENGTH_ERROR_MSG.format(
                type(value).__name__))


@register_constraint(name='min_length', constraint_data_type=_SCALAR)
class MinLength(Constraint):

    def predicate(self, value):
        return _try_predicate_func(
            lambda: len(value) >= self.args,
            _NO_LENGTH_ERROR_MSG.format(type(value).__name__))


@register_constraint(name='max_length', constraint_data_type=_SCALAR)
class MaxLength(Constraint):

    def predicate(self, value):
        return _try_predicate_func(
            lambda: len(value) <= self.args,
            _NO_LENGTH_ERROR_MSG.format(type(value).__name__))


@register_constraint(name='pattern', constraint_data_type=_REGEX)
class Pattern(Constraint):
    # This class represents a regex constraint.
    # E.g. if self.args = 'abc' then calling `predicate` will only return True
    # when value = "abc".
    def predicate(self, value):
        if not isinstance(value, str):
            raise exceptions.ConstraintException(
                "Value must be of type string, got type "
                "'{0}'".format(type(value).__name__))
        return bool(re.match(self.args, value))


@register_constraint(name='filter_id', constraint_data_type=_STRING)
class FilterId(DataBasedConstraint):
    SUPPORTED_DATA_TYPES = ['deployment_id', 'blueprint_id']


@register_constraint(name='labels', constraint_data_type=_SEQUENCE)
class Labels(DataBasedConstraint):
    SUPPORTED_DATA_TYPES = ['deployment_id', 'blueprint_id']


@register_constraint(name='tenants', constraint_data_type=_SEQUENCE)
class Tenants(DataBasedConstraint):
    SUPPORTED_DATA_TYPES = ['deployment_id', 'blueprint_id']


@register_constraint(name='name_pattern', constraint_data_type=_DICT)
class NamePattern(DataBasedConstraint):
    SUPPORTED_DATA_TYPES = ['deployment_id', 'blueprint_id', 'secret_key',
                            'capability_value', 'scaling_group',
                            'node_id', 'node_type', 'node_instance',
                            'operation_name', ]

    def query_param(self, data_type=None):
        if data_type == 'blueprint_id':
            return 'id_specs'
        elif data_type == 'deployment_id':
            return 'display_name_specs'
        elif data_type == 'secret_key':
            return 'key_specs'
        elif data_type == 'node_id':
            return 'id_specs'
        elif data_type == 'node_type':
            return 'type_specs'
        elif data_type == 'node_instance':
            return 'id_specs'
        elif data_type == 'capability_value':
            return 'capability_key_specs'
        elif data_type == 'scaling_group':
            return 'scaling_group_name_specs'
        elif data_type == 'operation_name':
            return 'operation_name_specs'
        else:
            raise NotImplementedError(
                "'{0}' constraint is not implemented for data type '{1}'"
                .format(self.name, data_type)
            )


@register_constraint(name='deployment_id', constraint_data_type=_STRING)
class DeploymentId(DataBasedConstraint):
    SUPPORTED_DATA_TYPES = ['capability_value', 'scaling_group',
                            'node_id', 'node_type', 'node_instance',
                            'operation_name', ]


@register_validation_func(constraint_data_type=_SCALAR)
def is_valid_scalar(arg):
    return isinstance(arg, numbers.Number) and not isinstance(arg, bool)


@register_validation_func(constraint_data_type=_DUAL_SCALAR)
def is_valid_dual_scalar(args):
    return is_valid_sequence(args) and len(args) == 2 \
           and is_valid_scalar(args[0]) \
           and is_valid_scalar(args[1])


@register_validation_func(constraint_data_type=_SEQUENCE)
def is_valid_sequence(args):
    return isinstance(args, (list, tuple))


@register_validation_func(constraint_data_type=_REGEX)
def is_valid_regex(arg):
    if not isinstance(arg, str):
        return False
    try:
        re.compile(arg)
        return True
    except re.error:
        return False


@register_validation_func(constraint_data_type=_STRING)
def is_string(arg):
    return isinstance(arg, str)


@register_validation_func(constraint_data_type=_DICT)
def is_dict(arg):
    return isinstance(arg, dict)


def validate_args(constraint_cls, args, ancestor):
    """Validates the given constraint class arguments.

    :param constraint_cls: a Constraint class.
    :param args: the constraint operator arguments.
    :param ancestor: ancestor element of this constraint.
    :raises DSLParsingFormatException: in case of a violation.
    """
    if utils.get_function(args):
        # allow functions to be constraint values. Those need to be
        # evaluated before actually checking the constraints (when creating
        # the deployment, with concrete input values).
        return
    if not VALIDATION_FUNCTIONS[constraint_cls.constraint_data_type](args):
        raise exceptions.DSLParsingLogicException(
            exceptions.ERROR_INVALID_CONSTRAINT_ARGUMENT,
            'Invalid constraint operator argument "{0}", for constraint '
            'operator "{1}" in '
            '"{2}".'.format(args, constraint_cls.name, ancestor))


def parse(definition):
    """
    :param definition: a Constraint definition. A constraint is a dictionary
    with a single key-value pair, i.e. { constraint_operator_name: argument/s }
    :return: an instantiated Constraint.
    """
    kwargs = {'error_message': definition.pop('error_message', None)}
    name, args = dict(definition).popitem()
    constraint_cls = CONSTRAINTS[name]
    return constraint_cls(args, **kwargs)


def validate_input_value(input_name, input_constraints, input_value,
                         type_name, item_type_name, value_getter):
    if type_name != 'list' or not item_type_name:
        return _validate_input_value(
            input_name, input_constraints, input_value,
            type_name, value_getter)
    for item_value in input_value:
        _validate_type_match(input_name, item_value, item_type_name)
        _validate_input_value(input_name, input_constraints, item_value,
                              item_type_name, value_getter)


def _validate_type_match(input_name, input_value, type_name):
    _, valid = utils.parse_simple_type_value(input_value, type_name)
    if not valid:
        raise exceptions.DSLParsingLogicException(
            exceptions.ERROR_VALUE_DOES_NOT_MATCH_TYPE,
            "Property type validation failed in '{0}': the defined "
            "type is '{1}', yet it was assigned with the "
            "value '{2}'".format(input_name, type_name, input_value)
        )


def _validate_input_value(input_name, input_constraints, input_value,
                          type_name, value_getter):
    if input_constraints and utils.get_function(input_value):
        raise exceptions.DSLParsingException(
            exceptions.ERROR_INPUT_WITH_FUNCS_AND_CONSTRAINTS,
            'Input value {0}, of input {1}, cannot contain an intrinsic '
            'function and also have '
            'constraints.'.format(input_value, input_name))

    if type_name in OBJECT_BASED_TYPES:
        if not value_getter:
            raise exceptions.ConstraintException(
                'Invalid call to validate_input_value: value_getter not set')
        if type_name in BLUEPRINT_OR_DEPLOYMENT_ID_CONSTRAINT_TYPES:
            if value_getter.has_deployment_id() \
                    or 'deployment_id' in {c.name for c in input_constraints}:
                pass
            elif value_getter.has_blueprint_id()\
                    or 'blueprint_id' in {c.name for c in input_constraints}:
                pass
            else:
                raise exceptions.ConstraintException(
                    f"Input '{input_name}' of type '{type_name}' lacks "
                    "'blueprint_id' or 'deployment_id' constraint.")
        if type_name in DEPLOYMENT_ID_CONSTRAINT_TYPES \
                and not value_getter.has_deployment_id() \
                and 'deployment_id' not in {c.name for c in input_constraints}:
            raise exceptions.ConstraintException(
                "Input '{0}' of type '{1}' lacks 'deployment_id' constraint."
                .format(input_name, type_name))
        if type_name not in ID_CONSTRAINT_TYPES \
                or ('deployment_id' not in {c.name for c in input_constraints}
                    and value_getter.has_deployment_id()):
            matching_values = value_getter.get(type_name, input_value)
            if not any(v == input_value for v in matching_values or []):
                raise exceptions.ConstraintException(
                    "Value '{0}' of '{1}' is not a valid value for data type "
                    "'{2}'.".format(input_value, input_name, type_name))

    data_based_constraints = []
    for c in input_constraints:
        if c.data_based(type_name):
            data_based_constraints.append(c)
            continue
        if not c.predicate(input_value):
            msg = f'is invalid. {c.error_message}' if c.error_message \
                else f'violates constraint {c}.'
            raise exceptions.ConstraintException(
                f"Value {input_value} of input {input_name} {msg}")
    if ((type_name in DEPLOYMENT_ID_CONSTRAINT_TYPES
         or data_based_constraints)
        and not predicate_many(input_value,
                               type_name,
                               value_getter,
                               data_based_constraints)):
        raise exceptions.ConstraintException(
            "Value '{0}' of input '{1}' does not match any relevant entity "
            "or violates at least one of the constraints: {2}."
            .format(input_value, input_name,
                    ", ".join(str(c) for c in data_based_constraints))
        )


def build_data_based_constraints_query(type_name, constraints):
    params = {}
    for c in constraints:
        c.type_check(type_name)
        params[c.query_param(type_name)] = c.args
    return params


def predicate_many(input_value, type_name, value_getter, constraints):
    params = build_data_based_constraints_query(type_name, constraints)
    matching_values = value_getter.get(type_name, input_value, **params)
    return any(v == input_value for v in matching_values or [])


def validate_input_defaults(plan):
    if INPUTS not in plan:
        return
    for input_name, input_def in plan[INPUTS].items():
        if DEFAULT not in input_def:
            continue
        input_default_value = input_def[DEFAULT]
        input_constraints = extract_constraints(input_def)
        validate_input_value(input_name, input_constraints,
                             input_default_value,
                             input_def.get(TYPE), input_def.get(ITEM_TYPE),
                             None)


def extract_constraints(input_def):
    """
    :param input_def: an input definition.
    :return: Constraint instances in respect to the constraint defined in the
    input definition.
    """
    return [parse(c) for c in input_def.get(CONSTRAINT_CONST, [])]
