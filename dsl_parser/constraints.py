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

from dsl_parser._compat import text_type
from dsl_parser import functions, exceptions
from dsl_parser.constants import (
    INPUTS,
    DEFAULT,
    CONSTRAINTS as CONSTRAINT_CONST
)

_NOT_COMPARABLE_ERROR_MSG = "Value is not comparable, the Constraint " \
                            "argument type  is '{0}' but value type is '{1}'."
_NO_LENGTH_ERROR_MSG = "Value's length could not be computed. Value " \
                       "type is '{0}'."

# Constraint types:
#  scalar (int/float/long), dual scalar (list/tuple), sequence (list/tuple),
#  regex (string)
# A few examples:
# - dual scalar: [ -0.5, 2 ] or it could be ( 1, 10 ) or any sequence of two
#                scalars
# - sequence: ( 1, 0, 'something', False ) or it could
#             be [ 1, 0, 'something', False ]
_SCALAR, _DUAL_SCALAR, _SEQUENCE, _REGEX = [0, 1, 2, 3]

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

    def __init__(self, args):
        """
        :param args: the constraint arguments.
        """
        self.args = args

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
class ValidValues(Constraint):

    def predicate(self, value):
        return _try_predicate_func(
            lambda: value in self.args,
            "Value cannot be searched in the given "
            "valid values. Constraint argument type "
            "is '{0}' but value type is "
            "'{1}'.".format(type(self.args).__name__, type(value).__name__))


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
        if not isinstance(value, text_type):
            raise exceptions.ConstraintException(
                "Value must be of type string, got type "
                "'{0}'".format(type(value).__name__))
        return bool(re.match(self.args, value))


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
    if not isinstance(arg, text_type):
        return False
    try:
        re.compile(arg)
        return True
    except re.error:
        return False


def validate_args(constraint_cls, args, ancestor):
    """Validates the given constraint class arguments.

    :param constraint_cls: a Constraint class.
    :param args: the constraint operator arguments.
    :param ancestor: ancestor element of this constraint.
    :raises DSLParsingFormatException: in case of a violation.
    """
    if functions.is_function(args) \
            or not VALIDATION_FUNCTIONS[
                constraint_cls.constraint_data_type](args):
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
    name, args = dict(definition).popitem()
    constraint_cls = CONSTRAINTS[name]
    return constraint_cls(args)


def validate_input_value(input_name, input_constraints, input_value):
    if input_constraints and functions.is_function(input_value):
        raise exceptions.DSLParsingException(
            exceptions.ERROR_INPUT_WITH_FUNCS_AND_CONSTRAINTS,
            'Input value {0}, of input {1}, cannot contain an intrinsic '
            'function and also have '
            'constraints.'.format(input_value, input_name))
    for c in input_constraints:
        if not c.predicate(input_value):
            raise exceptions.ConstraintException(
                "Value {0} of input {1} violates constraint "
                "{2}.".format(input_value, input_name, c))


def validate_input_defaults(plan):
    if INPUTS not in plan:
        return
    for input_name, input_def in plan[INPUTS].items():
        if DEFAULT not in input_def:
            continue
        input_default_value = input_def[DEFAULT]
        input_constraints = extract_constraints(input_def)
        validate_input_value(
            input_name, input_constraints, input_default_value)


def extract_constraints(input_def):
    """
    :param input_def: an input definition.
    :return: Constraint instances in respect to the constraint defined in the
    input definition.
    """
    return [parse(c) for c in input_def.get(CONSTRAINT_CONST, [])]
