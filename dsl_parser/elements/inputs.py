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

from dsl_parser import exceptions, constraints, utils
from dsl_parser.elements import version as _version
from dsl_parser.elements.data_types import (Schema,
                                            SchemaProperty,
                                            SchemaInputType,
                                            SchemaListItemType,
                                            SchemaPropertyDefault,
                                            SchemaPropertyDescription,
                                            SchemaPropertyDisplayLabel,
                                            SchemaPropertyHidden,
                                            SchemaPropertyRequired,
                                            )
from dsl_parser.framework.elements import Element, Leaf, List, Dict
from dsl_parser.framework.requirements import Requirement, sibling_predicate
from dsl_parser.version import version_description


class Constraint(Element):
    schema = Leaf(type=dict)

    add_namespace_to_schema_elements = False

    requires = {
        _version.ToscaDefinitionsVersion: ['version'],
        'inputs': ['validate_version']
    }

    def validate(self, version, validate_version, **kwargs):
        constraint_op_keys = list(self.initial_value)
        if not constraint_op_keys:
            raise exceptions.DSLParsingLogicException(
                exceptions.ERROR_UNKNOWN_TYPE,
                "Empty constraint operator name given. Allowed operators: "
                "{0}".format(constraints.CONSTRAINTS))
        if len(constraint_op_keys) > 1:
            if validate_version:
                self.validate_version(
                    version, (1, 5),
                    'Constraint\'s additional attributes are not supported '
                    f'in version {version_description(version)}, they were '
                    f'added in {version_description((1, 5))}')
            else:
                raise exceptions.DSLParsingLogicException(
                    exceptions.ERROR_UNKNOWN_TYPE,
                    "Each constraint operator dict must be in it's own list "
                    "item.")
        constraint_name = [cn for cn in constraint_op_keys
                           if cn != 'error_message'][0]
        if constraint_name not in constraints.CONSTRAINTS:
            raise exceptions.DSLParsingLogicException(
                exceptions.ERROR_UNKNOWN_TYPE,
                "No such constraint operator '{0}'. Allowed operators: "
                "{1}".format(constraint_name, constraints.CONSTRAINTS))
        ancestor = self.ancestor(SchemaProperty).name
        constraints.validate_args(
            constraints.CONSTRAINTS[constraint_name],
            self.initial_value[constraint_name],
            ancestor)


class InputSchemaPropertyDefault(SchemaPropertyDefault):
    requires = {
        SchemaInputType: [Requirement('component_types',
                                      required=False,
                                      predicate=sibling_predicate)]
    }

    def parse(self, component_types):
        initial_value = self.initial_value
        if initial_value is None:
            return None
        type_name = self.sibling(SchemaInputType).value
        component_types = component_types or {}
        undefined_property_error = "Undefined property {1} in default value " \
                                   "of input {0}."
        input_name = self.ancestor(InputSchemaProperty).name
        return utils.parse_value(
            value=initial_value,
            type_name=type_name,
            data_types=component_types,
            undefined_property_error_message=undefined_property_error,
            missing_property_error_message="Value of input {0} is missing "
                                           "property {1}.",
            node_name=input_name,
            path=[],
            raise_on_missing_property=True)


class Constraints(Element):
    schema = List(type=Constraint)


class DisplayRows(Element):
    schema = Leaf(type=int)


class DisplayHints(Element):
    schema = {
        'rows': DisplayRows,
    }

    def validate(self, **kwargs):
        if not self.children():
            return

        data_type = self.sibling(SchemaInputType)
        if data_type.initial_value == 'textarea':
            return

        parent = self.ancestor(InputSchemaProperty)
        raise exceptions.DSLParsingInputTypeException(
            exceptions.ERROR_DISPLAY_FOR_INVALID_TYPE,
            '`display` property defined for unsupported input type ({0}) '
            'for input `{1}`.'
            .format(data_type.initial_value, parent.name))


class InputSchemaProperty(SchemaProperty):
    schema = {
        'required': SchemaPropertyRequired,
        'default': InputSchemaPropertyDefault,
        'description': SchemaPropertyDescription,
        'type': SchemaInputType,
        'item_type': SchemaListItemType,
        'constraints': Constraints,
        'display_label': SchemaPropertyDisplayLabel,
        'hidden': SchemaPropertyHidden,
        'display': DisplayHints,
    }

    def parse(self):
        return self.build_dict_result(with_default=False)

    def validate(self):
        if self.initial_value and self.initial_value.get('required', True) \
                and self.initial_value.get('hidden', False) \
                and 'default' not in self.initial_value:
            raise exceptions.DSLParsingLogicException(
                exceptions.ERROR_HIDDEN_REQUIRED_INPUT_NO_DEFAULT,
                "Input is both hidden and required thus it should have a "
                "default value: '{0}'.".format(self.name))


class Inputs(Schema):
    schema = Dict(type=InputSchemaProperty)
