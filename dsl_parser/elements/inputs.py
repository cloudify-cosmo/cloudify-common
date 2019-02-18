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
from dsl_parser.elements.data_types import (Schema,
                                            SchemaProperty,
                                            SchemaInputType,
                                            SchemaPropertyDefault,
                                            SchemaPropertyRequired,
                                            SchemaPropertyDescription)
from dsl_parser.framework.elements import Element, Leaf, List, Dict
from dsl_parser.framework.requirements import Requirement, sibling_predicate


class Constraint(Element):
    schema = Leaf(type=dict)

    add_namespace_to_schema_elements = False

    def validate(self, **kwargs):
        constraint_op_keys = self.initial_value.keys()
        if not constraint_op_keys:
            raise exceptions.DSLParsingLogicException(
                exceptions.ERROR_UNKNOWN_TYPE,
                "Empty constraint operator name given. Allowed operators: "
                "{}".format(constraints.CONSTRAINTS))
        if len(constraint_op_keys) > 1:
            raise exceptions.DSLParsingLogicException(
                exceptions.ERROR_UNKNOWN_TYPE,
                "Each constraint operator dict must be in it's own list item.")
        constraint_name = constraint_op_keys[0]
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


class InputSchemaProperty(SchemaProperty):
    schema = {
        'required': SchemaPropertyRequired,
        'default': InputSchemaPropertyDefault,
        'description': SchemaPropertyDescription,
        'type': SchemaInputType,
        'constraints': Constraints
    }

    def parse(self):
        return self.build_dict_result(with_default=False)


class Inputs(Schema):
    schema = Dict(type=InputSchemaProperty)
