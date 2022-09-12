########
# Copyright (c) 2018 Cloudify Platform Ltd. All rights reserved
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

from dsl_parser import constants, constraints, exceptions, utils
from dsl_parser.elements import (data_types,
                                 plugins as _plugins,
                                 operation,
                                 misc,
                                 version as _version)
from dsl_parser.framework.requirements import (Value,
                                               Requirement,
                                               sibling_predicate)
from dsl_parser.framework.elements import (DictElement,
                                           Element,
                                           Leaf,
                                           List,
                                           Dict)


class Constraint(Element):
    schema = Leaf(type=dict)

    add_namespace_to_schema_elements = False

    def validate(self, **kwargs):
        constraint_op_keys = list(self.initial_value)
        if not constraint_op_keys:
            raise exceptions.DSLParsingLogicException(
                exceptions.ERROR_UNKNOWN_TYPE,
                "Empty constraint operator name given. Allowed operators: "
                "{0}".format(constraints.CONSTRAINTS))
        if len(constraint_op_keys) > 1:
            raise exceptions.DSLParsingLogicException(
                exceptions.ERROR_UNKNOWN_TYPE,
                "Each constraint operator dict must be in its own list item.")
        constraint_name = constraint_op_keys[0]
        if constraint_name not in constraints.CONSTRAINTS:
            raise exceptions.DSLParsingLogicException(
                exceptions.ERROR_UNKNOWN_TYPE,
                "No such constraint operator '{0}'. Allowed operators: "
                "{1}".format(constraint_name, constraints.CONSTRAINTS))
        ancestor = self.ancestor(data_types.SchemaProperty).name
        constraints.validate_args(
            constraints.CONSTRAINTS[constraint_name],
            self.initial_value[constraint_name],
            ancestor)


class Constraints(Element):
    schema = List(type=Constraint)


class ParameterSchemaPropertyDefault(data_types.SchemaPropertyDefault):
    requires = {
        data_types.SchemaInputType: [Requirement('component_types',
                                     required=False,
                                     predicate=sibling_predicate)]
    }

    def parse(self, component_types):
        initial_value = self.initial_value
        if initial_value is None:
            return None
        type_name = self.sibling(data_types.SchemaInputType).value
        component_types = component_types or {}
        undefined_property_error = "Undefined property {1} in default value " \
                                   "of input {0}."
        input_name = self.ancestor(ParameterSchemaProperty).name
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


class ParameterSchemaProperty(data_types.SchemaProperty):
    schema = {
        'required': data_types.SchemaPropertyRequired,
        'default': ParameterSchemaPropertyDefault,
        'description': data_types.SchemaPropertyDescription,
        'type': data_types.SchemaInputType,
        'item_type': data_types.SchemaListItemType,
        'constraints': Constraints,
    }

    def parse(self):
        return self.build_dict_result(with_default=False)


class WorkflowMapping(Element):

    required = True
    schema = Leaf(type=str)


class WorkflowParameters(data_types.Schema):

    add_namespace_to_schema_elements = False
    schema = Dict(type=ParameterSchemaProperty)


class WorkflowIsCascading(Element):

    schema = Leaf(type=bool)
    add_namespace_to_schema_elements = False


class WorkflowAvailable(Element):
    schema = Leaf(type=bool)
    add_namespace_to_schema_elements = False


class WorkflowAvailabilityNodeInstancesActive(Element):
    schema = Leaf(type=list)
    add_namespace_to_schema_elements = False
    valid_values = ('all', 'partial', 'none')

    def validate(self, **kwargs):
        if self.initial_value and \
                any(value not in self.valid_values
                    for value in self.initial_value):
            raise exceptions.DSLParsingLogicException(
                exceptions.ERROR_UNKNOWN_TYPE,
                "Invalid definition of 'active_node_instances' availability "
                "rule: '{0}'. Allowed values: {1}"
                .format(self.initial_value, self.valid_values))


class NodeTypeName(Element):
    schema = Leaf(type=str)
    add_namespace_to_schema_elements = False


class WorkflowAvailabilityNodeTypesRequired(Element):
    schema = List(type=NodeTypeName)
    add_namespace_to_schema_elements = False


class WorkflowAvailabilityRules(DictElement):
    schema = {
        'available': WorkflowAvailable,
        'node_instances_active': WorkflowAvailabilityNodeInstancesActive,
        'node_types_required': WorkflowAvailabilityNodeTypesRequired,
    }
    add_namespace_to_schema_elements = False
    requires = {
        _version.ToscaDefinitionsVersion: ['version'],
        'inputs': ['validate_version']
    }

    def validate(self, version, validate_version):
        if validate_version:
            self.validate_version(version, (1, 4))


class Workflow(Element):

    required = True
    schema = [
        Leaf(type=str),
        {
            'mapping': WorkflowMapping,
            'parameters': WorkflowParameters,
            'is_cascading': WorkflowIsCascading,
            'availability_rules': WorkflowAvailabilityRules,
        }
    ]
    requires = {
        'inputs': [Requirement('resource_base', required=False)],
        _plugins.Plugins: [Value('plugins')],
        misc.NamespacesMapping: [Value(constants.NAMESPACES_MAPPING)]
    }

    def parse(self, plugins, resource_base, namespaces_mapping):
        if isinstance(self.initial_value, str):
            operation_content = {'mapping': self.initial_value,
                                 'parameters': {}}
            is_cascading = False
            availability_rules = None
        else:
            operation_content = self.build_dict_result()
            is_cascading = self.initial_value.get('is_cascading', False)
            availability_rules = self.initial_value.get('availability_rules')
        return operation.process_operation(
            plugins=plugins,
            operation_name=self.name,
            operation_content=operation_content,
            error_code=21,
            partial_error_message='',
            resource_bases=resource_base,
            remote_resources_namespaces=namespaces_mapping,
            is_workflows=True,
            is_workflow_cascading=is_cascading,
            workflow_availability=availability_rules,
        )


class Workflows(DictElement):

    schema = Dict(type=Workflow)
    requires = {
        _plugins.Plugins: [Value('plugins')]
    }
    provides = ['workflow_plugins_to_install']

    def calculate_provided(self, plugins):
        workflow_plugins = []
        workflow_plugin_names = set()
        for workflow, op_struct in self.value.items():
            if op_struct['plugin'] not in workflow_plugin_names:
                plugin_name = op_struct['plugin']
                workflow_plugins.append(plugins[plugin_name])
                workflow_plugin_names.add(plugin_name)
        return {
            'workflow_plugins_to_install': workflow_plugins
        }
