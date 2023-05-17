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

import copy
import numbers

from dsl_parser import (constants,
                        exceptions,
                        utils)
from dsl_parser.elements import (inputs,
                                 version as _version)
from dsl_parser.framework.elements import (DictElement,
                                           Element,
                                           Leaf,
                                           Dict)
from dsl_parser.interfaces.utils import (operation,
                                         no_op_operation)


class OperationImplementation(Element):

    schema = Leaf(type=str)

    def parse(self):
        return self.initial_value if self.initial_value is not None else ''


class OperationExecutor(Element):

    schema = Leaf(type=str)

    add_namespace_to_schema_elements = False

    def validate(self):
        if self.initial_value is None:
            return
        value = self.initial_value
        valid_executors = [constants.CENTRAL_DEPLOYMENT_AGENT,
                           constants.HOST_AGENT]
        if value not in valid_executors:
            full_operation_name = '{0}.{1}'.format(
                self.ancestor(Interface).name,
                self.ancestor(Operation).name)
            raise exceptions.DSLParsingLogicException(
                28, "Operation '{0}' has an illegal executor value '{1}'. "
                    "valid values are [{2}]"
                    .format(full_operation_name,
                            value,
                            ','.join(valid_executors)))


class NodeTypeOperationInputs(inputs.Inputs):
    add_namespace_to_schema_elements = False


class NodeTemplateOperationInputs(Element):

    schema = Leaf(type=dict)

    add_namespace_to_schema_elements = False

    def parse(self):
        return self.initial_value if self.initial_value is not None else {}


class OperationMaxRetries(Element):

    schema = Leaf(type=int)
    requires = {
        _version.ToscaDefinitionsVersion: ['version'],
        'inputs': ['validate_version']
    }

    def validate(self, version, validate_version):
        value = self.initial_value
        if value is None:
            return
        if validate_version:
            self.validate_version(version, (1, 1))
        if value < -1:
            raise ValueError("'{0}' value must be either -1 to specify "
                             "unlimited retries or a non negative number but "
                             "got {1}."
                             .format(self.name, value))


class OperationRetryInterval(Element):

    schema = Leaf(type=numbers.Number)
    requires = {
        _version.ToscaDefinitionsVersion: ['version'],
        'inputs': ['validate_version']
    }

    def validate(self, version, validate_version):
        value = self.initial_value
        if value is None:
            return
        if validate_version:
            self.validate_version(version, (1, 1))
        if value is not None and value < 0:
            raise ValueError("'{0}' value must be a non-negative number but "
                             "got {1}.".format(self.name, value))


class OperationTimeout(Element):
    schema = Leaf(type=numbers.Number)

    def validate(self):
        value = self.initial_value
        if value is None:
            return
        if value < 0:
            raise ValueError("'{0}' value must be a non-negative number but "
                             "got {1}.".format(self.name, value))


class OperationTimeoutRecoverable(Element):
    schema = Leaf(type=bool)


class Operation(Element):

    def parse(self):
        if isinstance(self.initial_value, str):
            return {
                'implementation': self.initial_value,
                'executor': None,
                'inputs': {},
                'max_retries': None,
                'retry_interval': None,
                'timeout': None,
                'timeout_recoverable': None
            }
        else:
            return self.build_dict_result()


class NodeTypeOperation(Operation):

    schema = [
        Leaf(type=str),
        {
            'implementation': OperationImplementation,
            'inputs': NodeTypeOperationInputs,
            'executor': OperationExecutor,
            'max_retries': OperationMaxRetries,
            'retry_interval': OperationRetryInterval,
            'timeout': OperationTimeout,
            'timeout_recoverable': OperationTimeoutRecoverable
        }
    ]


class NodeTemplateOperation(Operation):

    schema = [
        Leaf(type=str),
        {
            'implementation': OperationImplementation,
            'inputs': NodeTemplateOperationInputs,
            'executor': OperationExecutor,
            'max_retries': OperationMaxRetries,
            'retry_interval': OperationRetryInterval,
            'timeout': OperationTimeout,
            'timeout_recoverable': OperationTimeoutRecoverable
        }
    ]


class Interface(DictElement):
    pass


class NodeTemplateInterface(Interface):

    schema = Dict(type=NodeTemplateOperation)
    add_namespace_to_schema_elements = False


class NodeTemplateInterfaces(DictElement):

    schema = Dict(type=NodeTemplateInterface)
    add_namespace_to_schema_elements = False


class NodeTypeInterface(Interface):

    schema = Dict(type=NodeTypeOperation)
    add_namespace_to_schema_elements = False


class NodeTypeInterfaces(DictElement):

    schema = Dict(type=NodeTypeInterface)
    add_namespace_to_schema_elements = False


def process_interface_operations(
        interface,
        plugins,
        error_code,
        partial_error_message,
        resource_bases,
        remote_resources_namespaces):
    return [process_operation(
        plugins=plugins,
        operation_name=operation_name,
        operation_content=operation_content,
        error_code=error_code,
        partial_error_message=partial_error_message,
        resource_bases=resource_bases,
        remote_resources_namespaces=remote_resources_namespaces)
            for operation_name, operation_content in interface.items()]


def _is_local_script_resource_exists(resource_bases, resource_path):
    def resource_exists(location_bases, resource_name):
        # Removing the namespace prefix from mapping string,
        # in case of operation with a script for checking
        # the existence of that script's file.
        resource_name = utils.remove_namespace(resource_name)

        return any(utils.url_exists('{0}/{1}'.format(base, resource_name))
                   for base in location_bases if base)

    return resource_bases and resource_exists(resource_bases, resource_path)


def _is_remote_script_resource(resource_path, remote_resources_namespaces):
    """
    If the script is prefixed with a namespace from the blueprint
    remote resources namespaces, that means the script is a remote one and
    already validated when that blueprint was loaded.
    """
    return (resource_path[:utils.find_namespace_location(resource_path)] in
            remote_resources_namespaces)


def workflow_operation(
    plugin_name,
    workflow_mapping,
    workflow_parameters,
    is_workflow_cascading,
    workflow_availability,
):
    return {
        'plugin': plugin_name,
        'operation': workflow_mapping,
        'parameters': workflow_parameters,
        'is_cascading': is_workflow_cascading,
        'availability_rules': workflow_availability,
    }


def process_operation(
        plugins,
        operation_name,
        operation_content,
        error_code,
        partial_error_message,
        resource_bases,
        remote_resources_namespaces,
        is_workflows=False,
        is_workflow_cascading=False,
        workflow_availability=None,
):
    payload_field_name = 'parameters' if is_workflows else 'inputs'
    mapping_field_name = 'mapping' if is_workflows else 'implementation'
    operation_mapping = operation_content[mapping_field_name]
    operation_payload = operation_content[payload_field_name]

    operation_payload_types = \
        {k: v['type']
         for k, v in operation_payload.items()
         if isinstance(v, dict) and 'type' in v}

    # only for node operations
    operation_executor = operation_content.get('executor', None)
    operation_max_retries = operation_content.get('max_retries', None)
    operation_retry_interval = operation_content.get('retry_interval', None)
    operation_timeout = operation_content.get('timeout', None)
    operation_timeout_recoverable = operation_content.get(
        'timeout_recoverable', None)

    if not operation_mapping:
        if is_workflows:
            raise RuntimeError('Illegal state. workflow mapping should always'
                               'be defined (enforced by schema validation)')
        else:
            return no_op_operation(operation_name=operation_name)

    candidate_plugins = [p for p in plugins
                         if operation_mapping.startswith('{0}.'.format(p))]

    is_valid_url = utils.is_valid_url(operation_mapping)
    local_resource_exists = _is_local_script_resource_exists(
        resource_bases, operation_mapping)
    if is_valid_url or local_resource_exists:
        is_script = False
    else:
        is_script = _is_inline_script(operation_mapping)

    if (
        is_valid_url or
        is_script or
        local_resource_exists or
        (not candidate_plugins and _is_remote_script_resource(
            operation_mapping, remote_resources_namespaces))
    ):
        if (
            constants.SCRIPT_PATH_PROPERTY in operation_payload or
            constants.SCRIPT_SOURCE_PROPERTY in operation_payload
        ):
            # if `implementation` is already a script source/path,
            # disallow also providing that argument in inputs
            wf_or_op = 'workflow' if is_workflows else 'operation'
            offending_property = constants.SCRIPT_PATH_PROPERTY
            if constants.SCRIPT_SOURCE_PROPERTY in operation_payload:
                offending_property = constants.SCRIPT_PATH_PROPERTY

            raise exceptions.DSLParsingLogicException(
                60,
                f"Cannot define '{offending_property}' property "
                f"in '{operation_mapping}' for {wf_or_op} '{operation_name}'"
            )

        script_path = operation_mapping
        operation_payload = copy.deepcopy(operation_payload or {})
        operation_executor = operation_executor or 'auto'

        script_property = constants.SCRIPT_PATH_PROPERTY
        if is_script:
            script_property = constants.SCRIPT_SOURCE_PROPERTY

        if is_workflows:
            operation_mapping = constants.SCRIPT_PLUGIN_EXECUTE_WORKFLOW_TASK
            operation_payload[script_property] = {
                'default': script_path,
                'description': 'Workflow script executed by the script plugin',
            }
        else:
            operation_mapping = constants.SCRIPT_PLUGIN_RUN_TASK
            operation_payload[script_property] = script_path

        # There can be more then one script plugin defined in the blueprint,
        # in case of the other one's are namespaced. But they are actually
        # pointing to the same installed one.
        script_plugins = utils.find_suffix_matches_in_list(
            constants.SCRIPT_PLUGIN_NAME,
            plugins)

        if script_plugins:
            script_plugin = script_plugins[0]
        else:
            message = "Script plugin is not defined but it is required for" \
                      " mapping '{0}' of {1} '{2}'" \
                .format(operation_mapping,
                        'workflow' if is_workflows else 'operation',
                        operation_name)
            raise exceptions.DSLParsingLogicException(61, message)

        if is_workflows:
            return workflow_operation(
                plugin_name=script_plugin,
                workflow_mapping=operation_mapping,
                workflow_parameters=operation_payload,
                is_workflow_cascading=is_workflow_cascading,
                workflow_availability=workflow_availability,
            )
        else:
            if not operation_executor:
                operation_executor = plugins[script_plugin]['executor']
            return operation(
                name=operation_name,
                plugin_name=script_plugin,
                operation_mapping=operation_mapping,
                operation_inputs=operation_payload,
                executor=operation_executor,
                max_retries=operation_max_retries,
                retry_interval=operation_retry_interval,
                timeout=operation_timeout,
                timeout_recoverable=operation_timeout_recoverable,
                operation_inputs_types=operation_payload_types,
            )

    if candidate_plugins:
        if len(candidate_plugins) > 1:
            raise exceptions.DSLParsingLogicException(
                91, 'Ambiguous operation mapping. [operation={0}, '
                    'plugins={1}]'.format(operation_name, candidate_plugins))
        plugin_name = candidate_plugins[0]
        mapping = operation_mapping[len(plugin_name) + 1:]
        if is_workflows:
            return workflow_operation(
                plugin_name=plugin_name,
                workflow_mapping=mapping,
                workflow_parameters=operation_payload,
                is_workflow_cascading=is_workflow_cascading,
                workflow_availability=workflow_availability,
            )
        else:
            if not operation_executor:
                operation_executor = plugins[plugin_name]['executor']
            return operation(
                name=operation_name,
                plugin_name=plugin_name,
                operation_mapping=mapping,
                operation_inputs=operation_payload,
                executor=operation_executor,
                max_retries=operation_max_retries,
                retry_interval=operation_retry_interval,
                timeout=operation_timeout,
                timeout_recoverable=operation_timeout_recoverable,
                operation_inputs_types=operation_payload_types,
            )

    else:
        base_error_message = (
            "Could not extract plugin or a script resource is not found from "
            "{2} mapping/script-path '{0}', which is declared for {2} '{1}'."
            .format(operation_mapping,
                    operation_name,
                    'workflow' if is_workflows else 'operation'))
        error_message = base_error_message + partial_error_message
        raise exceptions.DSLParsingLogicException(error_code, error_message)


def _is_inline_script(script):
    """Check if the string `script` contains a script.

    The string, which is coming from an operation mapping in the blueprint,
    can contain either an operation dotted import path, or a script written
    right there in the blueprint.
    """
    # a very simple heuristic: if it's multiple lines, then it's an inline
    # script. Otherwise it's a filename or a plugin path.
    return '\n' in script.strip()
