########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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
import json
from typing import List

from dsl_parser import (scan,
                        utils,
                        models,
                        parser,
                        functions,
                        exceptions,
                        constraints,
                        multi_instance)
from dsl_parser.constants import (
    INPUTS, DEFAULT, DATA_TYPES, TYPE, ITEM_TYPE, INTER_DEPLOYMENT_FUNCTIONS,
)
from dsl_parser.multi_instance import modify_deployment


__all__ = [
    'modify_deployment'
]


def parse_dsl(dsl_location,
              resources_base_path,
              resolver=None,
              validate_version=True,
              additional_resources=()):
    return parser.parse_from_path(
        dsl_file_path=dsl_location,
        resources_base_path=resources_base_path,
        resolver=resolver,
        validate_version=validate_version,
        additional_resource_sources=additional_resources)


def _set_plan_inputs(plan, inputs, auto_correct_types, values_getter):
    inputs = inputs if inputs else {}
    # Verify inputs satisfied
    missing_inputs = []
    for input_name, input_def in plan[INPUTS].items():
        input_is_missing = skip_input_validation = False
        if input_name in inputs:
            try:
                str(json.dumps(inputs[input_name], ensure_ascii=False))
            except UnicodeEncodeError:
                raise exceptions.DSLParsingInputTypeException(
                    exceptions.ERROR_INVALID_CHARS,
                    'Illegal characters in input: {0}. '
                    'Only valid ascii chars are supported.'.format(input_name))
        elif DEFAULT in input_def and input_def[DEFAULT] is not None:
            inputs[input_name] = input_def[DEFAULT]
        else:
            # Try to get some defaults from the data_type maybe or in other
            # words just try to parse the value before validating it.
            try:
                parsed_value = utils.parse_value(
                    {},
                    type_name=input_def.get(TYPE, None),
                    data_types=plan.get(DATA_TYPES, {}),
                    undefined_property_error_message="Undefined property "
                                                     "{1} in value of "
                                                     "input {0}.",
                    missing_property_error_message="Value of input {0} "
                                                   "is missing property "
                                                   "{1}.",
                    node_name=input_name,
                    path=[],
                    raise_on_missing_property=True)
            except exceptions.DSLParsingException:
                parsed_value = None
            finally:
                if parsed_value:
                    inputs[input_name] = parsed_value
                elif input_def.get('required', True):
                    missing_inputs.append(input_name)
                    input_is_missing = True
                else:
                    skip_input_validation = True
                    inputs[input_name] = None
        if skip_input_validation:
            continue

        # Verify inputs comply with the given constraints and also the
        # data_type, if mentioned
        input_constraints = constraints.extract_constraints(input_def)
        if not input_is_missing:
            if auto_correct_types:
                inputs[input_name] = utils.cast_to_type(
                    inputs[input_name], input_def.get(TYPE, None))

            constraints.validate_input_value(
                input_name, input_constraints, inputs[input_name],
                input_def.get(TYPE), input_def.get(ITEM_TYPE), values_getter)
            inputs_complete = inputs[input_name]
            try:
                inputs_complete = utils.parse_value(
                    value=inputs[input_name],
                    type_name=input_def.get(TYPE, None),
                    data_types=plan.get(DATA_TYPES, {}),
                    undefined_property_error_message="Undefined property "
                                                     "{1} in value of "
                                                     "input {0}.",
                    missing_property_error_message="Value of input {0} "
                                                   "is missing property "
                                                   "{1}.",
                    node_name=input_name,
                    path=[],
                    raise_on_missing_property=True)
            except exceptions.DSLParsingLogicException as e:
                raise exceptions.DSLParsingException(
                    exceptions.ERROR_INPUT_VIOLATES_DATA_TYPE_SCHEMA,
                    str(e))
            inputs[input_name] = inputs_complete

    if missing_inputs:
        raise exceptions.MissingRequiredInputError(
            "Required inputs {0} were not specified - expected "
            "inputs: {1}".format(missing_inputs, list(plan[INPUTS]))
        )
    # Verify all inputs appear in plan
    not_expected = [input_name for input_name in inputs
                    if input_name not in plan[INPUTS]]
    if not_expected:
        raise exceptions.UnknownInputError(
            "Unknown inputs {0} specified - "
            "expected inputs: {1}".format(not_expected,
                                          list(plan[INPUTS])))

    plan[INPUTS] = inputs


def _process_functions(plan, runtime_only_evaluation=False):
    handler = functions.plan_evaluation_handler(plan, runtime_only_evaluation)
    scan.scan_service_template(plan, handler, replace=True)


def _find_idd_creating_functions(plan) -> List[functions.IDDSpec]:
    handler = functions.IDDFindingHandler(plan)
    scan.scan_service_template(plan, handler, replace=False)
    return handler.dependencies


def _find_rel_target_by_iddspec(relationships, idd_spec):
    """Find the target instance that matches the relationship in IDDSpec

    The node-instance can have many relationships, so we need to find one
    that matches the target node name & type.
    If there's multiple relationships of the same type to the same node,
    then we will just select the first instance, because we have no way
    to distinguish them.
    (that case shouldn't be possible anyway)
    """
    assert idd_spec.scope == 'node_template_relationship'

    target_name = idd_spec.context['target_node_name']
    rel_type = idd_spec.context['relationship_type']

    for relationship in relationships:
        if (
            target_name == relationship['target_name']
            and rel_type == relationship['type']
        ):
            return relationship['target_id']

    node_name = idd_spec.context['node']
    raise ValueError(
        'IDDs: relationship not found: {0} {1} {2}'
        .format(node_name, rel_type, target_name),
    )


def _format_idds(plan, dep_specs):
    """Format IDDSpecs into actual dependencies by choosing the node-instances.

    IDDSpecs will contain just node name, but there can be many node instances
    for a given node name, so we need to expand every such IDDSpec into
    multiple actual IDD dicts.

    This returns a list of dicts containing the keys:
        - function_identifier - the IDDCreatingFunction path
        - target_deployment - the deployment id, or a dict representing
          a function call
        - context - a dict containing possibly the keys SELF, TARGET, SOURCE -
          this is the context that can be passed into function evaluation,
          when evaluating the target_deployment (if it is a function)
    """
    idds = []
    nis_by_node = {}
    for ni in plan.get('node_instances', []):
        node_name = ni['node_id']
        nis_by_node.setdefault(node_name, []).append(ni)

    for idd_spec in dep_specs:
        idd_base = {
            'function_identifier': idd_spec.function_identifier,
            'target_deployment': idd_spec.target_deployment,
        }

        if idd_spec.scope == 'node_template':
            # the IDDSpec is a node one (e.g. in node properties):
            # create an instance of IDD, for every node-instance of the node
            node_name = idd_spec.context['node_name']
            if not nis_by_node.get(node_name):
                # node with 0 instances - nothing to do
                continue
            for ni in nis_by_node[node_name]:
                idd = idd_base.copy()
                idd['context'] = {
                    'self': ni['id'],
                }
                idds.append(idd)

        elif idd_spec.scope == 'node_template_relationship':
            # the IDDSpec is a relationship one (e.g. in relationship operation
            # inputs) - create an instance of IDD for every node-instance of
            # the source node
            node_name = idd_spec.context['source_node_name']
            if not nis_by_node.get(node_name):
                # node with 0 instances - nothing to do
                continue
            for ni in nis_by_node[node_name]:
                target_id = _find_rel_target_by_iddspec(
                    ni.get('relationships', []),
                    idd_spec,
                )
                idd = idd_base.copy()
                idd['context'] = {
                    'self': ni['id'],
                    'source': ni['id'],
                    'target': target_id,
                }
                idds.append(idd)
        else:
            # other IDDSpec - e.g. outputs, capabilities, etc. Those can be
            # evaluated without any additional context.
            idd = idd_base.copy()
            idd['context'] = {}
            idds.append(idd)
    return idds


def _find_secrets(plan):
    """Find secret names used in the plan.

    Secret names that are non-evaluated (e.g. based on get_attribute calls)
    are not returned, only secret names known at plan creation time
    """
    handler = functions.SecretFindingHandler(plan)
    scan.scan_service_template(plan, handler, replace=False)
    return handler.secrets


def _validate_secrets(secrets, get_secret_method):
    # Mainly for local workflow that doesn't support secrets
    if not secrets:
        return

    if get_secret_method is None:
        raise exceptions.UnsupportedGetSecretError(
            "The get_secret intrinsic function is not supported"
        )

    invalid_secrets = []
    for secret_key in secrets:
        try:
            get_secret_method(secret_key)
        except Exception as exception:
            if hasattr(exception, 'status_code')\
                    and exception.status_code == 404:
                invalid_secrets.append(secret_key)
            else:
                raise

    if invalid_secrets:
        raise exceptions.UnknownSecretError(
            "Required secrets: [{0}] don't exist in this tenant"
            .format(', '.join(s for s in invalid_secrets))
        )


def prepare_deployment_plan(plan, get_secret_method=None, inputs=None,
                            runtime_only_evaluation=False,
                            auto_correct_types=False,
                            values_getter=None,
                            existing_ni_ids=None,
                            **_):
    """
    Prepare a plan for deployment
    """
    plan = models.Plan(copy.deepcopy(plan))
    _set_plan_inputs(plan, inputs, auto_correct_types, values_getter)
    _process_functions(plan, runtime_only_evaluation)
    secrets = _find_secrets(plan)
    _validate_secrets(secrets, get_secret_method)
    dep_specs = _find_idd_creating_functions(plan)
    dep_plan = multi_instance.create_deployment_plan(plan, existing_ni_ids)
    dep_plan[INTER_DEPLOYMENT_FUNCTIONS] = _format_idds(dep_plan, dep_specs)
    return dep_plan
