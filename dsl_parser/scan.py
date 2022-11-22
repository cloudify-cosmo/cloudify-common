#########
# Copyright (c) 2017-2019 Cloudify Platform Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

from dsl_parser.constants import (NODES,
                                  LABELS,
                                  INPUTS,
                                  OUTPUTS,
                                  POLICIES,
                                  OPERATIONS,
                                  PROPERTIES,
                                  CAPABILITIES,
                                  SCALING_GROUPS,
                                  DEPLOYMENT_SETTINGS)

NODE_TEMPLATE_SCOPE = 'node_template'
NODE_TEMPLATE_RELATIONSHIP_SCOPE = 'node_template_relationship'
OUTPUTS_SCOPE = 'outputs'
POLICIES_SCOPE = 'policies'
SCALING_GROUPS_SCOPE = 'scaling_groups'
CAPABILITIES_SCOPE = 'capabilities'
LABELS_SCOPE = 'labels'
DEPLOYMENT_SETTINGS_SCOPE = 'deployment_settings'


def scan_properties(value,
                    handler,
                    scope=None,
                    context=None,
                    path='',
                    replace=False):
    """
    Scans properties dict recursively and applies the provided handler
    method for each property.

    The handler method should have the following signature:
    def handler(value, scope, context, path):

    * value - the value of the property.
    * scope - scope of the operation (string).
    * context - scanner context (i.e. actual node template).
    * path - current property path.
    * replace - replace current dict/list values of scanned properties.

    :param value: The properties container (dict/list).
    :param handler: A method for applying for to each property.
    :param scope: (passed to the handler) - scope of the operation (string).
    :param context: (passed to the handler) - scanner context. It gets passed
     to the intrinsic functions' constructor.
    :param path: The properties base path.
    :param replace: whether to do an in-place replacement or not.
    """
    if isinstance(value, dict):
        for k, v in value.items():
            current_path = '{0}.{1}'.format(path, k)
            result = handler(v, scope, context, current_path)
            if replace and result != v:
                value[k] = result
    elif isinstance(value, list):
        for index, item in enumerate(value):
            current_path = '{0}[{1}]'.format(path, index)
            result = handler(item, scope, context, current_path)
            if replace and result != item:
                value[index] = result


def _scan_operations(operations,
                     handler,
                     scope=None,
                     context=None,
                     path='',
                     replace=False):
    for name, definition in operations.items():
        if isinstance(definition, dict) and INPUTS in definition:
            context = context.copy() if context else {}
            context['operation'] = definition
            scan_properties(definition[INPUTS],
                            handler,
                            scope=scope,
                            context=context,
                            path='{0}.{1}.{2}'.format(path, name, INPUTS),
                            replace=replace)


def scan_node_operation_properties(node_template, handler, replace=False):
    _scan_operations(node_template[OPERATIONS],
                     handler,
                     scope=NODE_TEMPLATE_SCOPE,
                     context=node_template,
                     path='{0}.{1}.{2}'.format(NODES,
                                               node_template['name'],
                                               OPERATIONS),
                     replace=replace)
    for r in node_template.get('relationships', []):
        context = {'node_template': node_template, 'relationship': r}
        _scan_operations(r.get('source_operations', {}),
                         handler,
                         scope=NODE_TEMPLATE_RELATIONSHIP_SCOPE,
                         context=context,
                         path='{0}.{1}.{2}'.format(NODES,
                                                   node_template['name'],
                                                   r['type']),
                         replace=replace)
        _scan_operations(r.get('target_operations', {}),
                         handler,
                         scope=NODE_TEMPLATE_RELATIONSHIP_SCOPE,
                         context=context,
                         path='{0}.{1}.{2}'.format(NODES,
                                                   node_template['name'],
                                                   r['type']),
                         replace=replace)


def scan_service_template(plan, handler, replace=False):
    for node_template in plan.node_templates:
        scan_node_template(node_template, handler, replace=replace)
        scan_node_operation_properties(node_template, handler, replace=replace)
    for output_name, output in plan.outputs.items():
        scan_properties(output,
                        handler,
                        scope=OUTPUTS_SCOPE,
                        context=plan.outputs,
                        path='{0}.{1}'.format(OUTPUTS, output_name),
                        replace=replace)
    for policy_name, policy in plan.get(POLICIES, {}).items():
        scan_properties(policy.get(PROPERTIES, {}),
                        handler,
                        scope=POLICIES_SCOPE,
                        context=policy,
                        path='{0}.{1}.{2}'.format(
                            POLICIES,
                            policy_name,
                            PROPERTIES),
                        replace=replace)
    for group_name, scaling_group in plan.get(SCALING_GROUPS, {}).items():
        scan_properties(scaling_group.get(PROPERTIES, {}),
                        handler,
                        scope=SCALING_GROUPS_SCOPE,
                        context=scaling_group,
                        path='{0}.{1}.{2}'.format(
                            SCALING_GROUPS,
                            group_name,
                            PROPERTIES),
                        replace=replace)
    for capability_name, capability in plan.get('capabilities', {}).items():
        scan_properties(capability,
                        handler,
                        scope=CAPABILITIES_SCOPE,
                        context=capability,
                        path='{0}.{1}'.format(
                            CAPABILITIES,
                            capability_name),
                        replace=replace)
    for label_key, label in plan.get('labels', {}).items():
        scan_properties(label,
                        handler,
                        scope=LABELS_SCOPE,
                        context=label,
                        path='{0}.{1}'.format(LABELS, label_key),
                        replace=replace)

    scan_properties(plan.get('deployment_settings'),
                    handler,
                    scope=DEPLOYMENT_SETTINGS_SCOPE,
                    context=plan,
                    path=DEPLOYMENT_SETTINGS,
                    replace=replace)


def scan_node_template(node_template, handler, replace=False):
    scan_properties(node_template[PROPERTIES],
                    handler,
                    scope=NODE_TEMPLATE_SCOPE,
                    context=node_template,
                    path='{0}.{1}.{2}'.format(
                        NODES,
                        node_template['name'],
                        PROPERTIES),
                    replace=replace)
    for name, capability in node_template.get(CAPABILITIES, {}).items():
        scan_properties(capability.get(PROPERTIES, {}),
                        handler,
                        scope=NODE_TEMPLATE_SCOPE,
                        context=node_template,
                        path='{0}.{1}.{2}.{3}'.format(
                            NODES,
                            node_template['name'],
                            CAPABILITIES,
                            name),
                        replace=replace)
