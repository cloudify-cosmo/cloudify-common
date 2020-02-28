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

from dsl_parser import (exceptions,
                        utils,
                        constants)
from dsl_parser.interfaces import interfaces_parser
from dsl_parser.elements import (node_types as _node_types,
                                 plugins as _plugins,
                                 relationships as _relationships,
                                 operation as _operation,
                                 data_types as _data_types,
                                 scalable,
                                 version as _version,
                                 misc)
from dsl_parser.framework.requirements import Value, Requirement
from dsl_parser.framework.elements import (DictElement,
                                           Element,
                                           Leaf,
                                           Dict,
                                           List)


class NodeTemplateType(Element):

    required = True
    schema = Leaf(type=str)
    requires = {
        _node_types.NodeTypes: [Value('node_types')]
    }

    def validate(self, node_types):
        if self.initial_value not in node_types:
            err_message = ("Could not locate node type: '{0}'; "
                           "existing types: {1}"
                           .format(self.initial_value,
                                   list(node_types)))
            raise exceptions.DSLParsingLogicException(7, err_message)


class NodeTemplateProperties(Element):

    schema = Leaf(type=dict)
    requires = {
        NodeTemplateType: [],
        _node_types.NodeTypes: [Value('node_types')],
        _data_types.DataTypes: [Value('data_types')]
    }

    def parse(self, node_types, data_types):
        properties = self.initial_value or {}
        node_type_name = self.sibling(NodeTemplateType).value
        node_type = node_types[node_type_name]
        return utils.merge_schema_and_instance_properties(
            instance_properties=properties,
            schema_properties=node_type['properties'],
            data_types=data_types,
            undefined_property_error_message=(
                "'{0}' node '{1}' property is not part of the derived"
                " type properties schema"),
            missing_property_error_message=(
                "'{0}' node does not provide a "
                "value for mandatory "
                "'{1}' property which is "
                "part of its type schema"),
            node_name=self.ancestor(NodeTemplate).name)


class NodeTemplateRelationshipType(Element):

    required = True
    schema = Leaf(type=str)
    requires = {
        _relationships.Relationships: [Value('relationships')]
    }

    def validate(self, relationships):
        if self.initial_value not in relationships:
            raise exceptions.DSLParsingLogicException(
                26, "A relationship instance under node '{0}' declares an "
                    "undefined relationship type '{1}'"
                    .format(self.ancestor(NodeTemplate).name,
                            self.initial_value))


class NodeTemplateRelationshipTarget(Element):

    required = True
    schema = Leaf(type=str)

    def validate(self):
        relationship_type = self.sibling(NodeTemplateRelationshipType).name
        node_name = self.ancestor(NodeTemplate).name
        node_template_names = self.ancestor(NodeTemplates).initial_value_holder
        if self.initial_value not in node_template_names:
            raise exceptions.DSLParsingLogicException(
                25, "A relationship instance under node '{0}' of type '{1}' "
                    "declares an undefined target node '{2}'"
                    .format(node_name,
                            relationship_type,
                            self.initial_value))
        if self.initial_value == node_name:
            raise exceptions.DSLParsingLogicException(
                23, "A relationship instance under node '{0}' of type '{1}' "
                    "illegally declares the source node as the target node"
                    .format(node_name, relationship_type))


class NodeTemplateRelationshipProperties(Element):

    schema = Leaf(type=dict)
    requires = {
        NodeTemplateRelationshipType: [],
        NodeTemplateRelationshipTarget: [],
        _relationships.Relationships: [Value('relationships')],
        _data_types.DataTypes: [Value('data_types')]
    }

    def parse(self, relationships, data_types):
        relationship_type_name = self.sibling(
            NodeTemplateRelationshipType).value
        properties = self.initial_value or {}
        return utils.merge_schema_and_instance_properties(
            instance_properties=properties,
            schema_properties=relationships[relationship_type_name][
                'properties'],
            data_types=data_types,
            undefined_property_error_message=(
                "'{0}' node relationship '{1}' property is not part of "
                "the derived relationship type properties schema"),
            missing_property_error_message=(
                "'{0}' node relationship does not provide a "
                "value for mandatory "
                "'{1}' property which is "
                'part of its relationship type schema'),
            node_name=self.ancestor(NodeTemplate).name)

    def validate(self, relationships, **kwargs):
        relationship_type = self.sibling(NodeTemplateRelationshipType).value
        node_name = self.ancestor(NodeTemplate).name

        if (relationship_type ==
                'cloudify.relationships.depends_on_lifecycle_operation'):
            target_node = self.sibling(
                NodeTemplateRelationshipTarget).value
            properties = self.initial_value or {}
            node_templates = self.ancestor(NodeTemplates).initial_value_holder

            operation_input = properties.get('operation', None)
            if not operation_input:
                raise exceptions.DSLParsingLogicException(
                    215,
                    'For "{1}" node, please supply "{0}" with a defined '
                    'lifecycle operation target '.format(relationship_type,
                                                         node_name))

            _, target_node_interfaces = node_templates[target_node].get_item(
                'interfaces')
            if (not target_node_interfaces or
                    operation_input not in
                    target_node_interfaces['cloudify.interfaces.lifecycle']):
                raise exceptions.DSLParsingLogicException(
                    216,
                    'Please define "{0}" operation in the target node "{1}" '
                    'for "{3}" node\'s "{2}" relationship'.format(
                        operation_input,
                        target_node,
                        relationship_type,
                        node_name))


class NodeTemplateInstancesDeploy(Element):

    required = True
    schema = Leaf(type=int)

    def validate(self):
        if self.initial_value < 0:
            raise exceptions.DSLParsingFormatException(
                1, 'deploy instances must be a non-negative number')


class NodeTemplateInstances(DictElement):

    schema = {
        'deploy': NodeTemplateInstancesDeploy
    }


# TODO: Capabilities should be implemented according to TOSCA as generic types
class NodeTemplateCapabilitiesScalable(DictElement):

    schema = {
        'properties': scalable.Properties
    }

    def parse(self):
        if self.initial_value is None:
            return {'properties': scalable.Properties.DEFAULT.copy()}
        else:
            return {
                'properties': self.child(scalable.Properties).value
            }


def _instances_predicate(source, target):
    return source.ancestor(NodeTemplate) is target.ancestor(NodeTemplate)


class NodeTemplateCapabilities(DictElement):

    schema = {
        'scalable': NodeTemplateCapabilitiesScalable
    }
    requires = {
        _version.ToscaDefinitionsVersion: ['version'],
        'inputs': ['validate_version'],
        NodeTemplateInstancesDeploy: [Value('instances_deploy',
                                            required=False,
                                            predicate=_instances_predicate)]
    }

    def validate(self, version, validate_version, instances_deploy):
        if validate_version:
            self.validate_version(version, (1, 3))
        if instances_deploy is not None and self.initial_value is not None:
            raise exceptions.DSLParsingLogicException(
                exceptions.ERROR_INSTANCES_DEPLOY_AND_CAPABILITIES,
                "Node '{0}' defines both instances.deploy and "
                "capabilities.scalable (Note: instances.deploy is deprecated)"
                .format(self.ancestor(NodeTemplate).name))

    def parse(self, instances_deploy, **kwargs):
        if self.initial_value is None:
            properties = scalable.Properties.DEFAULT.copy()
            if instances_deploy is not None:
                for key in properties:
                    if key not in ['min_instances', 'max_instances']:
                        properties[key] = instances_deploy
            return {
                'scalable': {
                    'properties': properties
                }
            }
        else:
            return {
                'scalable': self.child(NodeTemplateCapabilitiesScalable).value
            }


def _node_template_relationship_type_predicate(source, target):
    try:
        return (source.child(NodeTemplateRelationshipType).initial_value ==
                target.name)
    except exceptions.DSLParsingElementMatchException:
        return False


class NodeTemplateRelationship(Element):

    schema = {
        'type': NodeTemplateRelationshipType,
        'target': NodeTemplateRelationshipTarget,
        'properties': NodeTemplateRelationshipProperties,
        'source_interfaces': _operation.NodeTemplateInterfaces,
        'target_interfaces': _operation.NodeTemplateInterfaces,
    }
    requires = {
        _relationships.Relationship: [
            Value('relationship_type',
                  predicate=_node_template_relationship_type_predicate)]
    }

    def parse(self, relationship_type):
        result = self.build_dict_result()
        for interfaces in [constants.SOURCE_INTERFACES,
                           constants.TARGET_INTERFACES]:
            result[interfaces] = interfaces_parser. \
                merge_relationship_type_and_instance_interfaces(
                    relationship_type_interfaces=relationship_type[interfaces],
                    relationship_instance_interfaces=result[interfaces])

        result[constants.TYPE_HIERARCHY] = relationship_type[
            constants.TYPE_HIERARCHY]

        result['target_id'] = result['target']
        del result['target']

        return result


class NodeTemplateRelationships(Element):

    schema = List(type=NodeTemplateRelationship)
    provides = ['contained_in']

    def validate(self):
        contained_in_relationships = []
        contained_in_targets = []
        for relationship in self.children():
            relationship_target = relationship.child(
                NodeTemplateRelationshipTarget).value
            relationship_type = relationship.child(
                NodeTemplateRelationshipType).value
            type_hierarchy = relationship.value[constants.TYPE_HIERARCHY]
            if constants.CONTAINED_IN_REL_TYPE in type_hierarchy:
                contained_in_relationships.append(relationship_type)
                contained_in_targets.append(relationship_target)

        if len(contained_in_relationships) > 1:
            ex = exceptions.DSLParsingLogicException(
                112, "Node '{0}' has more than one relationship that is "
                     "derived from '{1}' relationship. Found: {2} for targets:"
                     " {3}"
                     .format(self.ancestor(NodeTemplate).name,
                             constants.CONTAINED_IN_REL_TYPE,
                             contained_in_relationships,
                             contained_in_targets))
            ex.relationship_types = contained_in_relationships
            raise ex

    def parse(self):
        return [c.value for c in sorted(self.children(),
                                        key=lambda child: child.index)]

    def calculate_provided(self):
        contained_in_list = [r.child(NodeTemplateRelationshipTarget).value
                             for r in self.children()
                             if constants.CONTAINED_IN_REL_TYPE in
                             r.value[constants.TYPE_HIERARCHY]]
        contained_in = contained_in_list[0] if contained_in_list else None
        return {
            'contained_in': contained_in
        }


def _node_template_related_nodes_predicate(source, target):
    if source.name == target.name:
        return False
    targets = source.descendants(NodeTemplateRelationshipTarget)
    relationship_targets = [e.initial_value for e in targets]
    return target.name in relationship_targets


def _node_template_node_type_predicate(source, target):
    try:
        return (source.child(NodeTemplateType).initial_value ==
                target.name)
    except exceptions.DSLParsingElementMatchException:
        return False


class NodeTemplate(Element):

    schema = {
        'type': NodeTemplateType,
        'instances': NodeTemplateInstances,
        'capabilities': NodeTemplateCapabilities,
        'interfaces': _operation.NodeTemplateInterfaces,
        'relationships': NodeTemplateRelationships,
        'properties': NodeTemplateProperties,
    }
    requires = {
        'inputs': [Requirement('resource_base', required=False)],
        'self': [Value('related_node_templates',
                       predicate=_node_template_related_nodes_predicate,
                       multiple_results=True)],
        _plugins.Plugins: [Value('plugins')],
        _node_types.NodeType: [
            Value('node_type',
                  predicate=_node_template_node_type_predicate)],
        _node_types.NodeTypes: ['host_types'],
        misc.NamespacesMapping: [Value(constants.NAMESPACES_MAPPING)]
    }

    def parse(self,
              node_type,
              host_types,
              plugins,
              resource_base,
              related_node_templates,
              namespaces_mapping):
        node = self.build_dict_result()
        node.update({
            'name': self.name,
            'id': self.name,
            constants.TYPE_HIERARCHY: node_type[constants.TYPE_HIERARCHY]
        })

        node[constants.INTERFACES] = interfaces_parser.\
            merge_node_type_and_node_template_interfaces(
                node_type_interfaces=node_type[constants.INTERFACES],
                node_template_interfaces=node[constants.INTERFACES])

        node['operations'] = _process_operations(
            partial_error_message="in node '{0}' of type '{1}'"
                                  .format(node['id'], node['type']),
            interfaces=node[constants.INTERFACES],
            plugins=plugins,
            error_code=10,
            resource_base=resource_base,
            remote_resources_namespaces=namespaces_mapping)

        node_name_to_node = dict((node['id'], node)
                                 for node in related_node_templates)
        _post_process_node_relationships(
            processed_node=node,
            node_name_to_node=node_name_to_node,
            plugins=plugins,
            resource_base=resource_base,
            remote_resources_namespaces=namespaces_mapping)

        contained_in = self.child(NodeTemplateRelationships).provided[
            'contained_in']
        if self.child(NodeTemplateType).value in host_types:
            node['host_id'] = self.name
        elif contained_in:
            containing_node = [n for n in related_node_templates
                               if n['name'] == contained_in][0]
            if 'host_id' in containing_node:
                node['host_id'] = containing_node['host_id']

        return node


def _post_process_node_relationships(processed_node,
                                     node_name_to_node,
                                     plugins,
                                     resource_base,
                                     remote_resources_namespaces):
    for relationship in processed_node[constants.RELATIONSHIPS]:
        target_node = node_name_to_node[relationship['target_id']]
        _process_node_relationships_operations(
            relationship=relationship,
            interfaces_attribute='source_interfaces',
            operations_attribute='source_operations',
            node_for_plugins=processed_node,
            plugins=plugins,
            resource_base=resource_base,
            remote_resources_namespaces=remote_resources_namespaces)
        _process_node_relationships_operations(
            relationship=relationship,
            interfaces_attribute='target_interfaces',
            operations_attribute='target_operations',
            node_for_plugins=target_node,
            plugins=plugins,
            resource_base=resource_base,
            remote_resources_namespaces=remote_resources_namespaces)


def _process_operations(partial_error_message,
                        interfaces,
                        plugins,
                        error_code,
                        resource_base,
                        remote_resources_namespaces):
    operations = {}
    for interface_name, interface in interfaces.items():
        interface_operations = \
            _operation.process_interface_operations(
                interface=interface,
                plugins=plugins,
                error_code=error_code,
                partial_error_message=(
                    "In interface '{0}' {1}".format(interface_name,
                                                    partial_error_message)),
                resource_bases=resource_base,
                remote_resources_namespaces=remote_resources_namespaces)
        for operation in interface_operations:
            operation_name = operation.pop('name')
            if operation_name in operations:
                # Indicate this implicit operation name needs to be
                # removed as we can only
                # support explicit implementation in this case
                operations[operation_name] = None
            else:
                operations[operation_name] = operation
            operations['{0}.{1}'.format(interface_name,
                                        operation_name)] = operation

    return dict((operation_name, operation) for operation_name, operation in
                operations.items() if operation is not None)


def _process_node_relationships_operations(relationship,
                                           interfaces_attribute,
                                           operations_attribute,
                                           node_for_plugins,
                                           plugins,
                                           resource_base,
                                           remote_resources_namespaces):
    partial_error_message = "in relationship of type '{0}' in node '{1}'" \
        .format(relationship['type'],
                node_for_plugins['id'])

    operations = _process_operations(
        partial_error_message=partial_error_message,
        interfaces=relationship[interfaces_attribute],
        plugins=plugins,
        error_code=19,
        resource_base=resource_base,
        remote_resources_namespaces=remote_resources_namespaces)

    relationship[operations_attribute] = operations


class NodeTemplates(Element):

    schema = Dict(type=NodeTemplate)
    requires = {
        _plugins.Plugins: [Value('plugins')],
        _node_types.NodeTypes: ['host_types']
    }
    provides = [
        'node_template_names',
        'plugins_to_install'
    ]

    def parse(self, host_types, plugins):
        processed_nodes = dict((node.name, node.value)
                               for node in self.children())
        _process_nodes_plugins(
            processed_nodes=processed_nodes,
            host_types=host_types,
            plugins=plugins)
        return list(processed_nodes.values())

    def calculate_provided(self, **kwargs):
        return {
            'node_template_names': set(c.name for c in self.children()),
            'plugins_to_install':
                {
                     constants.DEPLOYMENT_PLUGINS_TO_INSTALL:
                     self._fetch_node_plugins(
                         constants.DEPLOYMENT_PLUGINS_TO_INSTALL),
                     constants.HOST_AGENT_PLUGINS_TO_INSTALL:
                     self._fetch_node_plugins(
                         constants.PLUGINS_TO_INSTALL)
                 }
        }

    def _fetch_node_plugins(self, plugin_kind):
        used_plugins = {}
        for node in self.value:
            plugins = node.get(plugin_kind, [])
            for plugin in plugins:
                plugin_name = plugin[constants.PLUGIN_NAME_KEY]
                used_plugins[plugin_name] = plugin
        return list(used_plugins.values())


def _process_nodes_plugins(processed_nodes,
                           host_types,
                           plugins):
    # extract node plugins based on node operations
    # do we really need node.plugins?
    nodes_operations = dict((name, []) for name in processed_nodes)
    for node_name, node in processed_nodes.items():
        node_operations = nodes_operations[node_name]
        node_operations.append(node['operations'])
        for rel in node['relationships']:
            node_operations.append(rel['source_operations'])
            nodes_operations[rel['target_id']].append(rel['target_operations'])
    for node_name, node in processed_nodes.items():
        node[constants.PLUGINS] = _get_plugins_from_operations(
            operations_lists=nodes_operations[node_name],
            processed_plugins=plugins)

    for node in processed_nodes.values():
        # set plugins_to_install property for nodes
        if node['type'] in host_types:
            plugins_to_install = {}
            for another_node in processed_nodes.values():
                # going over all other nodes, to accumulate plugins
                # from different nodes whose host is the current node
                if another_node.get('host_id') == node['id']:
                    # ok to override here since we assume it is the same plugin
                    for plugin in another_node[constants.PLUGINS]:
                        if plugin[constants.PLUGIN_EXECUTOR_KEY] \
                                == constants.HOST_AGENT:
                            plugins_to_install[plugin['name']] = plugin
            node[constants.PLUGINS_TO_INSTALL] = \
                list(plugins_to_install.values())

        # set deployment_plugins_to_install property for nodes
        deployment_plugins_to_install = {}
        for plugin in node[constants.PLUGINS]:
            if plugin[constants.PLUGIN_EXECUTOR_KEY] \
                    == constants.CENTRAL_DEPLOYMENT_AGENT:
                deployment_plugins_to_install[plugin['name']] = plugin
        node[constants.DEPLOYMENT_PLUGINS_TO_INSTALL] = \
            list(deployment_plugins_to_install.values())

    _validate_agent_plugins_on_host_nodes(processed_nodes)


def _get_plugins_from_operations(operations_lists,
                                 processed_plugins):
    plugins = {}
    for operations in operations_lists:
        for operation in list(operations.values()):
            plugin_name = operation['plugin']
            if not plugin_name:
                # no-op
                continue
            plugin = processed_plugins[plugin_name]
            operation_executor = operation['executor']
            plugin_key = (plugin_name, operation_executor)
            if plugin_key not in plugins:
                plugin = copy.deepcopy(plugin)
                plugin['executor'] = operation_executor
                plugins[plugin_key] = plugin
    return list(plugins.values())


def _validate_agent_plugins_on_host_nodes(processed_nodes):
    for node in processed_nodes.values():
        if 'host_id' not in node:
            for plugin in node[constants.PLUGINS]:
                if plugin[constants.PLUGIN_EXECUTOR_KEY] \
                        == constants.HOST_AGENT:
                    raise exceptions.DSLParsingLogicException(
                        24, "node '{0}' has no relationship which makes it "
                            "contained within a host and it has a "
                            "plugin '{1}' with '{2}' as an executor. "
                            "These types of plugins must be "
                            "installed on a host".format(node['id'],
                                                         plugin['name'],
                                                         constants.HOST_AGENT))
