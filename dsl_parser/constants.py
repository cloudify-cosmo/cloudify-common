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

DSL_DEFINITIONS = 'dsl_definitions'
DESCRIPTION = 'description'
METADATA = 'metadata'
NODE_TEMPLATES = 'node_templates'
IMPORTS = 'imports'
NODE_TYPES = 'node_types'
PLUGINS = 'plugins'
INTERFACES = 'interfaces'
SOURCE_INTERFACES = 'source_interfaces'
TARGET_INTERFACES = 'target_interfaces'
WORKFLOWS = 'workflows'
RELATIONSHIPS = 'relationships'
PROPERTIES = 'properties'
RUNTIME_PROPERTIES = 'runtime_properties'
PARAMETERS = 'parameters'
TYPE_HIERARCHY = 'type_hierarchy'
POLICY_TRIGGERS = 'policy_triggers'
POLICY_TYPES = 'policy_types'
POLICIES = 'policies'
GROUPS = 'groups'
INPUTS = 'inputs'
OUTPUTS = 'outputs'
DERIVED_FROM = 'derived_from'
DATA_TYPES = 'data_types'
CAPABILITIES = 'capabilities'
IMPORTED_BLUEPRINTS = 'imported_blueprints'
NAMESPACES_MAPPING = 'namespaces_mapping'
CONSTRAINTS = 'constraints'
DEFAULT = 'default'
TYPE = 'type'
ITEM_TYPE = 'item_type'
LABELS = 'labels'
BLUEPRINT_LABELS = 'blueprint_labels'
DEFAULT_SCHEDULES = 'default_schedules'
DEPLOYMENT_SETTINGS = 'deployment_settings'
DISPLAY_LABEL = 'display_label'
HIDDEN = 'hidden'
RESOURCE_TAGS = 'resource_tags'

HOST_TYPE = 'cloudify.nodes.Compute'
DEPENDS_ON_REL_TYPE = 'cloudify.relationships.depends_on'
CONTAINED_IN_REL_TYPE = 'cloudify.relationships.contained_in'
CONNECTED_TO_REL_TYPE = 'cloudify.relationships.connected_to'
ROOT_ELEMENT_VALUE = 'root'

SCALING_POLICY = 'cloudify.policies.scaling'

CENTRAL_DEPLOYMENT_AGENT = 'central_deployment_agent'
HOST_AGENT = 'host_agent'
PLUGIN_EXECUTOR_KEY = 'executor'
PLUGIN_SOURCE_KEY = 'source'
PLUGIN_INSTALL_KEY = 'install'
PLUGIN_INSTALL_ARGUMENTS_KEY = 'install_arguments'
PLUGIN_NAME_KEY = 'name'
PLUGIN_PACKAGE_NAME = 'package_name'
PLUGIN_PACKAGE_VERSION = 'package_version'
PLUGIN_SUPPORTED_PLATFORM = 'supported_platform'
PLUGIN_DISTRIBUTION = 'distribution'
PLUGIN_DISTRIBUTION_VERSION = 'distribution_version'
PLUGIN_DISTRIBUTION_RELEASE = 'distribution_release'
PLUGINS_TO_INSTALL = 'plugins_to_install'
DEPLOYMENT_PLUGINS_TO_INSTALL = 'deployment_plugins_to_install'
WORKFLOW_PLUGINS_TO_INSTALL = 'workflow_plugins_to_install'
HOST_AGENT_PLUGINS_TO_INSTALL = 'host_agent_plugins_to_install'
INTER_DEPLOYMENT_FUNCTIONS = 'inter_deployment_functions'
VERSION = 'version'
CLOUDIFY = 'cloudify'

SCRIPT_PLUGIN_NAME = 'script'
SCRIPT_PLUGIN_RUN_TASK = 'script_runner.tasks.run'
SCRIPT_PLUGIN_EXECUTE_WORKFLOW_TASK = 'script_runner.tasks.execute_workflow'
SCRIPT_PATH_PROPERTY = 'script_path'
SCRIPT_SOURCE_PROPERTY = 'script_source'

FUNCTION_NAME_PATH_SEPARATOR = '__sep__'

NODES = 'nodes'
OPERATIONS = 'operations'
NODE_INSTANCES = 'node_instances'

IMPORT_RESOLVER_KEY = 'import_resolver'
VALIDATE_DEFINITIONS_VERSION = 'validate_definitions_version'
RESOLVER_IMPLEMENTATION_KEY = 'implementation'
RESLOVER_PARAMETERS_KEY = 'parameters'

OBJECT_BASED_TYPES_DSL_1_4 = [
    'blueprint_id', 'deployment_id', 'capability_value', 'scaling_group',
    'node_id', 'node_type', 'node_instance', 'secret_key', ]
OBJECT_BASED_TYPES_DSL_1_5 = ['operation_name', ]
OBJECT_BASED_TYPES = OBJECT_BASED_TYPES_DSL_1_4 + OBJECT_BASED_TYPES_DSL_1_5
BLUEPRINT_OR_DEPLOYMENT_ID_CONSTRAINT_TYPES = \
    ['scaling_group', 'operation_name', 'node_id', 'node_type']
DEPLOYMENT_ID_CONSTRAINT_TYPES = \
    ['capability_value', 'node_instance']
ID_CONSTRAINT_TYPES = BLUEPRINT_OR_DEPLOYMENT_ID_CONSTRAINT_TYPES + \
                      DEPLOYMENT_ID_CONSTRAINT_TYPES
USER_PRIMITIVE_TYPES = ['string', 'integer', 'float', 'boolean', 'list',
                        'dict', 'regex', 'textarea'] + OBJECT_BASED_TYPES

PLUGIN_DSL_KEYS_NOT_FROM_YAML = ['blueprint_labels', 'labels', 'resource_tags']
PLUGIN_DSL_KEYS_READ_FROM_DB = PLUGIN_DSL_KEYS_NOT_FROM_YAML
PLUGIN_DSL_KEYS_ADD_VALUES_NODE = ['blueprint_labels', 'labels']

UNBOUNDED_LITERAL = 'UNBOUNDED'
UNBOUNDED = -1

SCALING_GROUPS = 'scaling_groups'

NAMESPACE_DELIMITER = '--'
CLOUDIFY_TYPE_PREFIX = 'cloudify.'
BLUEPRINT_IMPORT = 'blueprint:'
PLUGIN_PREFIX = 'plugin:'

ADDED_AND_RELATED = 'added_and_related'
REMOVED_AND_RELATED = 'removed_and_related'
EXTENDED_AND_RELATED = 'extended_and_related'
REDUCED_AND_RELATED = 'reduced_and_related'

EVAL_FUNCS_PATH_PREFIX_KEY = '_path_prefix'
EVAL_FUNCS_PATH_DEFAULT_PREFIX = 'payload'
