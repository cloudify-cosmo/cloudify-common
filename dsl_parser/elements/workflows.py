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

from dsl_parser import constants
from dsl_parser._compat import text_type
from dsl_parser.elements import (data_types,
                                 plugins as _plugins,
                                 operation,
                                 misc)
from dsl_parser.framework.requirements import Value, Requirement
from dsl_parser.framework.elements import (DictElement,
                                           Element,
                                           Leaf,
                                           Dict)


class WorkflowMapping(Element):

    required = True
    schema = Leaf(type=text_type)


class WorkflowParameters(data_types.Schema):

    add_namespace_to_schema_elements = False


class WorkflowIsCascading(Element):

    schema = Leaf(type=bool)
    add_namespace_to_schema_elements = False


class Workflow(Element):

    required = True
    schema = [
        Leaf(type=text_type),
        {
            'mapping': WorkflowMapping,
            'parameters': WorkflowParameters,
            'is_cascading': WorkflowIsCascading
        }
    ]
    requires = {
        'inputs': [Requirement('resource_base', required=False)],
        _plugins.Plugins: [Value('plugins')],
        misc.NamespacesMapping: [Value(constants.NAMESPACES_MAPPING)]
    }

    def parse(self, plugins, resource_base, namespaces_mapping):
        if isinstance(self.initial_value, text_type):
            operation_content = {'mapping': self.initial_value,
                                 'parameters': {}}
            is_cascading = False
        else:
            operation_content = self.build_dict_result()
            is_cascading = self.initial_value.get('is_cascading', False)
        return operation.process_operation(
            plugins=plugins,
            operation_name=self.name,
            operation_content=operation_content,
            error_code=21,
            partial_error_message='',
            resource_bases=resource_base,
            remote_resources_namespaces=namespaces_mapping,
            is_workflows=True,
            is_workflow_cascading=is_cascading)


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
