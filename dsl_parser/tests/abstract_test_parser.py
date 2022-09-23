# Copyright (c) 2017-2019 Cloudify Platform Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import tempfile
import shutil
import os
from urllib.request import pathname2url
from urllib.parse import urljoin

import unittest
from mock import Mock

from cloudify.utils import uuid4
from dsl_parser.exceptions import DSLParsingException
from dsl_parser.parser import parse as dsl_parse
from dsl_parser.parser import parse_from_path as dsl_parse_from_path
from dsl_parser.import_resolver.default_import_resolver import \
    DefaultImportResolver
from dsl_parser.version import DSL_VERSION_PREFIX
from dsl_parser.multi_instance import (create_deployment_plan,
                                       modify_deployment)


class _MockRuntimeEvaluationStorage(object):
    def __init__(self, node_instances=None, nodes=None, inputs=None,
                 secrets=None, capabilities=None, labels=None,
                 group_capabilities=None, id=None, name=None, consumers=None):
        self._node_instances = node_instances or []
        self._nodes = nodes or []
        self._inputs = inputs or {}
        self._secrets = secrets or {}
        self._capabilities = capabilities or {}
        self._labels = labels or {}
        self._group_capabilities = group_capabilities or {}
        self._consumers = consumers or {}
        self._deployment_id = id or None
        self._deployment_name = name or None

    def get_input(self, input_name):
        return self._inputs[input_name]

    def get_node_instances(self, node_id=None):
        if not node_id:
            return self._node_instances()
        return [ni for ni in self._node_instances
                if ni.get('node_id') == node_id]

    def get_node_instance(self, node_instance_id):
        for node_instance in self._node_instances:
            if node_instance['id'] == node_instance_id:
                return node_instance
        else:
            raise KeyError('Instance not found: {0}'.format(node_instance_id))

    def get_node(self, node_name):
        for node in self._nodes:
            if node['id'] == node_name:
                return node
        else:
            raise KeyError('Node not found: {0}'.format(node_name))

    def get_secret(self, secret_id):
        if self._secrets:
            return Mock(value=self._secrets[secret_id])
        return Mock(value=secret_id + '_value')

    def get_capability(self, capability_path):
        dep_id, cap_id = capability_path[0], capability_path[1]
        return self._capabilities[dep_id][cap_id]

    def get_label(self, label_key, values_list_index):
        if values_list_index is not None:
            return self._labels[label_key][values_list_index]
        return self._labels[label_key]

    def get_group_capability(self, capability_path):
        group_id, cap_id = capability_path[0], capability_path[1]
        return self._group_capabilities[group_id][cap_id]

    def get_environment_capability(self, capability_path):
        label_value = self.get_label('csys-obj-parent', 0)
        capability_path = [label_value] + capability_path
        return self.get_capability(capability_path)

    def get_sys(self, entity, prop):
        if (entity, prop) == ('deployment', 'id'):
            return self._deployment_id
        if (entity, prop) == ('deployment', 'name'):
            return self._deployment_name

    def get_consumers(self, prop):
        consumers_list = list(self._consumers.keys())
        if prop == 'ids':
            return consumers_list
        if prop == 'count':
            return len(consumers_list)
        if prop == 'names':
            return list(self._consumers.values())


class AbstractTestParser(unittest.TestCase):
    BASIC_VERSION_SECTION_DSL_1_0 = """
tosca_definitions_version: cloudify_dsl_1_0
    """

    BASIC_VERSION_SECTION_DSL_1_1 = """
tosca_definitions_version: cloudify_dsl_1_1
    """

    BASIC_VERSION_SECTION_DSL_1_2 = """
tosca_definitions_version: cloudify_dsl_1_2
    """

    BASIC_VERSION_SECTION_DSL_1_3 = """
tosca_definitions_version: cloudify_dsl_1_3
    """

    BASIC_VERSION_SECTION_DSL_1_4 = """
tosca_definitions_version: cloudify_dsl_1_4
    """

    BASIC_VERSION_SECTION_DSL_1_5 = """
tosca_definitions_version: cloudify_dsl_1_5
    """

    BASIC_NODE_TEMPLATES_SECTION = """
node_templates:
    test_node:
        type: test_type
        properties:
            key: "val"
        """

    BASIC_PLUGIN = """
plugins:
    test_plugin:
        executor: central_deployment_agent
        source: dummy
"""

    PLUGIN_WITH_INSTALL_ARGS = """
plugins:
    test_plugin:
        executor: central_deployment_agent
        source: dummy
        install_arguments: -r requirements.txt
"""

    BASIC_TYPE = """
node_types:
    test_type:
        interfaces:
            test_interface1:
                install:
                    implementation: test_plugin.install
                    inputs: {}
                terminate:
                    implementation: test_plugin.terminate
                    inputs: {}
        properties:
            install_agent:
                default: 'false'
            key: {}
            """

    BASIC_INPUTS = """
inputs:
    test_input:
        type: string
        default: test_input_default_value
"""

    BASIC_OUTPUTS = """
outputs:
    test_output:
        value: test_output_value
"""

    # note that some tests extend the BASIC_NODE_TEMPLATES 'inline',
    # which is why it's appended in the end
    MINIMAL_BLUEPRINT = """
node_types:
    test_type:
        properties:
            key:
                default: 'default'
    """ + BASIC_NODE_TEMPLATES_SECTION

    BLUEPRINT_WITH_INTERFACES_AND_PLUGINS = BASIC_NODE_TEMPLATES_SECTION + \
        BASIC_PLUGIN + BASIC_TYPE

    PLUGIN_WITH_INTERFACES_AND_PLUGINS_WITH_INSTALL_ARGS = \
        BASIC_NODE_TEMPLATES_SECTION + PLUGIN_WITH_INSTALL_ARGS + BASIC_TYPE

    _mock_storage = None

    def setUp(self):
        super(AbstractTestParser, self).setUp()
        self._temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self._temp_dir)
        super(AbstractTestParser, self).tearDown()

    def make_file_with_name(self, content, filename, base_dir=None):
        base_dir = os.path.join(self._temp_dir, base_dir) \
            if base_dir else self._temp_dir
        filename_path = os.path.join(base_dir, filename)
        if not os.path.exists(base_dir):
            os.makedirs(base_dir)
        with open(filename_path, 'w') as f:
            f.write(content)
        return filename_path

    def make_yaml_file(self, content, as_uri=False):
        filename = 'tempfile{0}.yaml'.format(uuid4())
        filename_path = self.make_file_with_name(content, filename)
        return filename_path if not as_uri else self._path2url(filename_path)

    def _path2url(self, path):
        return urljoin('file:', pathname2url(path))

    def create_yaml_with_imports(self, contents, as_uri=False):
        yaml = """
imports:"""
        for content in contents:
            filename = self.make_yaml_file(content)
            yaml += """
    -   {0}""".format(filename if not as_uri else self._path2url(filename))
        return yaml

    def _local_resolver_rules(self):
        types_path = os.path.join(
            os.path.dirname(__file__), 'minimal_types.yaml')
        return [
            {'http://local-test-resolver/types.yaml': 'file://' + types_path}
        ]

    def parse(self, dsl_string,
              resources_base_path=None,
              dsl_version=BASIC_VERSION_SECTION_DSL_1_0,
              resolver=None,
              validate_version=True):
        # add dsl version if missing
        if DSL_VERSION_PREFIX not in dsl_string:
            dsl_string = dsl_version + dsl_string
        if not resolver:
            resolver = DefaultImportResolver(
                rules=self._local_resolver_rules())
        return dsl_parse(dsl_string,
                         resources_base_path=resources_base_path,
                         resolver=resolver,
                         validate_version=validate_version)

    def parse_1_0(self, dsl_string, resources_base_path=None, resolver=None):
        return self.parse(dsl_string, resources_base_path,
                          dsl_version=self.BASIC_VERSION_SECTION_DSL_1_0,
                          resolver=resolver)

    def parse_1_1(self, dsl_string, resources_base_path=None, resolver=None):
        return self.parse(dsl_string, resources_base_path,
                          dsl_version=self.BASIC_VERSION_SECTION_DSL_1_1,
                          resolver=resolver)

    def parse_1_2(self, dsl_string, resources_base_path=None, resolver=None):
        return self.parse(dsl_string, resources_base_path,
                          dsl_version=self.BASIC_VERSION_SECTION_DSL_1_2,
                          resolver=resolver)

    def parse_1_3(self, dsl_string, resources_base_path=None, resolver=None):
        return self.parse(dsl_string, resources_base_path,
                          dsl_version=self.BASIC_VERSION_SECTION_DSL_1_3,
                          resolver=resolver)

    def parse_1_4(self, dsl_string, resources_base_path=None, resolver=None):
        return self.parse(dsl_string, resources_base_path,
                          dsl_version=self.BASIC_VERSION_SECTION_DSL_1_4,
                          resolver=resolver)

    def parse_1_5(self, dsl_string, resources_base_path=None, resolver=None):
        return self.parse(dsl_string, resources_base_path,
                          dsl_version=self.BASIC_VERSION_SECTION_DSL_1_5,
                          resolver=resolver)

    def parse_from_path(self,
                        dsl_path,
                        resources_base_path=None,
                        resolver=None):
        if not resolver:
            resolver = DefaultImportResolver(
                rules=self._local_resolver_rules())
        return dsl_parse_from_path(dsl_path,
                                   resources_base_path,
                                   resolver=resolver)

    def parse_multi(self, yaml):
        return create_deployment_plan(self.parse_1_3(yaml))

    @staticmethod
    def modify_multi(plan, modified_nodes):
        return modify_deployment(
            nodes=plan['nodes'],
            previous_nodes=plan['nodes'],
            previous_node_instances=plan['node_instances'],
            modified_nodes=modified_nodes,
            scaling_groups=plan['scaling_groups'])

    def _assert_dsl_parsing_exception_error_code(
            self, dsl,
            expected_error_code, exception_type=DSLParsingException,
            parsing_method=None, message_regex=None):
        if not parsing_method:
            parsing_method = self.parse
        if message_regex:
            with self.assertRaisesRegex(exception_type, message_regex) as cm:
                parsing_method(dsl)
            ex = cm.exception
        else:
            with self.assertRaises(exception_type) as cm:
                parsing_method(dsl)
            ex = cm.exception
        self.assertEqual(ex.err_code, expected_error_code)
        return ex

    def get_node_by_name(self, plan, name):
        return [x for x in plan.node_templates if x['name'] == name][0]

    @staticmethod
    def _sort_result_nodes(result_nodes, ordered_nodes_ids):
        ordered_nodes = []

        for node_id in ordered_nodes_ids:
            for result_node in result_nodes:
                if result_node['id'] == node_id:
                    ordered_nodes.append(result_node)
                    break

        return ordered_nodes

    mock_evaluation_storage = _MockRuntimeEvaluationStorage

    def get_secret(self, secret_name):
        return self.mock_evaluation_storage().get_secret(secret_name)


# 2.7 compat
if not hasattr(AbstractTestParser, 'assertRaisesRegex'):
    AbstractTestParser.assertRaisesRegex = \
        AbstractTestParser.assertRaisesRegexp
if not hasattr(AbstractTestParser, 'assertRegex'):
    AbstractTestParser.assertRegex = \
        AbstractTestParser.assertRegexpMatches
