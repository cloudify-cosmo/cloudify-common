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
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestNamespacedGetProperty(AbstractTestParser):

    def test_node_template_properties(self):
        imported_yaml = """
node_types:
    vm_type:
        properties:
            ip: {}
            ip_duplicate: {}
    server_type:
        properties:
            endpoint: {}
node_templates:
    vm:
        type: vm_type
        properties:
            ip: 10.0.0.1
            ip_duplicate: { get_property: [ SELF, ip ] }
    server:
        type: server_type
        properties:
            endpoint: { get_property: [ vm, ip ] }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed = prepare_deployment_plan(self.parse(main_yaml))
        vm = self.get_node_by_name(parsed, 'test->vm')
        self.assertEqual('10.0.0.1', vm['properties']['ip_duplicate'])
        server = self.get_node_by_name(parsed, 'test->server')
        self.assertEqual('10.0.0.1', server['properties']['endpoint'])

    def test_node_template_properties_with_dsl_definitions(self):
        imported_yaml = """
dsl_definitions:
    props: &props
        prop2: { get_property: [SELF, prop1] }
        prop3:
            nested: { get_property: [SELF, prop1] }
node_types:
    type1:
        properties:
            prop1: {}
            prop2: {}
            prop3: {}
node_templates:
    node1:
        type: type1
        properties:
            <<: *props
            prop1: value1
    node2:
        type: type1
        properties:
            <<: *props
            prop1: value2
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse_1_2(main_yaml))
        props1 = self.get_node_by_name(plan, 'test->node1')['properties']
        props2 = self.get_node_by_name(plan, 'test->node2')['properties']
        self.assertEqual({
            'prop1': 'value1',
            'prop2': 'value1',
            'prop3': {'nested': 'value1'}
        }, props1)
        self.assertEqual({
            'prop1': 'value2',
            'prop2': 'value2',
            'prop3': {'nested': 'value2'}
        }, props2)

    def test_node_template_interfaces(self):
        imported_yaml = """
plugins:
    plugin:
        executor: central_deployment_agent
        install: false
node_types:
    vm_type:
        properties:
            ip:
                type: string
node_templates:
    vm:
        type: vm_type
        properties:
            ip: 10.0.0.1
        interfaces:
            interface:
                op:
                    implementation: plugin.op
                    inputs:
                        x: { get_property: [vm, ip] }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed = prepare_deployment_plan(self.parse(main_yaml))
        vm = self.get_node_by_name(parsed, 'test->vm')
        self.assertEqual('10.0.0.1', vm['operations']['op']['inputs']['x'])
        self.assertEqual('10.0.0.1',
                         vm['operations']['interface.op']['inputs']['x'])

    def test_node_template_interfaces_with_dsl_definitions(self):
        imported_yaml = """
dsl_definitions:
    op: &op
        implementation: plugin.op
        inputs:
            x: { get_property: [SELF, prop1] }
plugins:
    plugin:
        executor: central_deployment_agent
        install: false
node_types:
    type1:
        properties:
            prop1: {}
node_templates:
    node1:
        type: type1
        properties:
            prop1: value1
        interfaces:
            interface:
                op: *op
    node2:
        type: type1
        properties:
            prop1: value2
        interfaces:
            interface:
                op: *op
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed = prepare_deployment_plan(self.parse_1_2(main_yaml))
        node1 = self.get_node_by_name(parsed, 'test->node1')
        node2 = self.get_node_by_name(parsed, 'test->node2')
        self.assertEqual('value1', node1['operations']['op']['inputs']['x'])
        self.assertEqual('value1',
                         node1['operations']['interface.op']['inputs']['x'])
        self.assertEqual('value2', node2['operations']['op']['inputs']['x'])
        self.assertEqual('value2',
                         node2['operations']['interface.op']['inputs']['x'])

    def test_node_template_capabilities(self):
        imported_yaml = """
node_templates:
    node:
        type: type
        capabilities:
            scalable:
                properties:
                    default_instances: { get_property: [node, prop1] }
                    max_instances: { get_property: [SELF, prop1] }
                    min_instances: { get_input: my_input }
inputs:
    my_input:
        default: 20
node_types:
    type:
        properties:
            prop1:
                default: 10
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed = prepare_deployment_plan(self.parse_1_3(main_yaml))
        node = self.get_node_by_name(parsed, 'test->node')
        self.assertEqual({
            'default_instances': 10,
            'min_instances': 20,
            'max_instances': 10,
            'current_instances': 10,
            'planned_instances': 10,
        }, node['capabilities']['scalable']['properties'])

    def test_policies_properties(self):
        imported_yaml = """
node_templates:
    node:
        type: type
inputs:
    my_input:
        default: 20
node_types:
    type:
        properties:
            prop1:
                default: 10
groups:
    group:
        members: [node]
policies:
    policy:
        type: cloudify.policies.scaling
        targets: [group]
        properties:
            default_instances: { get_property: [node, prop1] }
            min_instances: { get_input: my_input }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed = prepare_deployment_plan(self.parse_1_3(main_yaml))
        expected = {
            'default_instances': 10,
            'min_instances': 20,
            'max_instances': -1,
            'current_instances': 10,
            'planned_instances': 10,
        }
        self.assertEqual(expected,
                         parsed['scaling_groups']['test->group']['properties'])
        self.assertEqual(expected,
                         parsed['policies']['test->policy']['properties'])

    def test_recursive(self):
        imported_yaml = """
inputs:
    i:
        default: 1
node_types:
    vm_type:
        properties:
            a: { type: string }
            b: { type: string }
            c: { type: string }
            x: { type: string }
            y: { type: string }
            z: { type: string }
node_templates:
    vm:
        type: vm_type
        properties:
            a: 0
            b: { get_property: [ SELF, a ] }
            c: { get_property: [ SELF, b ] }
            x: { get_property: [ SELF, z ] }
            y: { get_property: [ SELF, x ] }
            z: { get_input: i }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed = prepare_deployment_plan(self.parse(main_yaml))
        vm = self.get_node_by_name(parsed, 'test->vm')
        self.assertEqual(0, vm['properties']['b'])
        self.assertEqual(1, vm['properties']['x'])
        self.assertEqual(1, vm['properties']['y'])
        self.assertEqual(1, vm['properties']['z'])

    def test_source_and_target_interfaces(self):
        imported_yaml = """
plugins:
    plugin:
        executor: central_deployment_agent
        source: dummy
node_types:
    some_type:
        properties:
            a: { type: string }
relationships:
    cloudify.relationships.contained_in: {}
    rel:
        derived_from: cloudify.relationships.contained_in
        source_interfaces:
            source_interface:
                op1:
                    implementation: plugin.operation
                    inputs:
                        source_a:
                            default: { get_property: [node2, a] }
                        target_a:
                            default: { get_property: [node1, a] }
        target_interfaces:
            target_interface:
                op2:
                    implementation: plugin.operation
                    inputs:
                        source_a:
                            default: { get_property: [node2, a] }
                        target_a:
                            default: { get_property: [node1, a] }
node_templates:
    node1:
        type: some_type
        properties:
            a: 1
    node2:
        type: some_type
        properties:
            a: 2
        relationships:
            -   type: rel
                target: node1
"""

        def do_assertions():
            """
            Assertions are made for explicit node names in a relationship
            and another time for SOURCE & TARGET keywords.
            """
            node = self.get_node_by_name(prepared, 'test->node2')
            source_ops = node['relationships'][0]['source_operations']
            self.assertEqual(2,
                             source_ops['source_interface.op1']['inputs']
                             ['source_a'])
            self.assertEqual(1,
                             source_ops['source_interface.op1']['inputs']
                             ['target_a'])
            target_ops = node['relationships'][0]['target_operations']
            self.assertEqual(2,
                             target_ops['target_interface.op2']['inputs']
                             ['source_a'])
            self.assertEqual(1,
                             target_ops['target_interface.op2']['inputs']
                             ['target_a'])
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)
        # Explicit node template names
        prepared = prepare_deployment_plan(self.parse(main_yaml % {
            'source': 'test->node2', 'target': 'node1'}))
        do_assertions()

    def test_recursive_with_nesting(self):
        imported_yaml = """
node_types:
    vm_type:
        properties:
            a: { type: string }
            b: { type: string }
            c: { type: string }
node_templates:
    vm:
        type: vm_type
        properties:
            a: 1
            b: { get_property: [SELF, c] }
            c: [ { get_property: [SELF, a ] }, 2 ]
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)
        parsed = prepare_deployment_plan(self.parse(main_yaml))
        vm = self.get_node_by_name(parsed, 'test->vm')
        self.assertEqual(1, vm['properties']['b'][0])
        self.assertEqual(2, vm['properties']['b'][1])
        self.assertEqual(1, vm['properties']['c'][0])
        self.assertEqual(2, vm['properties']['c'][1])

    def test_recursive_get_property_in_outputs(self):
        imported_yaml = """
node_types:
    vm_type:
        properties:
            a: { type: string }
            b: { type: string }
            c: { type: string }
node_templates:
    vm:
        type: vm_type
        properties:
            a: 1
            b: { get_property: [SELF, c] }
            c: [ { get_property: [SELF, a ] }, 2 ]
outputs:
    o:
        value:
            a: { get_property: [vm, b] }
            b: [0, { get_property: [vm, b] }]
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed = prepare_deployment_plan(self.parse(main_yaml))
        output_value = parsed.outputs['test->o']['value']
        self.assertEqual(1, output_value['a'][0])
        self.assertEqual(2, output_value['a'][1])
        self.assertEqual(0, output_value['b'][0])
        self.assertEqual(1, output_value['b'][1][0])
        self.assertEqual(2, output_value['b'][1][1])

    def test_nested_property_path(self):
        imported_yaml = """
node_types:
    vm_type:
        properties:
            endpoint: {}
            a: { type: integer }
            b: {}
            c: {}
    server_type:
        properties:
            port: { type: integer }
node_templates:
    vm:
        type: vm_type
        properties:
            endpoint:
                url:
                    protocol: http
                port: 80
                names: [site1, site2, site3]
                pairs:
                    - key: key1
                      value: value1
                    - key: key2
                      value: value2
            a: { get_property: [ SELF, endpoint, port ] }
            b: { get_property: [ SELF, endpoint, names, 0 ] }
            c: { get_property: [ SELF, endpoint, pairs, 1 , key] }
    server:
        type: server_type
        properties:
            port: { get_property: [ vm, endpoint, port ] }
outputs:
    a:
        value: { get_property: [ vm, endpoint, port ] }
    b:
        value: { get_property: [ vm, endpoint, url, protocol ] }
    c:
        value: { get_property: [ vm, endpoint, names, 1 ] }
    d:
        value: { get_property: [ vm, endpoint, pairs, 1, value] }

"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed = prepare_deployment_plan(self.parse(main_yaml))
        vm = self.get_node_by_name(parsed, 'test->vm')
        self.assertEqual(80, vm['properties']['a'])
        self.assertEqual('site1', vm['properties']['b'])
        self.assertEqual('key2', vm['properties']['c'])
        server = self.get_node_by_name(parsed, 'test->server')
        self.assertEqual(80, server['properties']['port'])
        outputs = parsed.outputs
        self.assertEqual(80, outputs['test->a']['value'])
        self.assertEqual('http', outputs['test->b']['value'])
        self.assertEqual('site2', outputs['test->c']['value'])
        self.assertEqual('value2', outputs['test->d']['value'])

    def test_get_property_from_namespaced_node(self):
        imported_yaml = """
node_types:
    test_type:
        properties:
            key:
                default: value
node_templates:
    node:
        type: test_type
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)
        main_yaml += """
outputs:
    port:
        description: the port
        value: { get_property: [test->node, key] }
"""

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml),
                                              self._get_secret_mock)
        self.assertEqual(
            'value',
            parsed_yaml[constants.OUTPUTS]['port']['value'])

    def test_multi_layer_namespaced_import(self):
        layer1_yaml = """
node_types:
    test_type:
        properties:
            key:
                default: value
node_templates:
    node:
        type: test_type
"""
        layer1_file_name = self.make_yaml_file(layer1_yaml)

        layer2_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('middle_test', layer1_file_name)
        layer2_file_name = self.make_yaml_file(layer2_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', layer2_file_name)
        main_yaml += """
outputs:
    port:
        description: the port
        value: { get_property: [test->middle_test->node, key] }
"""

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml),
                                              self._get_secret_mock)
        self.assertEqual(
            'value',
            parsed_yaml[constants.OUTPUTS]['port']['value'])


class TestGetAttribute(AbstractTestParser):
    def test_has_intrinsic_functions_property(self):
        imported_yaml = """
relationships:
    cloudify.relationships.contained_in: {}
plugins:
    p:
        executor: central_deployment_agent
        install: false
node_types:
    webserver_type: {}
node_templates:
    node:
        type: webserver_type
    webserver:
        type: webserver_type
        interfaces:
            test:
                op_with_no_get_attribute:
                    implementation: p.p
                    inputs:
                        a: 1
                op_with_get_attribute:
                    implementation: p.p
                    inputs:
                        a: { get_attribute: [SELF, a] }
        relationships:
            -   type: cloudify.relationships.contained_in
                target: node
                source_interfaces:
                    test:
                        op_with_no_get_attribute:
                            implementation: p.p
                            inputs:
                                a: 1
                        op_with_get_attribute:
                            implementation: p.p
                            inputs:
                                a: { get_attribute: [SOURCE, a] }
                target_interfaces:
                    test:
                        op_with_no_get_attribute:
                            implementation: p.p
                            inputs:
                                a: 1
                        op_with_get_attribute:
                            implementation: p.p
                            inputs:
                                a: { get_attribute: [TARGET, a] }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)
        parsed = prepare_deployment_plan(self.parse(main_yaml))
        webserver_node = None
        for node in parsed.node_templates:
            if node['id'] == 'test->webserver':
                webserver_node = node
                break
        self.assertIsNotNone(webserver_node)

        def assertion(operations):
            op = operations['test.op_with_no_get_attribute']
            self.assertIs(False, op.get('has_intrinsic_functions'))
            op = operations['test.op_with_get_attribute']
            self.assertIs(True, op.get('has_intrinsic_functions'))

        assertion(webserver_node['operations'])
        assertion(webserver_node['relationships'][0]['source_operations'])
        assertion(webserver_node['relationships'][0]['target_operations'])

    def test_get_attribute_from_namespaced_node(self):
        imported_yaml = """
node_types:
    test_type:
        properties:
            key:
                default: value
node_templates:
    node:
        type: test_type
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)
        main_yaml += """
outputs:
    port:
        description: the port
        value: { get_attribute: [test->node, key] }
"""

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml),
                                              self._get_secret_mock)
        self.assertEqual(
            {'get_attribute': ['test->node', 'key']},
            parsed_yaml[constants.OUTPUTS]['port']['value'])

    def test_basic_namespace_multi_import(self):
        layer1_yaml = """
node_types:
    test_type:
        properties:
            key:
                default: value
node_templates:
    node:
        type: test_type
"""
        layer1_file_name = self.make_yaml_file(layer1_yaml)

        layer2_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('middle_test', layer1_file_name)
        layer2_file_name = self.make_yaml_file(layer2_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', layer2_file_name)
        main_yaml += """
outputs:
    port:
        description: the port
        value: { get_attribute: [test->middle_test->node, key] }
"""

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml),
                                              self._get_secret_mock)
        self.assertEqual(
            {'get_attribute': ['test->middle_test->node', 'key']},
            parsed_yaml[constants.OUTPUTS]['port']['value'])


class TestConcat(AbstractTestParser):
    def test_concat_with_namespace(self):
        imported_yaml = """
    outputs:
        port:
            description: the port
            value: { concat: [one, two, three] }
    """
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml))
        self.assertEqual(1, len(parsed_yaml[constants.OUTPUTS]))
        self.assertEqual(
            'onetwothree',
            parsed_yaml[constants.OUTPUTS]['test->port']['value'])


class TestNonRelatedNodeFunctions(AbstractTestParser):
    def test_get_secret(self):
        imported_yaml = """
outputs:
    port:
        description: the port
        value: { get_secret: secret }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml),
                                              self._get_secret_mock)
        self.assertEqual(
            {'get_secret': 'secret'},
            parsed_yaml[constants.OUTPUTS]['test->port']['value'])

    def test_get_capability(self):
        imported_yaml = """
outputs:
    port:
        description: the port
        value: { get_capability: [ dep_1, cap_a ] }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml),
                                              self._get_secret_mock)
        self.assertEqual(
            {'get_capability': ['dep_1', 'cap_a']},
            parsed_yaml[constants.OUTPUTS]['test->port']['value'])

    def test_get_input(self):
        imported_yaml = """
inputs:
    port:
        default: 8080
outputs:
    port:
        description: the port
        value: { get_input: port }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml),
                                              self._get_secret_mock)
        self.assertEqual(
            8080,
            parsed_yaml[constants.OUTPUTS]['test->port']['value'])

    def test_multi_layer_import(self):
        layer1_yaml = """
inputs:
    port:
        default: 8080
outputs:
    port:
        description: the port
        value: { get_input: port }
"""
        layer1_file_name = self.make_yaml_file(layer1_yaml)

        layer2_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('middle_test', layer1_file_name)
        layer2_file_name = self.make_yaml_file(layer2_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', layer2_file_name)

        parsed_yaml = prepare_deployment_plan(self.parse(main_yaml),
                                              self._get_secret_mock)
        self.assertEqual(
            8080,
            parsed_yaml[constants.OUTPUTS]['test->middle_test->port']['value'])
