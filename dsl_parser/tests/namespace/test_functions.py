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
from dsl_parser.tests.utils import ResolverWithBlueprintSupport as Resolver


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
    -   {0}--{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse(main_yaml))
        vm = self.get_node_by_name(plan, 'test--vm')
        self.assertEqual('10.0.0.1', vm[constants.PROPERTIES]['ip_duplicate'])
        server = self.get_node_by_name(plan, 'test--server')
        self.assertEqual('10.0.0.1', server[constants.PROPERTIES]['endpoint'])

    def test_nested_functions_calls(self):
        imported_yaml = """
inputs:
    name:
        default: ip
node_types:
    vm_type: {}
    server_type:
        properties:
            endpoint: {}
node_templates:
    vm:
        type: vm_type
    server:
        type: server_type
        properties:
            endpoint: { get_property: [ vm, { get_input: name } ] }
"""
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
""".format('test', import_file_name)

        parsed = self.parse(main_yaml)
        vm = self.get_node_by_name(parsed, 'test--server')
        self.assertEqual(vm[constants.PROPERTIES]['endpoint'],
                         {'get_property':
                             ['test--vm', {'get_input': 'test--name'}]})

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
    -   {0}--{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse_1_2(main_yaml))
        props1 = self.get_node_by_name(plan,
                                       'test--node1')[constants.PROPERTIES]
        props2 = self.get_node_by_name(plan,
                                       'test--node2')[constants.PROPERTIES]
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

        main_yaml = """
imports:
    -   {0}--{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse(main_yaml))
        vm = self.get_node_by_name(plan, 'test--vm')
        self.assertEqual('10.0.0.1',
                         vm['operations']['op'][constants.INPUTS]['x'])
        self.assertEqual(
            '10.0.0.1',
            vm['operations']['interface.op'][constants.INPUTS]['x'])

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

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
imports:
    -   {0}--{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse_1_2(main_yaml))
        node1_operations = self.get_node_by_name(plan,
                                                 'test--node1')['operations']
        node2_operations = self.get_node_by_name(plan,
                                                 'test--node2')['operations']
        self.assertEqual('value1',
                         node1_operations['op'][constants.INPUTS]['x'])
        self.assertEqual(
            'value1',
            node1_operations['interface.op'][constants.INPUTS]['x'])
        self.assertEqual('value2',
                         node2_operations['op'][constants.INPUTS]['x'])
        self.assertEqual(
            'value2',
            node2_operations['interface.op'][constants.INPUTS]['x'])

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
    -   {0}--{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse_1_3(main_yaml))
        node = self.get_node_by_name(plan, 'test--node')
        self.assertEqual({
            'default_instances': 10,
            'min_instances': 20,
            'max_instances': 10,
            'current_instances': 10,
            'planned_instances': 10,
        }, node[constants.CAPABILITIES]['scalable'][constants.PROPERTIES])

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
    -   {0}--{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse_1_3(main_yaml))
        expected = {
            'default_instances': 10,
            'min_instances': 20,
            'max_instances': -1,
            'current_instances': 10,
            'planned_instances': 10,
        }
        self.assertEqual(
            expected,
            plan['scaling_groups']['test--group'][constants.PROPERTIES])
        self.assertEqual(
            expected,
            plan[constants.POLICIES]['test--policy'][constants.PROPERTIES])

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
    -   {0}--{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse(main_yaml))
        vm = self.get_node_by_name(plan, 'test--vm')
        self.assertEqual(0, vm[constants.PROPERTIES]['b'])
        self.assertEqual(1, vm[constants.PROPERTIES]['x'])
        self.assertEqual(1, vm[constants.PROPERTIES]['y'])
        self.assertEqual(1, vm[constants.PROPERTIES]['z'])

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
        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
""".format('test', import_file_name)
        plan = prepare_deployment_plan(self.parse(main_yaml % {
            'source': 'test--node2', 'target': 'node1'}))
        node = self.get_node_by_name(plan, 'test--node2')
        source_ops = node[constants.RELATIONSHIPS][0]['source_operations']
        self.assertEqual(2,
                         source_ops['source_interface.op1'][constants.INPUTS]
                         ['source_a'])
        self.assertEqual(1,
                         source_ops['source_interface.op1'][constants.INPUTS]
                         ['target_a'])
        target_ops = node[constants.RELATIONSHIPS][0]['target_operations']
        self.assertEqual(2,
                         target_ops['target_interface.op2'][constants.INPUTS]
                         ['source_a'])
        self.assertEqual(1,
                         target_ops['target_interface.op2'][constants.INPUTS]
                         ['target_a'])

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
    -   {0}--{1}
""".format('test', import_file_name)
        plan = prepare_deployment_plan(self.parse(main_yaml))
        vm = self.get_node_by_name(plan, 'test--vm')
        self.assertEqual(1, vm[constants.PROPERTIES]['b'][0])
        self.assertEqual(2, vm[constants.PROPERTIES]['b'][1])
        self.assertEqual(1, vm[constants.PROPERTIES]['c'][0])
        self.assertEqual(2, vm[constants.PROPERTIES]['c'][1])

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
    -   {0}--{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse(main_yaml))
        output_value = plan.outputs['test--o']['value']
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
    -   {0}--{1}
""".format('test', import_file_name)

        parsed = prepare_deployment_plan(self.parse(main_yaml))
        vm = self.get_node_by_name(parsed, 'test--vm')
        self.assertEqual(80, vm[constants.PROPERTIES]['a'])
        self.assertEqual('site1', vm[constants.PROPERTIES]['b'])
        self.assertEqual('key2', vm[constants.PROPERTIES]['c'])
        server = self.get_node_by_name(parsed, 'test--server')
        self.assertEqual(80, server[constants.PROPERTIES]['port'])
        outputs = parsed.outputs
        self.assertEqual(80, outputs['test--a']['value'])
        self.assertEqual('http', outputs['test--b']['value'])
        self.assertEqual('site2', outputs['test--c']['value'])
        self.assertEqual('value2', outputs['test--d']['value'])

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
    -   {0}--{1}
""".format('test', import_file_name)
        main_yaml += """
outputs:
    port:
        description: the port
        value: { get_property: [test--node, key] }
"""

        plan = prepare_deployment_plan(self.parse(main_yaml), self.get_secret)
        self.assertEqual(
            'value',
            plan[constants.OUTPUTS]['port']['value'])

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
    -   {0}--{1}
""".format('middle_test', layer1_file_name)
        layer2_file_name = self.make_yaml_file(layer2_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
""".format('test', layer2_file_name)
        main_yaml += """
outputs:
    port:
        description: the port
        value: { get_property: [test--middle_test--node, key] }
"""

        plan = prepare_deployment_plan(self.parse(main_yaml), self.get_secret)
        self.assertEqual(
            'value',
            plan[constants.OUTPUTS]['port']['value'])

    def test_node_template_properties_with_blueprint_import(self):
        basic_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
inputs:
    port:
        default: 90
node_templates:
  node:
    type: cloudify.nodes.WebServer
    properties:
      port: { get_input: port}
  ip:
    type: cloudify.nodes.WebServer
    properties:
      port: { get_property: [node, port] }
"""
        second_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns--blueprint:first_layer
"""
        resolver = Resolver({'blueprint:first_layer': basic_yaml,
                             'blueprint:second_layer': second_layer})
        third_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns2--blueprint:second_layer
"""
        parsed = self.parse(third_layer, resolver=resolver)
        for node in parsed[constants.NODES]:
            if node['name'].endswith('ip'):
                ip = node
                break
        else:
            raise RuntimeError('No node "ip" found')
        self.assertEqual(ip['id'], 'ns2--ns--ip')
        self.assertEquals(ip['properties']['port'],
                          {'get_property': ['ns2--ns--node', 'port']})


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
    -   {0}--{1}
""".format('test', import_file_name)
        parsed = prepare_deployment_plan(self.parse(main_yaml))
        webserver_node = None
        for node in parsed.node_templates:
            if node['id'] == 'test--webserver':
                webserver_node = node
                break
        self.assertIsNotNone(webserver_node)

        def assert_operation_fields(operations_list):
            for operations in operations_list:
                op = operations['test.op_with_no_get_attribute']
                self.assertIs(False, op.get('has_intrinsic_functions'))
                op = operations['test.op_with_get_attribute']
                self.assertIs(True, op.get('has_intrinsic_functions'))

        assert_operation_fields(
            [webserver_node['operations'],
             webserver_node[constants.RELATIONSHIPS][0]['source_operations'],
             webserver_node[constants.RELATIONSHIPS][0]['target_operations']])

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
    -   {0}--{1}
""".format('test', import_file_name)
        main_yaml += """
outputs:
    port:
        description: the port
        value: { get_attribute: [test--node, key] }
"""

        plan = prepare_deployment_plan(self.parse(main_yaml), self.get_secret)
        self.assertEqual(
            {'get_attribute': ['test--node', 'key']},
            plan[constants.OUTPUTS]['port']['value'])

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
    -   {0}--{1}
""".format('middle_test', layer1_file_name)
        layer2_file_name = self.make_yaml_file(layer2_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
""".format('test', layer2_file_name)
        main_yaml += """
outputs:
    port:
        description: the port
        value: { get_attribute: [test--middle_test--node, key] }
"""

        plan = prepare_deployment_plan(self.parse(main_yaml), self.get_secret)
        self.assertEqual(
            {'get_attribute': ['test--middle_test--node', 'key']},
            plan[constants.OUTPUTS]['port']['value'])

    def test_node_template_properties_with_blueprint_import(self):
        basic_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
inputs:
    port:
        default: 90
node_templates:
  node:
    type: cloudify.nodes.WebServer
    properties:
      port: { get_input: port}
  ip:
    type: cloudify.nodes.WebServer
    properties:
      port: { get_attribute: [node, port] }
"""
        second_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns--blueprint:first_layer
"""
        resolver = Resolver({'blueprint:first_layer': basic_yaml,
                             'blueprint:second_layer': second_layer})
        third_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns2--blueprint:second_layer
"""
        parsed = self.parse(third_layer, resolver=resolver)
        ip = parsed[constants.NODES][0]
        for node in parsed[constants.NODES]:
            if node['name'].endswith('ip'):
                ip = node
                break
        else:
            raise RuntimeError('No node "ip" found')
        self.assertEqual(ip['id'], 'ns2--ns--ip')
        self.assertEquals(ip['properties']['port'],
                          {'get_attribute': ['ns2--ns--node', 'port']})

    def test_node_type_properties_with_blueprint_import(self):
        basic_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
node_types:
  test:
    properties:
      server:
        default:
          image: { get_attribute: [node, port] }
node_templates:
  vm:
    type: test
  node:
    type: test
"""
        second_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns--blueprint:first_layer
"""
        resolver = Resolver({'blueprint:first_layer': basic_yaml,
                             'blueprint:second_layer': second_layer})
        third_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns2--blueprint:second_layer
"""
        parsed = self.parse(third_layer, resolver=resolver)
        vm_props = parsed[constants.NODES][0][constants.PROPERTIES]
        self.assertEqual(vm_props['server']['image'],
                         {'get_attribute': ['ns2--ns--node', 'port']})


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
    -   {0}--{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse(main_yaml))
        self.assertEqual(1, len(plan[constants.OUTPUTS]))
        self.assertEqual(
            'onetwothree',
            plan[constants.OUTPUTS]['test--port']['value'])

    def test_node_template_properties_with_blueprint_import(self):
        basic_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
inputs:
    port:
        default: 90
node_templates:
  ip:
    type: cloudify.nodes.WebServer
    properties:
      port: { concat: [one, { get_input: port }, three] }
"""
        second_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns--blueprint:first_layer
"""
        resolver = Resolver({'blueprint:first_layer': basic_yaml,
                             'blueprint:second_layer': second_layer})
        third_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns2--blueprint:second_layer
"""
        parsed = self.parse(third_layer, resolver=resolver)
        for node in parsed[constants.NODES]:
            if node['name'].endswith('ip'):
                ip = node
                break
        else:
            raise RuntimeError('No node "ip" found')
        self.assertEqual(ip['id'], 'ns2--ns--ip')
        self.assertEquals(ip['properties']['port'],
                          {'concat':
                            ['one', {'get_input': 'ns2--ns--port'}, 'three']})

    def test_node_type_properties_with_blueprint_import(self):
        basic_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
inputs:
    port:
        default: 90
node_types:
  test:
    properties:
      server:
        default:
          image: { concat: [one, { get_input: port }, three] }
node_templates:
  vm:
    type: test
"""
        second_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns--blueprint:first_layer
"""
        resolver = Resolver({'blueprint:first_layer': basic_yaml,
                             'blueprint:second_layer': second_layer})
        third_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns2--blueprint:second_layer
"""
        parsed = self.parse(third_layer, resolver=resolver)
        inputs = parsed[constants.INPUTS]
        self.assertIn('ns2--ns--port', inputs)
        vm_props = parsed[constants.NODES][0][constants.PROPERTIES]
        self.assertEqual(vm_props['server']['image'],
                         {'concat':
                          ['one', {'get_input': 'ns2--ns--port'}, 'three']})

    def test_outputs_with_blueprint_import(self):
        basic_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
inputs:
    port:
        default: 90
outputs:
  port:
    value: { concat: [one, { get_input: port }, three] }
"""
        second_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns--blueprint:first_layer
"""
        resolver = Resolver({'blueprint:first_layer': basic_yaml,
                             'blueprint:second_layer': second_layer})
        third_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns2--blueprint:second_layer
"""
        parsed = self.parse(third_layer, resolver=resolver)
        outputs = parsed[constants.OUTPUTS]
        self.assertEqual(outputs['ns2--ns--port']['value'],
                         {'concat':
                         ['one', {'get_input': 'ns2--ns--port'}, 'three']})


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
    -   {0}--{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse(main_yaml), self.get_secret)
        self.assertEqual(
            {'get_secret': 'secret'},
            plan[constants.OUTPUTS]['test--port']['value'])

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
    -   {0}--{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse(main_yaml), self.get_secret)
        self.assertEqual(
            {'get_capability': ['dep_1', 'cap_a']},
            plan[constants.OUTPUTS]['test--port']['value'])


class TestGetInput(AbstractTestParser):
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
    -   {0}--{1}
""".format('test', import_file_name)

        plan = prepare_deployment_plan(self.parse(main_yaml), self.get_secret)
        self.assertEqual(
            8080,
            plan[constants.OUTPUTS]['test--port']['value'])

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
    -   {0}--{1}
""".format('middle_test', layer1_file_name)
        layer2_file_name = self.make_yaml_file(layer2_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
""".format('test', layer2_file_name)

        plan = prepare_deployment_plan(self.parse(main_yaml), self.get_secret)
        self.assertEqual(
            8080,
            plan[constants.OUTPUTS]['test--middle_test--port']['value'])

    def test_node_template_properties_with_blueprint_import(self):
        basic_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
inputs:
    port:
        default: 90
node_templates:
  ip:
    type: cloudify.nodes.WebServer
    properties:
      port: { get_input: port }
"""
        second_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns--blueprint:first_layer
"""
        resolver = Resolver({'blueprint:first_layer': basic_yaml,
                             'blueprint:second_layer': second_layer})
        third_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns2--blueprint:second_layer
"""
        parsed = self.parse(third_layer, resolver=resolver)
        inputs = parsed[constants.INPUTS]
        self.assertIn('ns2--ns--port', inputs)

    def test_node_type_properties_with_blueprint_import(self):
        basic_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
inputs:
    port:
        default: 90
node_types:
  test:
    properties:
      rules:
        default: []
      server:
        default:
          image: { get_input: port }
node_templates:
  vm:
    type: test
    properties:
      rules:
        - remote_ip_prefix: 0.0.0.0/0
          port: { get_input: port }
"""
        second_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns--blueprint:first_layer
"""
        resolver = Resolver({'blueprint:first_layer': basic_yaml,
                             'blueprint:second_layer': second_layer})
        third_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns2--blueprint:second_layer
"""
        parsed = self.parse(third_layer, resolver=resolver)
        inputs = parsed[constants.INPUTS]
        self.assertIn('ns2--ns--port', inputs)
        vm_props = parsed[constants.NODES][0][constants.PROPERTIES]
        self.assertEqual(vm_props['server']['image'],
                         {'get_input': 'ns2--ns--port'})
        self.assertEqual(vm_props['rules'][0],
                         {'remote_ip_prefix': '0.0.0.0/0',
                          'port': {'get_input': 'ns2--ns--port'}})

    def test_dsl_definitions_with_blueprint_import(self):
        basic_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - http://www.getcloudify.org/spec/cloudify/4.5/types.yaml
inputs:
    port:
        default: 90
dsl_definitions:
  config: &config
    region: { get_input: port }
node_types:
    test:
        properties:
            port:
                default: 0
            data:
                default: []
node_templates:
  vm:
    type: test
    properties:
      port: *config
      data: { get_input: port }
  vm2:
    type: test
    properties:
      port: *config
      data: { get_input: port }
"""
        second_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns--blueprint:first_layer
"""
        resolver = Resolver({'blueprint:first_layer': basic_yaml,
                             'blueprint:second_layer': second_layer})
        third_layer = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    - ns2--blueprint:second_layer
"""
        parsed = self.parse(third_layer, resolver=resolver)
        inputs = parsed[constants.INPUTS]
        self.assertIn('ns2--ns--port', inputs)
        vm_properties = parsed[constants.NODES][0][constants.PROPERTIES]
        self.assertEqual(vm_properties['port']['region'],
                         {'get_input': 'ns2--ns--port'})
        vm_properties = parsed[constants.NODES][1][constants.PROPERTIES]
        self.assertEqual(vm_properties['port']['region'],
                         {'get_input': 'ns2--ns--port'})
        self.assertEqual(vm_properties['data'],
                         {'get_input': 'ns2--ns--port'})
