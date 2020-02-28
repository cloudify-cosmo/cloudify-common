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

from dsl_parser import constants as consts
from dsl_parser._compat import text_type
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.exceptions import (UnknownInputError,
                                   ERROR_UNKNOWN_TYPE,
                                   ConstraintException,
                                   DSLParsingException,
                                   InputEvaluationError,
                                   ERROR_MISSING_PROPERTY,
                                   ERROR_UNDEFINED_PROPERTY,
                                   DSLParsingLogicException,
                                   MissingRequiredInputError,
                                   DSLParsingInputTypeException,
                                   ERROR_VALUE_DOES_NOT_MATCH_TYPE,
                                   ERROR_INPUT_VIOLATES_DATA_TYPE_SCHEMA)
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestInputs(AbstractTestParser):

    def test_empty_inputs(self):
        yaml = """
inputs: {}
"""
        parsed = self.parse(yaml)
        self.assertEqual(0, len(parsed[consts.INPUTS]))

    def test_input_definition(self):
        yaml = """
inputs:
    port:
        description: the port
        default: 8080
node_templates: {}
"""
        parsed = self.parse(yaml)
        self.assertEqual(1, len(parsed[consts.INPUTS]))
        self.assertEqual(
            8080, parsed[consts.INPUTS]['port'][consts.DEFAULT])
        self.assertEqual(
            'the port', parsed[consts.INPUTS]['port'][consts.DESCRIPTION])

    def test_inputs_definition(self):
        yaml = """
inputs:
    port:
        description: the port
        default: 8080
    port2:
        default: 9090
    ip: {}
node_templates: {}
"""
        parsed = self.parse(yaml)
        inputs = parsed[consts.INPUTS]
        self.assertEqual(3, len(inputs))
        self.assertEqual(8080, inputs['port'][consts.DEFAULT])
        self.assertEqual('the port', inputs['port']['description'])
        self.assertEqual(9090, inputs['port2'][consts.DEFAULT])
        self.assertNotIn('description', inputs['port2'])
        self.assertEqual(0, len(inputs['ip']))

    def test_verify_get_input_in_properties(self):
        yaml = """
node_types:
    webserver_type:
        properties:
            port: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: port }
"""
        self.assertRaises(UnknownInputError, self.parse, yaml)

        yaml = """
node_types:
    webserver_type:
        properties:
            port: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: {} }
"""
        self.assertRaises(ValueError, self.parse, yaml)

        yaml = """
node_types:
    webserver_type:
        properties:
            port: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: [] }
"""
        self.assertRaises(ValueError, self.parse, yaml)

        yaml = """
inputs:
    port: {}
node_types:
    webserver_type:
        properties:
            port: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: port }
"""
        self.parse(yaml)

        yaml = """
inputs:
    port: {}
node_types:
    webserver_type:
        properties:
            port: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: [port] }
"""
        self.parse(yaml)

    def test_inputs_provided_to_plan(self):
        yaml = """
inputs:
    port:
        default: 9000
    port2:
        default: [{'a': [9000]}]
node_types:
    webserver_type:
        properties:
            port: {}
            port2: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: port }
            port2: { get_input: [port2, 0, 'a', 0] }
"""
        parsed = prepare_deployment_plan(self.parse(yaml),
                                         inputs={'port': 8000,
                                                 'port2': [{'a': [8000]}]})
        self.assertEqual(8000,
                         parsed['nodes'][0]['properties']['port'])
        self.assertEqual(8000,
                         parsed['nodes'][0]['properties']['port2'])

    def test_missing_input(self):
        yaml = """
inputs:
    port: {}
    name_i: {}
    name_j: {}
node_types:
    webserver_type:
        properties:
            port: {}
            name: {}
            name2: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: port }
            name: { get_input: name_i }
            name2: { get_input: [name_j, attr1, 0] }
"""
        e = self.assertRaises(
            MissingRequiredInputError,
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={'port': '8080'}
        )

        msg = str(e).split('-')[0]  # get first part of message
        self.assertTrue('name_i' in msg)
        self.assertTrue('name_j' in msg)
        self.assertFalse('port' in msg)

        e = self.assertRaises(
            MissingRequiredInputError,
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={}
        )

        msg = str(e).split('-')[0]  # get first part of message
        self.assertTrue('name_j' in msg)
        self.assertTrue('name_i' in msg)
        self.assertTrue('port' in msg)

    def test_unicode_input(self):
        yaml = """
inputs:
    port: {}
    name_i: {}
    name_j: {}
node_types:
    webserver_type:
        properties:
            port: {}
            name: {}
            name2: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: port }
            name: { get_input: name_i }
            name2: { get_input: [name_j, attr1, 0] }
"""

        u = u'M\xf6tley'

        e = self.assertRaises(
            DSLParsingInputTypeException,
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={'port': '8080', 'name_i': u}
        )
        msg = str(e).split('-')[0]  # get first part of message
        self.assertTrue('name_i' in msg)

        e = self.assertRaises(
            DSLParsingInputTypeException,
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={'port': '8080', 'name_i': 'a', 'name_j': u}
        )
        msg = str(e).split('-')[0]  # get first part of message
        self.assertTrue('name_j' in msg)

        e = self.assertRaises(
            DSLParsingInputTypeException,
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={'port': '8080', 'name_i': {'a': [{'a': [u]}]}}
        )
        msg = str(e).split('-')[0]  # get first part of message
        self.assertTrue('name_i' in msg)

        e = self.assertRaises(
            DSLParsingInputTypeException,
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={'port': '8080',
                    'name_i': 'a',
                    'name_j': {'a': [{'a': [u]}]}}
        )
        msg = str(e).split('-')[0]  # get first part of message
        self.assertTrue('name_j' in msg)

    def test_inputs_default_value(self):
        yaml = """
inputs:
    port:
        default: 8080
    port2:
        default:
            a:
                [8090]
node_types:
    webserver_type:
        properties:
            port: {}
            port2: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: port }
            port2: { get_input: [port2, a, 0] }
"""
        parsed = prepare_deployment_plan(self.parse(yaml))
        self.assertEqual(8080,
                         parsed['nodes'][0]['properties']['port'])
        self.assertEqual(8090,
                         parsed['nodes'][0]['properties']['port2'])

    def test_unknown_input_provided(self):
        yaml = """
inputs:
    port:
        default: 8080
    port2:
        default: [8090]
node_types:
    webserver_type:
        properties:
            port: {}
            port2: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: port }
            port2: { get_input: [port2, 0] }
"""

        self.assertRaisesRegex(
            UnknownInputError,
            'unknown_input_1',
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={'unknown_input_1': 'a'}
        )

        e = self.assertRaises(
            UnknownInputError,
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={'unknown_input_1': 'a', 'unknown_input_2': 'b'}
        )

        msg = str(e)
        self.assertTrue('unknown_input_1' in msg)
        self.assertTrue('unknown_input_2' in msg)

    def test_get_input_in_nested_property(self):
        yaml = """
inputs:
    port:
        default: 8080
    port2:
        default:
            a: [8090]
node_types:
    webserver_type:
        properties:
            server: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            server:
                port: { get_input: port }
                port2: { get_input: [port2, a, 0] }
"""
        parsed = prepare_deployment_plan(self.parse(yaml))
        self.assertEqual(8080,
                         parsed['nodes'][0]['properties']['server']['port'])
        self.assertEqual(8090,
                         parsed['nodes'][0]['properties']['server']['port2'])
        yaml = """
inputs:
    port:
        default: 8080
    port2:
        default:
            a: [8090]
node_types:
    webserver_type:
        properties:
            server: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            server:
                port: { get_input: port }
                port2: { get_input: [port2, a, 0] }
                some_prop: { get_input: unknown }
"""
        self.assertRaises(UnknownInputError, self.parse, yaml)
        yaml = """
inputs:
    port:
        default: 8080
    port2:
        default:
            a: [8090]
node_types:
    webserver_type:
        properties:
            server: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            server:
                port: { get_input: port }
                port2: { get_input: [unknown, a, 0] }
                some_prop: { get_input: port }
"""
        self.assertRaises(UnknownInputError, self.parse, yaml)

    def test_get_input_list_property(self):
        yaml = """
inputs:
    port:
        default: 8080
    port2:
        default:
            a: [8090]
node_types:
    webserver_type:
        properties:
            server: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            server:
                - item1
                - port: { get_input: port }
                - port2: { get_input: [port2, a, 0] }
"""
        parsed = prepare_deployment_plan(self.parse(yaml))
        self.assertEqual(8080,
                         parsed['nodes'][0]['properties']['server'][1]['port'])
        self.assertEqual(
            8090, parsed['nodes'][0]['properties']['server'][2]['port2'])
        yaml = """
inputs:
    port:
        default: 8080
    port2:
        default:
            a: [8090]
node_types:
    webserver_type:
        properties:
            server: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            server:
                - item1
                - port: { get_input: port1122 }
                - port2: { get_input: [port2, a, 0] }
"""
        self.assertRaises(UnknownInputError, self.parse, yaml)
        yaml = """
inputs:
    port:
        default: 8080
    port2:
        default:
            a: [8090]
node_types:
    webserver_type:
        properties:
            server: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            server:
                - item1
                - port: { get_input: port }
                - port2: { get_input: [unknown, a, 0] }
"""
        self.assertRaises(UnknownInputError, self.parse, yaml)

    def test_input_in_interface(self):
        yaml = """
plugins:
    plugin:
        executor: central_deployment_agent
        source: dummy
inputs:
    port:
        default: 8080
node_types:
    webserver_type: {}
relationships:
    cloudify.relationships.contained_in: {}
    rel:
        derived_from: cloudify.relationships.contained_in
        source_interfaces:
            source_interface:
                op1:
                    implementation: plugin.operation
                    inputs:
                        source_port:
                            default: { get_input: port }
        target_interfaces:
            target_interface:
                op2:
                    implementation: plugin.operation
                    inputs:
                        target_port:
                            default: { get_input: port }
node_templates:
    ws1:
        type: webserver_type
    webserver:
        type: webserver_type
        interfaces:
            lifecycle:
                configure:
                    implementation: plugin.operation
                    inputs:
                        port: { get_input: port }
        relationships:
            -   type: rel
                target: ws1
"""
        prepared = prepare_deployment_plan(self.parse(yaml))

        node_template = \
            [x for x in prepared['nodes'] if x['name'] == 'webserver'][0]
        op = node_template['operations']['lifecycle.configure']
        self.assertEqual(8080, op[consts.INPUTS]['port'])
        op = node_template['operations']['configure']
        self.assertEqual(8080, op[consts.INPUTS]['port'])
        # relationship interfaces
        source_ops = node_template['relationships'][0]['source_operations']
        self.assertEqual(
            8080,
            source_ops['source_interface.op1'][consts.INPUTS]['source_port'])
        self.assertEqual(
            8080, source_ops['op1'][consts.INPUTS]['source_port'])
        target_ops = node_template['relationships'][0]['target_operations']
        self.assertEqual(
            8080,
            target_ops['target_interface.op2'][consts.INPUTS]['target_port'])
        self.assertEqual(
            8080, target_ops['op2'][consts.INPUTS]['target_port'])

        prepared = prepare_deployment_plan(self.parse(yaml),
                                           inputs={'port': 8000})
        node_template = \
            [x for x in prepared['nodes'] if x['name'] == 'webserver'][0]
        op = node_template['operations']['lifecycle.configure']
        self.assertEqual(8000, op[consts.INPUTS]['port'])
        op = node_template['operations']['configure']
        self.assertEqual(8000, op[consts.INPUTS]['port'])
        # relationship interfaces
        source_ops = node_template['relationships'][0]['source_operations']
        self.assertEqual(
            8000,
            source_ops['source_interface.op1'][consts.INPUTS]['source_port'])
        self.assertEqual(8000, source_ops['op1'][consts.INPUTS]['source_port'])
        target_ops = node_template['relationships'][0]['target_operations']
        self.assertEqual(
            8000,
            target_ops['target_interface.op2'][consts.INPUTS]['target_port'])
        self.assertEqual(8000, target_ops['op2'][consts.INPUTS]['target_port'])

    def test_list_input_in_interface(self):
        yaml = """
plugins:
    plugin:
        executor: central_deployment_agent
        source: dummy
inputs:
    port:
        default:
            a: [8080]
node_types:
    webserver_type: {}
relationships:
    cloudify.relationships.contained_in: {}
    rel:
        derived_from: cloudify.relationships.contained_in
        source_interfaces:
            source_interface:
                op1:
                    implementation: plugin.operation
                    inputs:
                        source_port:
                            default: { get_input: [port, a, 0] }
        target_interfaces:
            target_interface:
                op2:
                    implementation: plugin.operation
                    inputs:
                        target_port:
                            default: { get_input: [port, a, 0] }
node_templates:
    ws1:
        type: webserver_type
    webserver:
        type: webserver_type
        interfaces:
            lifecycle:
                configure:
                    implementation: plugin.operation
                    inputs:
                        port: { get_input: [port, a, 0] }
        relationships:
            -   type: rel
                target: ws1
"""
        prepared = prepare_deployment_plan(self.parse(yaml))

        node_template = \
            [x for x in prepared['nodes'] if x['name'] == 'webserver'][0]
        op = node_template['operations']['lifecycle.configure']
        self.assertEqual(8080, op[consts.INPUTS]['port'])
        op = node_template['operations']['configure']
        self.assertEqual(8080, op[consts.INPUTS]['port'])
        # relationship interfaces
        source_ops = node_template['relationships'][0]['source_operations']
        self.assertEqual(
            8080,
            source_ops['source_interface.op1'][consts.INPUTS]['source_port'])
        self.assertEqual(8080, source_ops['op1'][consts.INPUTS]['source_port'])
        target_ops = node_template['relationships'][0]['target_operations']
        self.assertEqual(
            8080,
            target_ops['target_interface.op2'][consts.INPUTS]['target_port'])
        self.assertEqual(8080, target_ops['op2'][consts.INPUTS]['target_port'])

        prepared = prepare_deployment_plan(self.parse(yaml),
                                           inputs={'port': {'a': [8000]}})
        node_template = \
            [x for x in prepared['nodes'] if x['name'] == 'webserver'][0]
        op = node_template['operations']['lifecycle.configure']
        self.assertEqual(8000, op[consts.INPUTS]['port'])
        op = node_template['operations']['configure']
        self.assertEqual(8000, op[consts.INPUTS]['port'])
        # relationship interfaces
        source_ops = node_template['relationships'][0]['source_operations']
        self.assertEqual(
            8000,
            source_ops['source_interface.op1'][consts.INPUTS]['source_port'])
        self.assertEqual(8000, source_ops['op1'][consts.INPUTS]['source_port'])
        target_ops = node_template['relationships'][0]['target_operations']
        self.assertEqual(
            8000,
            target_ops['target_interface.op2'][consts.INPUTS]['target_port'])
        self.assertEqual(8000, target_ops['op2'][consts.INPUTS]['target_port'])

    def test_invalid_input_in_interfaces(self):
        yaml = """
plugins:
    plugin:
        executor: central_deployment_agent
        source: dummy
node_types:
    webserver_type: {}
node_templates:
    webserver:
        type: webserver_type
        interfaces:
            lifecycle:
                configure:
                    implementation: plugin.operation
                    inputs:
                        port: { get_input: aaa }
"""
        self.assertRaises(UnknownInputError, self.parse, yaml)
        yaml = """
plugins:
    plugin:
        executor: central_deployment_agent
        source: dummy
node_types:
    webserver_type: {}
node_templates:
    webserver:
        type: webserver_type
        interfaces:
            lifecycle:
                configure:
                    implementation: plugin.operation
                    inputs:
                        port: { get_input: [aaa, 0] }
"""
        self.assertRaises(UnknownInputError, self.parse, yaml)

    def test_input_in_outputs(self):
        yaml = """
inputs:
    port:
        default: 8080
    port2:
        default:
            a: [8090]
node_types:
    webserver_type:
        properties: {}
node_templates:
    webserver:
        type: webserver_type
outputs:
    a:
        value: { get_input: port }
    b:
        value: { get_input: [port2, a, 0] }
"""
        prepared = prepare_deployment_plan(self.parse(yaml))
        outputs = prepared.outputs
        self.assertEqual(8080, outputs['a']['value'])
        self.assertEqual(8090, outputs['b']['value'])

    def test_missing_input_exception(self):
        yaml = """
node_types:
  type:
    interfaces:
      interface:
        op:
          implementation: plugin.mapping
          inputs:
            some_input:
              type: string
node_templates:
  node:
    type: type
plugins:
  plugin:
    install: false
    executor: central_deployment_agent
"""
        ex = self._assert_dsl_parsing_exception_error_code(
            yaml, ERROR_MISSING_PROPERTY, DSLParsingLogicException)
        self.assertIn('some_input', ex.message)

    def test_missing_inputs_both_reported(self):
        yaml = """
node_types:
  type:
    interfaces:
      interface:
        op:
          implementation: plugin.mapping
          inputs:
            some_input:
              type: string
            another_input:
              type: string
node_templates:
  node:
    type: type
plugins:
  plugin:
    install: false
    executor: central_deployment_agent
"""
        ex = self._assert_dsl_parsing_exception_error_code(
            yaml, ERROR_MISSING_PROPERTY, DSLParsingLogicException)
        self.assertIn('some_input', ex.message)
        self.assertIn('another_input', ex.message)

    def test_invalid_index_obj_type(self):
        yaml = """
inputs:
    port: {}
node_types:
    webserver_type:
        properties:
            port: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: [port, a] }
"""
        self.assertRaises(
            InputEvaluationError,
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={'port': [8000]})

    def test_index_out_of_bounds(self):
        yaml = """
inputs:
    port: {}
node_types:
    webserver_type:
        properties:
            port: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: [port, 1] }
"""
        self.assertRaises(
            InputEvaluationError,
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={'port': [8000]})

    def test_input_missing_attr_raises_keyerror(self):
        yaml = """
inputs:
    port: {}
node_types:
    webserver_type:
        properties:
            port: {}
node_templates:
    webserver:
        type: webserver_type
        properties:
            port: { get_input: [port, b] }
"""
        self.assertRaises(
            InputEvaluationError,
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={'port': {'a': 2}})


class TestInputsConstraints(AbstractTestParser):
    def test_constraints_successful(self):
        yaml = """
inputs:
    some_input:
        default: hi
        constraints:
            - max_length: 2
            - min_length: 2
            - valid_values: [ 'hi', 'ab' ]
            - length: 2
            - pattern: '(hi)|(ab)'
"""
        parsed = self.parse(yaml)
        self.assertEqual(1, len(parsed[consts.INPUTS]))
        self.assertEqual('hi',
                         parsed[consts.INPUTS]['some_input'][consts.DEFAULT])
        plan = prepare_deployment_plan(parsed, inputs={'some_input': 'ab'})
        self.assertEqual(1, len(plan[consts.INPUTS]))
        self.assertEqual('ab', plan[consts.INPUTS]['some_input'])
        plan = prepare_deployment_plan(parsed, inputs={'some_input': u'ab'})
        self.assertEqual(1, len(plan[consts.INPUTS]))
        self.assertEqual('ab', plan[consts.INPUTS]['some_input'])

    def test_constraints_not_successful_with_default(self):
        yaml = """
inputs:
    some_input:
        default: hi
        constraints:
            - max_length: 1
            - min_length: 2
            - valid_values: [ 'hi' ]
            - length: 2
            - pattern: 'hi'
"""

        self.assertRaises(ConstraintException, self.parse, yaml)

    def test_constraints_not_successful_with_inserted_input(self):
        yaml = """
inputs:
    some_input:
        constraints:
            - max_length: 1
            - min_length: 2
            - valid_values: [ 'hi' ]
            - length: 2
            - pattern: 'hi'
"""

        self.assertRaises(ConstraintException, prepare_deployment_plan,
                          self.parse(yaml), inputs={'some_input': 'hi'})

    def test_input_default_value_has_no_func_and_constraints(self):
        yaml = """
inputs:
    some_input:
        default: {get_input: i}
        constraints:
            - max_length: 1
"""
        self.assertRaises(
            DSLParsingException,
            self.parse,
            yaml)

    def test_input_value_has_no_func_and_constraints(self):
        yaml = """
inputs:
    some_input:
        constraints:
            - max_length: 1
"""
        self.assertRaises(
            DSLParsingException,
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={'some_input': {'get_input': 'shouldnt matter'}})

    def test_constraints_successful_with_import(self):
        yaml = """
inputs:
    some_input:
        default: hi
        constraints:
            - max_length: 2
            - min_length: 2
            - valid_values: [ 'hi', 'ab' ]
            - length: 2
            - pattern: '(hi)|(ab)'
"""
        import_file_name = self.make_yaml_file(yaml)
        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}
""".format(import_file_name)
        parsed = self.parse(main_yaml)
        inputs = parsed[consts.INPUTS]
        self.assertEqual(5, len(inputs['some_input'][consts.CONSTRAINTS]))


class TestInputsTypeValidation(AbstractTestParser):
    def test_input_default_value_data_type_validation_successful(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
inputs:
    some_input:
        type: a
        default:
            b:
                c:
                    d: 123
data_types:
    a:
        properties:
            b:
                type: b
    b:
        properties:
            c:
                type: c
    c:
        properties:
            d:
                type: integer

"""
        parsed = self.parse_1_2(yaml)
        self.assertEqual(
            parsed[consts.INPUTS]['some_input'][consts.TYPE], 'a')
        self.assertDictEqual(
            parsed[consts.INPUTS]['some_input'][consts.DEFAULT],
            {'b': {'c': {'d': 123}}})

    def test_input_value_data_type_validation_successful(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
inputs:
    some_input:
        type: a
data_types:
    a:
        properties:
            b:
                type: b
    b:
        properties:
            c:
                type: c
    c:
        properties:
            d:
                type: integer

"""
        prepare_deployment_plan(
            self.parse_1_2(yaml),
            inputs={'some_input': {'b': {'c': {'d': 123}}}})

    def test_input_default_value_data_type_validation_raises(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
inputs:
    some_input:
        type: a
        default:
            b:
                c:
                    d: should_be_int
data_types:
    a:
        properties:
            b:
                type: b
    b:
        properties:
            c:
                type: c
    c:
        properties:
            d:
                type: integer

"""
        ex = self._assert_dsl_parsing_exception_error_code(
            yaml,
            ERROR_VALUE_DOES_NOT_MATCH_TYPE)
        self.assertIn('b.c.d', ex.message)

    def test_input_value_data_type_validation_raises(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
inputs:
    some_input:
        type: a
data_types:
    a:
        properties:
            b:
                type: b
    b:
        properties:
            c:
                type: c
    c:
        properties:
            d:
                type: integer

"""
        e = self.assertRaises(
            DSLParsingException,
            prepare_deployment_plan,
            self.parse_1_2(yaml),
            inputs={'some_input': {'b': {'c': {'d': 'should_be_int'}}}})
        self.assertEqual(ERROR_INPUT_VIOLATES_DATA_TYPE_SCHEMA, e.err_code)
        self.assertIn('b.c.d', e.message)

    def test_input_default_value_data_type_validation_raises_undefined(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
inputs:
    some_input:
        type: a
        default:
            e:
                c:
                    d: should_be_int
data_types:
    a:
        properties:
            b:
                type: b
    b:
        properties:
            c:
                type: c
    c:
        properties:
            d:
                type: integer

"""
        ex = self._assert_dsl_parsing_exception_error_code(
            yaml,
            ERROR_UNDEFINED_PROPERTY)
        self.assertIn('Undefined property e', ex.message)

    def test_input_value_data_type_validation_raises_undefined(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
inputs:
    some_input:
        type: a
data_types:
    a:
        properties:
            b:
                type: b
    b:
        properties:
            c:
                type: c
    c:
        properties:
            d:
                type: integer

"""
        e = self.assertRaises(
            DSLParsingException,
            prepare_deployment_plan,
            self.parse_1_2(yaml),
            inputs={'some_input': {'e': {'c': {'d': 'should_be_int'}}}})
        self.assertEqual(ERROR_INPUT_VIOLATES_DATA_TYPE_SCHEMA, e.err_code)
        self.assertIn('Undefined property e', e.message)

    def test_input_value_data_type_validation_raises_with_derived(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
inputs:
    some_input:
        type: b
data_types:
    a:
        properties:
            b:
                type: integer
    b:
        derived_from: a
        properties:
            c:
                type: integer
"""
        e = self.assertRaises(
            DSLParsingException,
            prepare_deployment_plan,
            self.parse_1_2(yaml),
            inputs={'some_input': {'c': 123}})
        self.assertEqual(ERROR_INPUT_VIOLATES_DATA_TYPE_SCHEMA, e.err_code)
        self.assertIn('is missing property b', e.message)

    def test_input_default_value_type_validation_raises_with_derived(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
inputs:
    some_input:
        type: b
        default:
            c: 123
data_types:
    a:
        properties:
            b:
                type: integer
    b:
        derived_from: a
        properties:
            c:
                type: integer
"""
        ex = self._assert_dsl_parsing_exception_error_code(
            yaml,
            ERROR_MISSING_PROPERTY)
        self.assertIn('is missing property b', ex.message)

    def test_input_validate_non_existing_type(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
inputs:
    some_input:
        type: who_dis?
"""
        ex = self._assert_dsl_parsing_exception_error_code(
            yaml,
            ERROR_UNKNOWN_TYPE)
        self.assertIn('Illegal type name', ex.message)

    def test_validate_regex_value_successful(self):
        self._test_validate_value_successful(
            'regex', '^$', '^.$', self.assertEqual)

    def test_validate_regex_value_mismatch(self):
        self._test_validate_value_type_mismatch('regex', '\\')

    def test_validate_regex_value_mismatch_with_deployment_plan(self):
        self._test_validate_value_type_mismatch_with_deployment_plan('regex',
                                                                     '\\')

    def test_validate_list_value_successful(self):
        self._test_validate_value_successful(
            'list', ['one', 'two', 3], ['', 'three', 1], self.assertListEqual)

    def test_validate_list_value_mismatch(self):
        self._test_validate_value_type_mismatch('list', set(['0_o']))

    def test_validate_list_value_mismatch_with_deployment_plan(self):
        self._test_validate_value_type_mismatch_with_deployment_plan('list',
                                                                     '123')

    def test_validate_dict_value_successful(self):
        self._test_validate_value_successful(
            'dict', {'k': 'v'}, {'k1': 'v1'}, self.assertDictEqual)

    def test_validate_dict_value_mismatch(self):
        self._test_validate_value_type_mismatch('dict', [])

    def test_validate_dict_value_mismatch_with_deployment_plan(self):
        self._test_validate_value_type_mismatch_with_deployment_plan('dict',
                                                                     'x')

    def test_validate_integer_value_successful(self):
        self._test_validate_value_successful(
            'integer', 123, 456, self.assertEqual)

    def test_validate_integer_value_mismatch(self):
        self._test_validate_value_type_mismatch('integer', [])
        self._test_validate_value_type_mismatch('integer', True)

    def test_validate_integer_value_mismatch_with_deployment_plan(self):
        self._test_validate_value_type_mismatch_with_deployment_plan('integer',
                                                                     'x')
        self._test_validate_value_type_mismatch_with_deployment_plan('integer',
                                                                     True)

    def test_validate_float_value_successful(self):
        self._test_validate_value_successful(
            'float', 123, 456, self.assertEqual)
        self._test_validate_value_successful(
            'float', 123.1, 456.1, self.assertEqual)

    def test_validate_float_value_mismatch(self):
        self._test_validate_value_type_mismatch('float', [])
        self._test_validate_value_type_mismatch('float', True)

    def test_validate_float_value_mismatch_with_deployment_plan(self):
        self._test_validate_value_type_mismatch_with_deployment_plan('float',
                                                                     'x')
        self._test_validate_value_type_mismatch_with_deployment_plan('float',
                                                                     True)

    def test_validate_boolean_value_successful(self):
        self._test_validate_value_successful(
            'boolean', False, True, self.assertEqual)

    def test_validate_boolean_value_mismatch(self):
        self._test_validate_value_type_mismatch('boolean', [])

    def test_validate_boolean_value_mismatch_with_deployment_plan(self):
        self._test_validate_value_type_mismatch_with_deployment_plan('boolean',
                                                                     'x')

    def test_validate_string_value_successful(self):
        self._test_validate_value_successful(
            'string', 'some_string', 'some_other_string', self.assertEqual)
        # If the type is string, it should accept anything
        self._test_validate_value_successful(
            'string', list(), [''], self.assertListEqual)

    def _test_validate_value_successful(self,
                                        type_name,
                                        default_value,
                                        input_value,
                                        value_assert_equal_func):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
inputs:
    some_input:
        type: {0}
        default: {1}
""".format(type_name, default_value)
        parsed = self.parse(yaml)
        self.assertEqual(
            parsed[consts.INPUTS]['some_input'][consts.TYPE], type_name)
        value_assert_equal_func(
            parsed[consts.INPUTS]['some_input'][consts.DEFAULT], default_value)
        plan = prepare_deployment_plan(
            parsed, inputs={'some_input': input_value})
        value_assert_equal_func(plan['inputs']['some_input'], input_value)
        if not isinstance(input_value, text_type):
            return
        # Testing Unicode case
        unicode_input_value = unicode(input_value, encoding='utf-8')
        plan = prepare_deployment_plan(
            parsed, inputs={'some_input': unicode_input_value})
        value_assert_equal_func(
            plan['inputs']['some_input'], unicode_input_value)

    def _test_validate_value_type_mismatch(self, type_name, default_value):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
inputs:
    some_input:
        type: {0}
        default: {1}
""".format(type_name, default_value)
        ex = self._assert_dsl_parsing_exception_error_code(
            yaml,
            ERROR_VALUE_DOES_NOT_MATCH_TYPE)
        self.assertIn('Property type validation failed in', ex.message)
        self.assertIn("type is '{0}'".format(type_name), ex.message)

    def _test_validate_value_type_mismatch_with_deployment_plan(
            self, type_name, value):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_2 + """
inputs:
    some_input:
        type: {0}
""".format(type_name)
        ex = self.assertRaises(
            DSLParsingException,
            prepare_deployment_plan,
            self.parse(yaml),
            inputs={'some_input': value})
        self.assertIn('Property type validation failed in', ex.message)
        self.assertIn("type is '{0}'".format(type_name), ex.message)
