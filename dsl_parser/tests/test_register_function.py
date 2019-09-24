########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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

from dsl_parser import functions
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestFunctionRegistration(AbstractTestParser):

    def setUp(self):
        super(TestFunctionRegistration, self).setUp()
        self.addCleanup(self.cleanup)

    def cleanup(self):
        functions.unregister('to_upper')

    def test_registration(self):
        @functions.register(name='to_upper',
                            func_eval_type=functions.HYBRID_FUNC)
        class ToUpper(functions.Function):

            def __init__(self, args, **kwargs):
                self.arg = None
                super(ToUpper, self).__init__(args, **kwargs)

            def parse_args(self, args):
                self.arg = args

            def evaluate(self, handler):
                if functions.parse(self.arg) != self.arg:
                    return self.raw
                return str(self.arg).upper()

            def validate(self, plan):
                pass

        yaml = """
node_types:
    webserver_type:
        properties:
            property:
                default: property_value
node_templates:
    webserver:
        type: webserver_type
outputs:
    output1:
        value: { to_upper: first }
    output2:
        value: { to_upper: { get_property: [webserver, property] } }
    output3:
        value: { to_upper: { get_attribute: [webserver, attribute] } }
    output4:
        value: { to_upper: { get_secret: secret } }
    output5:
        value: { to_upper: { get_capability: [ dep_1, cap_a ] } }
"""
        node_instances = [{
            'id': 'webserver1',
            'node_id': 'webserver',
            'runtime_properties': {
                'attribute': 'attribute_value'
            }
        }]
        nodes = [{'id': 'webserver'}]
        storage = self._mock_evaluation_storage(node_instances, nodes)

        parsed = prepare_deployment_plan(self.parse(yaml), storage.get_secret)

        outputs = parsed['outputs']
        self.assertEqual('FIRST', outputs['output1']['value'])
        self.assertEqual('PROPERTY_VALUE', outputs['output2']['value'])
        self.assertEqual({'to_upper': {'get_attribute': ['webserver',
                                                         'attribute']}},
                         outputs['output3']['value'])

        outputs = functions.evaluate_outputs(parsed['outputs'], storage)
        self.assertEqual('FIRST', outputs['output1'])
        self.assertEqual('PROPERTY_VALUE', outputs['output2'])
        self.assertEqual('ATTRIBUTE_VALUE', outputs['output3'])
        self.assertEqual('SECRET_VALUE', outputs['output4'])
        self.assertEqual('VALUE_A_1', outputs['output5'])


class NodeInstance(dict):
    @property
    def id(self):
        return self.get('id')

    @property
    def node_id(self):
        return self.get('node_id')

    @property
    def runtime_properties(self):
        return self.get('runtime_properties')


class Node(dict):
    @property
    def id(self):
        return self.get('id')

    @property
    def properties(self):
        return self.get('properties', {})
