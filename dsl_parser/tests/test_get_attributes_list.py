import testtools.testcase

from dsl_parser import functions
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestGetAttributesList(AbstractTestParser):

    def test_has_intrinsic_functions_property(self):
        yaml = """
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
                        a: { get_attributes_list: [SELF, a] }
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
                                a: { get_attributes_list: [SOURCE, a] }
                target_interfaces:
                    test:
                        op_with_no_get_attribute:
                            implementation: p.p
                            inputs:
                                a: 1
                        op_with_get_attribute:
                            implementation: p.p
                            inputs:
                                a: { get_attributes_list: [TARGET, a] }
"""
        parsed = prepare_deployment_plan(self.parse(yaml))
        webserver_node = None
        for node in parsed.node_templates:
            if node['id'] == 'webserver':
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

    def test_get_attributes_list(self):
        node_instances = [
            {
                'id': 'goodnode_r1ck00',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'never'},
            },
            {
                'id': 'badnode_0hn011',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
            {
                'id': 'goodnode_45713y',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'gonna'},
            },
            {
                'id': 'goodnode_r0113d',
                'node_id': 'goodnode',
            },
            {
                'id': 'goodnode_s1n613',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'you'},
            },
            {
                'id': 'goodnode_41b2m5',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'up'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode', 'properties': {'sing': 'give'}},
        ]
        storage = self._mock_evaluation_storage(
            node_instances=node_instances, nodes=nodes)

        payload = {'a': {'get_attributes_list': ['goodnode', 'sing']}}
        context = {}
        result = functions.evaluate_functions(payload.copy(), context, storage)

        attributes_list = result['a']
        # We expect this to be returned in the same order the node instances
        # were retrieved from the storage manager
        assert attributes_list == ['never', 'gonna', 'give', 'you', 'up']

    def test_get_attributes_list_missing_value(self):
        node_instances = [
            {
                'id': 'badnode_0hn011',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
            {
                'id': 'goodnode_45713y',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'gonna'},
            },
            {
                'id': 'goodnode_r0113d',
                'node_id': 'goodnode',
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)
        payload = {'a': {'get_attributes_list': ['goodnode', 'sing']}}
        result = functions.evaluate_functions(payload, {}, storage)

        attributes_list = result['a']
        assert attributes_list == ['gonna']

    def test_get_attributes_list_no_values(self):
        node_instances = [
            {
                'id': 'badnode_0hn011',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
            {
                'id': 'goodnode_45713y',
                'node_id': 'goodnode',
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)
        payload = {'a': {'get_attributes_list': ['goodnode', 'sing']}}
        result = functions.evaluate_functions(payload, {}, storage)

        attributes_list = result['a']
        assert attributes_list == []

    def test_get_attributes_list_one_value(self):
        node_instances = [
            {
                'id': 'badnode_0hn011',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
            {
                'id': 'goodnode_45713y',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'gonna'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)
        payload = {'a': {'get_attributes_list': ['goodnode', 'sing']}}
        result = functions.evaluate_functions(payload, {}, storage)

        attributes_list = result['a']
        assert attributes_list == ['gonna']

    def test_get_attributes_list_no_node_instances(self):
        node_instances = [
            {
                'id': 'badnode_0hn011',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)
        payload = {'a': {'get_attributes_list': ['goodnode', 'sing']}}
        result = functions.evaluate_functions(payload, {}, storage)

        attributes_list = result['a']
        assert attributes_list == []

    def test_get_attributes_list_no_node(self):
        node_instances = [
            {
                'id': 'badnode_0hn011',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)
        payload = {'a': {'get_attributes_list': ['goodnode', 'sing']}}
        with testtools.testcase.ExpectedException(
                KeyError,
                '.*Node not found.*'):
            functions.evaluate_functions(payload, {}, storage)

    def test_get_attributes_list_node_instance_ids(self):
        node_instances = [
            {
                'id': 'badnode_0hn011',
                'node_id': 'badnode',
            },
            {
                'id': 'goodnode_45713y',
                'node_id': 'goodnode',
            },
            {
                'id': 'goodnode_r0113d',
                'node_id': 'goodnode',
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)
        payload = {
            'a': {'get_attributes_list': ['goodnode', 'node_instance_id']}}
        result = functions.evaluate_functions(payload, {}, storage)

        attributes_list = result['a']
        assert attributes_list == ['goodnode_45713y', 'goodnode_r0113d']

    def test_get_attributes_list_relationship(self):
        node_instances = [
            {
                'id': 'acre_4m4d34',
                'node_id': 'acre',
                'runtime_properties': {},
            },
            {
                'id': 'acre_8r00k5',
                'node_id': 'acre',
                'runtime_properties': {'thing': 'hello'},
            },
            {
                'id': 'mandel_aa50f7',
                'node_id': 'mandel',
                'runtime_properties': {'thing': 'wave'},
            },
            {
                'id': 'mandel_c311aa',
                'node_id': 'mandel',
                'runtime_properties': {},
            },
        ]
        nodes = [
            {
                'id': 'acre',
                'properties': {'thing': 'say'},
            },
            {
                'id': 'mandel',
                'properties': {'thing': 'goodbye'},
            },
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)

        payload = {
            'src': {'get_attributes_list': ['SOURCE', 'thing']},
            'trg': {'get_attributes_list': ['TARGET', 'thing']},
        }

        context = {
            'source': 'acre',
            'target': 'mandel',
        }

        result = functions.evaluate_functions(payload, context, storage)

        assert result['src'] == ['say', 'hello']
        assert result['trg'] == ['wave', 'goodbye']

    def test_get_attributes_list_self(self):
        node_instances = [
            {
                'id': 'badnode_51573r',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
            {
                'id': 'goodnode_aa0faa',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'this'},
            },
            {
                'id': 'goodnode_m3rcya',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'corrosion'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)

        payload = {'a': {'get_attributes_list': ['SELF', 'sing']}}
        context = {'self': 'goodnode'}
        result = functions.evaluate_functions(payload, context, storage)

        assert result['a'] == ['this', 'corrosion']

    def test_get_attributes_list_concat(self):
        node_instances = [
            {
                'id': 'badnode_51573r',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
            {
                'id': 'goodnode_aa0faa',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'this'},
            },
            {
                'id': 'goodnode_m3rcya',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'corrosion'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)

        payload = {'a': {'concat': {'get_attributes_list': ['SELF', 'sing']}}}
        context = {'self': 'goodnode'}
        result = functions.evaluate_functions(payload, context, storage)
        assert result['a'] == 'thiscorrosion'

    def test_get_attributes_list_nested(self):
        node_instances = [
            {
                'id': 'badnode_w08813',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
            {
                'id': 'goodnode_d3p3ch',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': {'song': 'faith'}},
            },
            {
                'id': 'goodnode_m0d3aa',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': {'song': 'devotion'}},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)

        payload = {'a': {'get_attributes_list': ['goodnode', 'sing', 'song']}}
        result = functions.evaluate_functions(payload, {}, storage)
        assert result['a'] == ['faith', 'devotion']
