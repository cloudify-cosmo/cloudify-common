import testtools.testcase

from dsl_parser import exceptions
from dsl_parser import functions
from dsl_parser.tasks import prepare_deployment_plan
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestGetAttributesDict(AbstractTestParser):

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
                        a: { get_attributes_dict: [SELF, a] }
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
                                a: { get_attributes_dict: [SOURCE, a] }
                target_interfaces:
                    test:
                        op_with_no_get_attribute:
                            implementation: p.p
                            inputs:
                                a: 1
                        op_with_get_attribute:
                            implementation: p.p
                            inputs:
                                a: { get_attributes_dict: [TARGET, a] }
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

    def test_get_attributes_dict_one_attribute(self):
        node_instances = [
            {
                'id': 'goodnode_r1ck00',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'never',
                                       'eat': 'drømekage'},
            },
            {
                'id': 'badnode_0hn011',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget',
                                       'eat': 'apple'},
            },
            {
                'id': 'goodnode_45713y',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'gonna',
                                       'eat': 'chip'},
            },
            {
                'id': 'goodnode_r0113d',
                'node_id': 'goodnode',
                'runtime_properties': {'eat': 'biscuit'},
            },
            {
                'id': 'goodnode_s1n613',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'you'},
            },
            {
                'id': 'goodnode_41b2m5',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'up',
                                       'eat': 'cheesecake'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode', 'properties': {'sing': 'give',
                                              'eat': 'marshmallow'}},
        ]
        storage = self._mock_evaluation_storage(
            node_instances=node_instances, nodes=nodes)

        payload = {'a': {'get_attributes_dict': ['goodnode', 'sing']}}
        context = {}
        result = functions.evaluate_functions(payload.copy(), context, storage)

        attributes_list = result['a']
        assert attributes_list == {
            'goodnode_r1ck00': {'sing': 'never'},
            'goodnode_45713y': {'sing': 'gonna'},
            'goodnode_r0113d': {'sing': 'give'},
            'goodnode_s1n613': {'sing': 'you'},
            'goodnode_41b2m5': {'sing': 'up'},
        }

    def test_get_attributes_dict_multi_attributes(self):
        node_instances = [
            {
                'id': 'goodnode_r1ck00',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'never',
                                       'eat': 'drømekage'},
            },
            {
                'id': 'badnode_0hn011',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget',
                                       'eat': 'apple'},
            },
            {
                'id': 'goodnode_45713y',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'gonna',
                                       'eat': 'chip'},
            },
            {
                'id': 'goodnode_r0113d',
                'node_id': 'goodnode',
                'runtime_properties': {'eat': 'biscuit'},
            },
            {
                'id': 'goodnode_s1n613',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'you'},
            },
            {
                'id': 'goodnode_41b2m5',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': 'up',
                                       'eat': 'cheesecake'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode', 'properties': {'sing': 'give',
                                              'eat': 'marshmallow'}},
        ]
        storage = self._mock_evaluation_storage(
            node_instances=node_instances, nodes=nodes)

        payload = {'a': {'get_attributes_dict': ['goodnode', 'sing', 'eat']}}
        context = {}
        result = functions.evaluate_functions(payload.copy(), context, storage)

        attributes_list = result['a']
        assert attributes_list == {
            'goodnode_r1ck00': {'sing': 'never',
                                'eat': 'drømekage'},
            'goodnode_45713y': {'sing': 'gonna',
                                'eat': 'chip'},
            'goodnode_r0113d': {'sing': 'give',
                                'eat': 'biscuit'},
            'goodnode_s1n613': {'sing': 'you',
                                'eat': 'marshmallow'},
            'goodnode_41b2m5': {'sing': 'up',
                                'eat': 'cheesecake'},
        }

    def test_get_attributes_dict_missing_value(self):
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
                'runtime_properties': {'eat': 'æbleskive'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)
        payload = {'a': {'get_attributes_dict': ['goodnode', 'sing', 'eat']}}
        result = functions.evaluate_functions(payload, {}, storage)

        attributes_list = result['a']
        assert attributes_list == {
            'goodnode_45713y': {'sing': 'gonna',
                                'eat': None},
            'goodnode_r0113d': {'sing': None,
                                'eat': 'æbleskive'},
        }

    def test_get_attributes_dict_no_values(self):
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
        payload = {'a': {'get_attributes_dict': ['goodnode', 'sing', 'eat']}}
        result = functions.evaluate_functions(payload, {}, storage)

        attributes_list = result['a']
        assert attributes_list == {
            'goodnode_45713y': {'sing': None, 'eat': None},
        }

    def test_get_attributes_dict_no_node_instances(self):
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
        payload = {'a': {'get_attributes_dict': ['goodnode', 'sing', 'eat']}}
        result = functions.evaluate_functions(payload, {}, storage)

        attributes_list = result['a']
        assert attributes_list == {}

    def test_get_attributes_dict_no_node(self):
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
        payload = {'a': {'get_attributes_dict': ['goodnode', 'sing', 'eat']}}
        with testtools.testcase.ExpectedException(
                KeyError,
                '.*Node not found.*'):
            functions.evaluate_functions(payload, {}, storage)

    def test_get_attributes_dict_relationship(self):
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
            'src': {'get_attributes_dict': ['SOURCE', 'thing']},
            'trg': {'get_attributes_dict': ['TARGET', 'thing']},
        }

        context = {
            'source': 'acre',
            'target': 'mandel',
        }

        result = functions.evaluate_functions(payload, context, storage)

        assert result['src'] == {
            'acre_4m4d34': {'thing': 'say'},
            'acre_8r00k5': {'thing': 'hello'},
        }
        assert result['trg'] == {
            'mandel_aa50f7': {'thing': 'wave'},
            'mandel_c311aa': {'thing': 'goodbye'},
        }

    def test_get_attributes_dict_self(self):
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

        payload = {'a': {'get_attributes_dict': ['SELF', 'sing']}}
        context = {'self': 'goodnode'}
        result = functions.evaluate_functions(payload, context, storage)

        assert result['a'] == {
            'goodnode_aa0faa': {'sing': 'this'},
            'goodnode_m3rcya': {'sing': 'corrosion'},
        }

    def test_get_attributes_dict_nested(self):
        node_instances = [
            {
                'id': 'badnode_w08813',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
            {
                'id': 'goodnode_d3p3ch',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': {'song': 'faith'},
                                       'eat': 'rødgrød'},
            },
            {
                'id': 'goodnode_m0d3aa',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': {'song': 'devotion'},
                                       'eat': 'fløde'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)

        payload = {'a': {'get_attributes_dict': ['goodnode',
                                                 ['sing', 'song'], 'eat']}}
        result = functions.evaluate_functions(payload, {}, storage)
        assert result['a'] == {
            'goodnode_d3p3ch': {'sing.song': 'faith', 'eat': 'rødgrød'},
            'goodnode_m0d3aa': {'sing.song': 'devotion', 'eat': 'fløde'},
        }

    def test_get_attributess_dict_nested_ambiguous_not_requested(self):
        node_instances = [
            {
                'id': 'badnode_w08813',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
            {
                'id': 'goodnode_d3p3ch',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': {'song': 'faith'},
                                       'sing.song': 'rødgrød'},
            },
            {
                'id': 'goodnode_m0d3aa',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': {'song': 'devotion'},
                                       'sing.song': 'fløde'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)

        payload = {'a': {'get_attributes_dict': ['goodnode',
                                                 ['sing', 'song']]}}
        result = functions.evaluate_functions(payload, {}, storage)
        assert result['a'] == {
            'goodnode_d3p3ch': {'sing.song': 'faith'},
            'goodnode_m0d3aa': {'sing.song': 'devotion'},
        }

    def test_get_attributess_dict_nested_ambiguous_requested(self):
        node_instances = [
            {
                'id': 'badnode_w08813',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
            {
                'id': 'goodnode_d3p3ch',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': {'song': 'faith'},
                                       'sing.song': 'rødgrød'},
            },
            {
                'id': 'goodnode_m0d3aa',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': {'song': 'devotion'},
                                       'sing.song': 'fløde'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)

        payload = {'a': {'get_attributes_dict': ['goodnode',
                                                 ['sing', 'song'],
                                                 'sing.song']}}
        with testtools.testcase.ExpectedException(
                exceptions.FunctionEvaluationError,
                '.*ambiguous.*'):
            functions.evaluate_functions(payload, {}, storage)

    def test_get_attributess_dict_nested_duplicate_requested(self):
        node_instances = [
            {
                'id': 'badnode_w08813',
                'node_id': 'badnode',
                'runtime_properties': {'sing': 'forget'},
            },
            {
                'id': 'goodnode_d3p3ch',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': {'song': 'faith'},
                                       'sing.song': 'rødgrød'},
            },
            {
                'id': 'goodnode_m0d3aa',
                'node_id': 'goodnode',
                'runtime_properties': {'sing': {'song': 'devotion'},
                                       'sing.song': 'fløde'},
            },
        ]
        nodes = [
            {'id': 'badnode'},
            {'id': 'goodnode'},
        ]
        storage = self._mock_evaluation_storage(node_instances, nodes)

        payload = {'a': {'get_attributes_dict': ['goodnode', 'sing', 'sing']}}
        with testtools.testcase.ExpectedException(
                exceptions.FunctionEvaluationError,
                '.*duplicated.*'):
            functions.evaluate_functions(payload, {}, storage)
