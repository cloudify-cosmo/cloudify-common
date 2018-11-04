from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestNamespaceImport(AbstractTestParser):

    def test_node_type_import(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value"""
        bottom_file_name = self.make_yaml_file(yaml)

        top_level_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_templates:
    test_node:
        type: test::test_type
imports:
    -   {0}::{1}
""".format('test', bottom_file_name)

        parsed = self.parse(top_level_yaml)
        vm = parsed['nodes'][0]
        self.assertEqual(
            'value',
            vm['properties']['prop1'])

    def test_node_type_collision_import(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value"""
        bottom_file_name = self.make_yaml_file(yaml)

        top_level_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value2
node_templates:
    test_node1:
        type: test::test_type
    test_node2:
        type: test_type
imports:
    -   {0}::{1}
""".format('test', bottom_file_name)

        result = self.parse(top_level_yaml)

    def test_merging_node_type_import(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type:
    properties:
      prop1:
        default: value"""
        bottom_file_name = self.make_yaml_file(yaml)

        top_level_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
node_types:
  test_type2:
    properties:
      prop1:
        default: value

node_templates:
    test_node:
        type: test::test_type
    test_node2:
        type: test_type2
imports:
    -   {0}::{1}
""".format('test', bottom_file_name)

        result = self.parse(top_level_yaml)


class TestNamespacedDataTypes(AbstractTestParser):
    def test_namespaced_date_types(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
data_types:
    test_type:
        properties:
            test: {}
    pair_of_pairs_type:
        properties:
            test_prop:
                type: test_type
"""
        bottom_file_name = self.make_yaml_file(yaml)

        top_level_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + self.MINIMAL_BLUEPRINT + """
imports:
    -   {0}::{1}
""".format('test', bottom_file_name)

        self.parse(top_level_yaml)

    def test_nested_defaults(self):
        pass

    def test_collision(self):
        file1 = """
data_types:
    data1:
        properties:
            prop1:
                default: value1
"""
        import_path = self.make_yaml_file(file1)
        yaml = """
imports:
  - {0}::{1}
data_types:
    data1:
        properties:
            prop1:
                default: value2
node_types:
    type:
        properties:
            prop1:
                type: test::data1
            prop2:
                type: data1
node_templates:
    node:
        type: type
""".format('test', import_path)
        properties = self.parse_1_3(yaml)['nodes'][0]['properties']
        self.assertEqual(properties['prop1']['prop1'], 'value1')
        self.assertEqual(properties['prop2']['prop1'], 'value2')

    def test_two_lvl_collision(self):
        layer1 = """
data_types:
    data1:
        properties:
            prop1:
                default: value1
"""
        layer1_import_path = self.make_yaml_file(layer1)
        layer2 = """
imports:
  - {0}::{1}
data_types:
    data1:
        properties:
            prop1:
                default: value2
""".format('test1', layer1_import_path)
        layer2_import_path = self.make_yaml_file(layer2)
        yaml = """
imports:
  - {0}::{1}
data_types:
    data1:
        properties:
            prop1:
                default: value3
node_types:
    type:
        properties:
            prop1:
                type: test::data1
            prop2:
                type: data1
            prop3:
                type: test::test1::data1
node_templates:
    node:
        type: type
""".format('test', layer2_import_path)
        properties = self.parse_1_3(yaml)['nodes'][0]['properties']
        self.assertEqual(properties['prop1']['prop1'], 'value2')
        self.assertEqual(properties['prop2']['prop1'], 'value3')
        self.assertEqual(properties['prop3']['prop1'], 'value1')

    def test_imports_merging(self):
        file1 = """
data_types:
    data1:
        properties:
            prop1:
                default: value1
"""
        import_path = self.make_yaml_file(file1)
        yaml = """
imports:
  - {0}::{1}
data_types:
    data2:
        properties:
            prop2:
                default: value2
node_types:
    type:
        properties:
            prop1:
                type: test::data1
            prop2:
                type: data2
node_templates:
    node:
        type: type
""".format('test', import_path)
        properties = self.parse_1_3(yaml)['nodes'][0]['properties']
        self.assertEqual(properties['prop1']['prop1'], 'value1')
        self.assertEqual(properties['prop2']['prop2'], 'value2')

    def test_nested_merge_with_inheritance(self):
        pass

    def test_complex_nested_merging(self):
        pass

    def test_subtype_override_field_type(self):
        pass

    def test_derives(self):
        yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
data_types:
    agent_connection:
        properties:
            username:
                type: string
                default: ubuntu
            key:
                type: string
                default: ~/.ssh/id_rsa
    agent_installer:
        properties:
            connection:
                type: agent_connection
                default: {}
"""
        import_path = self.make_yaml_file(yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
  - {0}::{1}
node_types:
    vm_type:
        properties:
            agent:
                type: agent
node_templates:
    vm:
        type: vm_type
        properties:
            agent:
                connection:
                    key: /home/ubuntu/id_rsa
data_types:
    agent:
        derived_from: test::agent_installer
        properties:
            basedir:
                type: string
                default: /home/
""".format('test', import_path)

        parsed = self.parse(main_yaml)
        vm = parsed['nodes'][0]
        self.assertEqual(
            'ubuntu',
            vm['properties']['agent']['connection']['username'])
        self.assertEqual(
            '/home/ubuntu/id_rsa',
            vm['properties']['agent']['connection']['key'])
