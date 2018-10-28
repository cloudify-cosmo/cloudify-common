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

        result = self.parse(top_level_yaml)