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

from dsl_parser import exceptions
from dsl_parser.tests.abstract_test_parser import AbstractTestParser


class TestNotLegalNamespaces(AbstractTestParser):

    def test_not_legal(self):
        bad_namespace = ['--test', 'te--st', 'test--', '--']
        for namespace in bad_namespace:
            imported_yaml = """
inputs:
    port:
        description: the port
        default: 8080
"""
            import_file_name = self.make_yaml_file(imported_yaml)

            main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}--{1}
""".format(namespace, import_file_name)
            self.assertRaises(exceptions.DSLParsingLogicException,
                              self.parse, main_yaml)
