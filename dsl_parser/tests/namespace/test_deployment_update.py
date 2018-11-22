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

import copy

from dsl_parser.tests.test_deployment_update import BaseDeploymentUpdateTest
from dsl_parser.constants import (NODES,
                                  ADDED_AND_RELATED,
                                  REMOVED_AND_RELATED,
                                  EXTENDED_AND_RELATED,
                                  REDUCED_AND_RELATED,
                                  RELATIONSHIPS
                                  )


class TestNamespacedDeploymentUpdate(BaseDeploymentUpdateTest):
    BASE_BLUEPRINT = """
node_types:
    cloudify.nodes.Compute:
        properties:
            x:
                default: y
    db: {}
    webserver: {}
    db_dependent: {}
    type: {}
    network: {}
relationships:
    cloudify.relationships.depends_on:
        properties:
            connection_type:
                default: 'all_to_all'
    cloudify.relationships.contained_in:
        derived_from: cloudify.relationships.depends_on
    cloudify.relationships.connected_to:
        derived_from: cloudify.relationships.depends_on

node_templates:
    without_rel:
        type: type
    with_rel:
        type: type
"""

    def test_add_node(self):
        import_file_name = self.make_yaml_file(self.BASE_BLUEPRINT)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        plan = self.parse_multi(main_yaml)
        plan[NODES].append({
            'name': 'new_node',
            'id': 'new_node',
            'type': 'new_type',
            'number_of_instances': 1,
            'deploy_number_of_instances': 1,
            'min_number_of_instances': 1,
            'max_number_of_instances': 1,
            'relationships': [
                {'type': 'test->cloudify.relationships.connected_to',
                 'target_id': 'test->without_rel',
                 'type_hierarchy': ['cloudify.relationships.connected_to'],
                 'properties': {
                     'connection_type': 'all_to_all'
                 },
                 }
            ]
        })

        modified_nodes = plan[NODES]
        node_instances = self.modify_multi(plan, modified_nodes=modified_nodes)

        self.assertEqual(len(node_instances[ADDED_AND_RELATED]), 2)
        added_and_related = node_instances[ADDED_AND_RELATED]
        added = [n for n in added_and_related if 'modification' in n]
        related = [n for n in added_and_related if n not in added]
        self.assertEqual(len(added), 1)
        self.assertEqual(len(related), 1)
        self.assertEqual(len(node_instances[REMOVED_AND_RELATED]), 0)
        self.assertEqual(len(node_instances[EXTENDED_AND_RELATED]), 0)
        self.assertEqual(len(node_instances[REDUCED_AND_RELATED]), 0)

    def test_remove_node(self):
        imported_yaml = self.BASE_BLUEPRINT + """
        relationships:
            -  type: cloudify.relationships.connected_to
               target: without_rel
"""

        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        plan = self.parse_multi(main_yaml)
        nodes = copy.deepcopy(
            [n for n in plan[NODES] if n['id'] != 'test->without_rel'])
        with_rel_node = nodes[0]
        with_rel_node[RELATIONSHIPS] = \
            [r for r in
             with_rel_node[RELATIONSHIPS]
             if r['target_id'] != 'test->without_rel']
        node_instances = self.modify_multi(plan, modified_nodes=nodes)

        self.assertEqual(len(node_instances[ADDED_AND_RELATED]), 0)
        self.assertEqual(len(node_instances[REMOVED_AND_RELATED]), 2)
        removed_and_related = node_instances[REMOVED_AND_RELATED]
        removed = [n for n in removed_and_related if 'modification' in n]
        related = [n for n in removed_and_related if n not in removed]
        self.assertEqual(len(removed), 1)
        self.assertEqual(len(related), 1)
        self.assertEqual(len(node_instances[EXTENDED_AND_RELATED]), 0)
        self.assertEqual(len(node_instances[REDUCED_AND_RELATED]), 1)
        reduced_and_related = node_instances[REDUCED_AND_RELATED]
        reduced = [n for n in reduced_and_related if 'modification' in n]
        self.assertEqual(len(reduced), 1)

    def test_add_relationship(self):
        import_file_name = self.make_yaml_file(self.BASE_BLUEPRINT)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        plan = self.parse_multi(main_yaml)

        rel_type = 'test->cloudify.relationships.connected_to'
        with_rel = [n for n in plan[NODES]
                    if n['id'] == 'test->with_rel'][0]
        without_rel = [n for n in plan[NODES]
                       if n['id'] == 'test->without_rel'][0]
        with_rel[RELATIONSHIPS] = \
            [{'type': rel_type,
              'type_hierarchy': [rel_type],
              'target_id': without_rel['id'],
              'source_interface': {
                  'cloudify.interfaces.relationship_lifecycle': {
                      'preconfigure': 'scripts/increment.sh',
                      'establish': 'scripts/increment.sh',
                      'postconfigure': 'scripts/increment.sh'
                  }
              },
              'properties': {
                  'connection_type': 'all_to_all'
              }}]
        modified_nodes = [with_rel, without_rel]
        node_instances = self.modify_multi(plan, modified_nodes=modified_nodes)

        self.assertEqual(len(node_instances[ADDED_AND_RELATED]), 0)
        self.assertEqual(len(node_instances[REMOVED_AND_RELATED]), 0)
        self.assertEqual(len(node_instances[EXTENDED_AND_RELATED]), 2)
        extended_and_related = node_instances[EXTENDED_AND_RELATED]
        extended = [n for n in extended_and_related if 'modification' in n]
        related = [n for n in extended_and_related if n not in extended]
        self.assertEqual(len(extended), 1)
        self.assertEqual(len(related), 1)
        self.assertEqual(len(node_instances[REDUCED_AND_RELATED]), 0)

    def test_remove_relationship(self):
        imported_yaml = self.BASE_BLUEPRINT + """
        relationships:
            -  type: cloudify.relationships.connected_to
               target: without_rel
    """

        import_file_name = self.make_yaml_file(imported_yaml)

        main_yaml = self.BASIC_VERSION_SECTION_DSL_1_3 + """
imports:
    -   {0}->{1}
""".format('test', import_file_name)

        plan = self.parse_multi(main_yaml)

        nodes = copy.deepcopy(plan[NODES])
        node_with_rel = [n for n in nodes if n['id'] == 'test->with_rel'][0]
        node_with_rel[RELATIONSHIPS] = []

        node_instances = self.modify_multi(plan, modified_nodes=nodes)

        self.assertEqual(len(node_instances[ADDED_AND_RELATED]), 0)
        self.assertEqual(len(node_instances[REMOVED_AND_RELATED]), 0)
        self.assertEqual(len(node_instances[EXTENDED_AND_RELATED]), 0)
        self.assertEqual(len(node_instances[REDUCED_AND_RELATED]), 2)
        reduced_and_related = node_instances[REDUCED_AND_RELATED]
        reduced = [n for n in reduced_and_related if 'modification' in n]
        related = [n for n in reduced_and_related if n not in reduced]
        self.assertEqual(len(reduced), 1)
        self.assertEqual(len(related), 1)
