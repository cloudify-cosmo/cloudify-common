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


from dsl_parser import (constants,
                        utils)
from dsl_parser.interfaces import interfaces_parser
from dsl_parser.elements import (operation,
                                 data_types as _data_types,
                                 types)
from dsl_parser.framework import requirements
from dsl_parser.framework.elements import Dict


class NodeTypeProperty(_data_types.SchemaProperty):
    pass


class NodeTypeProperties(_data_types.SchemaWithInitialDefault):
    schema = Dict(type=NodeTypeProperty)


class NodeTypeRuntimeProperty(_data_types.SchemaProperty):
    min_version = (1, 5)


class NodeTypeRuntimeProperties(_data_types.SchemaWithInitialDefault):
    min_version = (1, 5)
    schema = Dict(type=NodeTypeRuntimeProperty)


class NodeType(types.Type):

    schema = {
        'derived_from': types.TypeDerivedFrom,
        'interfaces': operation.NodeTypeInterfaces,
        'properties': NodeTypeProperties,
        'runtime_properties': NodeTypeRuntimeProperties,
    }
    requires = {
        'self': [requirements.Value('super_type',
                                    predicate=types.derived_from_predicate,
                                    required=False)],
        _data_types.DataTypes: [requirements.Value('data_types')]
    }

    def parse(self, super_type, data_types):
        node_type = self.build_dict_result()
        if not node_type.get('derived_from'):
            node_type.pop('derived_from', None)
        if super_type:
            node_type[constants.PROPERTIES] = utils.merge_schemas(
                overridden_schema=super_type.get(constants.PROPERTIES, {}),
                overriding_schema=node_type.get(constants.PROPERTIES, {}),
                data_types=data_types)
            node_type[constants.RUNTIME_PROPERTIES] = utils.merge_schemas(
                overridden_schema=super_type.get(
                    constants.RUNTIME_PROPERTIES, {}),
                overriding_schema=node_type.get(
                    constants.RUNTIME_PROPERTIES, {}),
                data_types=data_types)
            node_type[constants.INTERFACES] = interfaces_parser. \
                merge_node_type_interfaces(
                    overridden_interfaces=super_type[constants.INTERFACES],
                    overriding_interfaces=node_type[constants.INTERFACES])
        node_type[constants.TYPE_HIERARCHY] = self.create_type_hierarchy(
            super_type)
        self.fix_properties(node_type)
        return node_type


class NodeTypes(types.Types):

    schema = Dict(type=NodeType)
    provides = ['host_types']

    def calculate_provided(self):
        return {
            'host_types': self._types_derived_from_host_type()
        }

    def _types_derived_from_host_type(self):
        """
        Finding the types which derived from host type, while
        disregarding their namespace because host type is a base
        which will not change.
        """
        return set(type_name for type_name, _type in self.value.items()
                   if any(constants.HOST_TYPE in
                   item for item in _type[constants.TYPE_HIERARCHY]))
