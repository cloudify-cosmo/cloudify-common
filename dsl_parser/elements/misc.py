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

from dsl_parser import elements
from dsl_parser._compat import text_type
from dsl_parser.elements import version as element_version
from dsl_parser.framework.elements import (DictElement,
                                           DictNoDefaultElement,
                                           Element,
                                           Leaf,
                                           List,
                                           Dict)


class OutputDescription(Element):

    schema = Leaf(type=text_type)
    add_namespace_to_schema_elements = False


class OutputValue(Element):

    required = True
    schema = Leaf(type=elements.PRIMITIVE_TYPES)


class Output(DictNoDefaultElement):

    schema = {
        'description': OutputDescription,
        'value': OutputValue
    }


class Outputs(DictElement):

    schema = Dict(type=Output)


class CapabilityDescription(Element):

    schema = Leaf(type=text_type)
    add_namespace_to_schema_elements = False


class CapabilityValue(Element):

    required = True
    schema = Leaf(type=elements.PRIMITIVE_TYPES)


class Capability(DictNoDefaultElement):

    schema = {
        'description': CapabilityDescription,
        'value': CapabilityValue
    }


class Capabilities(DictElement):

    schema = Dict(type=Capability)


class DSLDefinitions(Element):

    schema = Leaf(type=[dict, list])
    requires = {
        element_version.ToscaDefinitionsVersion: ['version'],
        'inputs': ['validate_version']
    }

    def validate(self, version, validate_version):
        if validate_version:
            self.validate_version(version, (1, 2))


class Description(Element):

    schema = Leaf(type=text_type)

    requires = {
        element_version.ToscaDefinitionsVersion: ['version'],
        'inputs': ['validate_version']
    }

    def validate(self, version, validate_version):
        if validate_version:
            self.validate_version(version, (1, 2))


class Metadata(Element):

    schema = Leaf(type=dict)


class Imported(Element):

    schema = Leaf(type=text_type)


class ImportedBlueprints(Element):
    """
    Internal DSL element used for maintaining a list of imported blueprints,
    this is for enabling protection against their deletion when used.
    """

    schema = List(type=Imported)


class NamespaceMapping(Element):

    schema = Leaf(type=text_type)
    add_namespace_to_schema_elements = False


class NamespacesMapping(DictElement):
    """
    An internal DSL element for mapping all the used blueprints import
    namespaces.
    """
    schema = Dict(type=NamespaceMapping)
