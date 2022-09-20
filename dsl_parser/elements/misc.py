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

from dsl_parser import elements, exceptions
from dsl_parser.elements import version as element_version
from dsl_parser.elements.deployment_schedules import DeploymentSchedules
from dsl_parser.framework.elements import (DictElement,
                                           DictNoDefaultElement,
                                           Element,
                                           Leaf,
                                           List,
                                           Dict)


class OutputDescription(Element):

    schema = Leaf(type=str)
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

    schema = Leaf(type=str)
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

    schema = Leaf(type=str)

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

    schema = Leaf(type=str)


class ImportedBlueprints(Element):
    """
    Internal DSL element used for maintaining a list of imported blueprints,
    this is for enabling protection against their deletion when used.
    """

    schema = List(type=Imported)


class NamespaceMapping(Element):

    schema = Leaf(type=str)
    add_namespace_to_schema_elements = False


class NamespacesMapping(DictElement):
    """
    An internal DSL element for mapping all the used blueprints import
    namespaces.
    """
    schema = Dict(type=NamespaceMapping)


class LabelValue(Element):
    required = True
    schema = Leaf(type=list)


class BlueprintLabel(DictNoDefaultElement):
    schema = {
        'values': LabelValue
    }

    def validate(self, **kwargs):
        """
        A blueprint label's value cannot be an intrinsic function, as labels
        are assigned to a blueprint while it's uploaded, and the intrinsic
        functions are not yet processed.
        """
        type_err_msg = "The blueprint label's value must be a string. " \
                       "Please modify the values of {0}"

        for value in self.initial_value['values']:
            if not isinstance(value, str):
                raise exceptions.DSLParsingException(
                    1, type_err_msg.format(self.name))


class DeploymentLabel(DictNoDefaultElement):
    schema = {
        'values': LabelValue
    }

    def validate(self, **kwargs):
        """
        A label's value cannot be a runtime property, since labels are
        assigned to deployment during its creation.
        """
        type_err_msg = "The label's value must be a string or an intrinsic " \
                       "function. Please modify the values of {0}"
        get_attr_err_msg = "The label's value cannot be a runtime property. " \
                           "Please remove the `get_attribute` function from " \
                           "the values of {0}"

        for value in self.initial_value['values']:
            if not isinstance(value, (dict, str)):
                raise exceptions.DSLParsingException(
                    1, type_err_msg.format(self.name))
            if isinstance(value, dict) and 'get_attribute' in value:
                raise exceptions.DSLParsingException(
                    1, get_attr_err_msg.format(self.name))


class ResourceTag(Element):
    schema = Leaf(type=elements.PRIMITIVE_TYPES)


class Labels(DictElement):
    schema = Dict(type=DeploymentLabel)


class BlueprintLabels(DictElement):
    schema = Dict(type=BlueprintLabel)


class DeploymentGroups(Element):
    schema = Leaf(list)


class DeploymentIDTemplate(Element):
    schema = Leaf(str)


class DeploymentDisplayName(Element):
    schema = Leaf(type=(str, dict))


class DeploymentSettings(DictNoDefaultElement):
    schema = {
        'default_schedules': DeploymentSchedules,
        'default_groups': DeploymentGroups,
        'id_template': DeploymentIDTemplate,
        'display_name': DeploymentDisplayName
    }


class ResourceTags(DictElement):
    schema = Dict(type=ResourceTag)
    requires = {
        element_version.ToscaDefinitionsVersion: ['version'],
        'inputs': ['validate_version']
    }

    def validate(self, version, validate_version):
        if validate_version:
            self.validate_version(version, (1, 3))
