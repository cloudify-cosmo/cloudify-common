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
from io import StringIO

from dsl_parser import exceptions
from dsl_parser import holder
from dsl_parser import version as _version


class Unparsed(object):
    pass


UNPARSED = Unparsed()


class ElementType(object):

    def __init__(self, type):
        if isinstance(type, list):
            type = tuple(type)
        self.type = type


class Leaf(ElementType):
    pass


class Dict(ElementType):
    pass


class List(ElementType):
    pass


class Element(object):

    schema = None
    required = False
    requires = {}
    provides = []
    """
    Values this element provides in the format of [ string [, string, ... ] ].
    """

    add_namespace_to_schema_elements = True
    """
    When True this flag is for marking DSL element's schema fields
    will get the namespace prefix, when they are imported with
    a namespace context. So those elements will not collide if
    imported several of times with different namespaces.

    This flag should be overwritten to False for the following elements:
    - A leaf element of primitive types (int, float, string and etc),
      this case is not definite so consider each case to it self.
    - An element of type "Type" (like NodeType, DataType and etc).
    - An element which is contained fully in the context of father element,
      and cannot be referenced from outside that context. like: node type
      properties.
    NOTICE: You need to add unit tests both to the element and to the
    namespaced scenario in both cases.
    """

    def __init__(self, context, initial_value, name=None):
        self.context = context
        initial_value = holder.Holder.of(initial_value)
        self.initial_value_holder = initial_value
        self._initial_value = initial_value.restore()
        self.start_line = initial_value.start_line
        self.start_column = initial_value.start_column
        self.end_line = initial_value.end_line
        self.end_column = initial_value.end_column
        self.filename = initial_value.filename
        name = holder.Holder.of(name)
        self.name = name.restore()
        self.name_start_line = name.start_line
        self.name_start_column = name.start_column
        self.name_end_line = name.end_line
        self.name_end_column = name.end_column
        self._parsed_value = UNPARSED
        self._provided = None
        self.namespace = self.context.namespace
        self.is_cloudify_type = self.context.is_cloudify_type

    def __str__(self):
        message = StringIO()
        if self.filename:
            message.write('\n  in: {0}'.format(self.filename))
        if self.name_start_line is not None and self.name_start_line >= 0:
            message.write('\n  in line: {0}, column: {1}'
                          .format(self.name_start_line + 1,
                                  self.name_start_column))
        elif self.start_line is not None and self.start_line >= 0:
            message.write('\n  in line {0}, column {1}'
                          .format(self.start_line + 1, self.start_column))
        message.write('\n  path: {0}'.format(self.path))
        message.write('\n  value: {0}'.format(self._initial_value))

        return message.getvalue()

    def validate(self, **kwargs):
        pass

    def parse(self, **kwargs):
        return self.initial_value

    @property
    def index(self):
        """Alias name for list based elements"""
        return self.name

    @property
    def initial_value(self):
        return copy.deepcopy(self._initial_value)

    @property
    def value(self):
        if self._parsed_value == UNPARSED:
            raise exceptions.DSLParsingSchemaAPIException(
                exceptions.ERROR_CODE_ILLEGAL_VALUE_ACCESS,
                'Cannot access element value before parsing')
        return copy.deepcopy(self._parsed_value)

    @value.setter
    def value(self, val):
        self._parsed_value = val

    def calculate_provided(self, **kwargs):
        return {}

    @property
    def provided(self):
        return copy.deepcopy(self._provided)

    @provided.setter
    def provided(self, value):
        self._provided = value

    @property
    def path(self):
        elements = [str(e.name) for e in self.context.ancestors_iter(self)]
        if elements:
            elements.pop()
        elements.reverse()
        elements.append(str(self.name))
        return '.'.join(elements)

    @property
    def defined(self):
        return self.value is not None or self.start_line is not None

    def parent(self):
        return next(self.context.ancestors_iter(self))

    def ancestor(self, element_type):
        matches = [e for e in self.context.ancestors_iter(self)
                   if isinstance(e, element_type)]
        if not matches:
            raise exceptions.DSLParsingElementMatchException(
                "No matches found for '{0}'".format(element_type))
        if len(matches) > 1:
            raise exceptions.DSLParsingElementMatchException(
                "Multiple matches found for '{0}'".format(element_type))
        return matches[0]

    def descendants(self, element_type):
        return [e for e in self.context.descendants(self)
                if isinstance(e, element_type)]

    def child(self, element_type):
        matches = [e for e in self.context.child_elements(self)
                   if isinstance(e, element_type)]
        if not matches:
            raise exceptions.DSLParsingElementMatchException(
                "No matches found for '{0}'".format(element_type))
        if len(matches) > 1:
            raise exceptions.DSLParsingElementMatchException(
                "Multiple matches found for '{0}'".format(element_type))
        return matches[0]

    def build_dict_result(self, with_default=True):
        return dict((child.name, child.value)
                    for child in self.context.child_elements(self)
                    if with_default or child.defined)

    def children(self):
        return self.context.child_elements(self)

    def sibling(self, element_type):
        return self.parent().child(element_type)

    def validate_version(self, version, min_version, error_msg=None):
        if self.initial_value is not None and version < min_version:
            if self.name == 'type':
                dsl_node = "type '{0}'".format(self.initial_value)
            else:
                dsl_node = self.name
            raise exceptions.DSLParsingLogicException(
                exceptions.ERROR_CODE_DSL_DEFINITIONS_VERSION_MISMATCH,
                error_msg or
                '{0} not supported in version {1}, it was added in {2}'.format(
                    dsl_node,
                    _version.version_description(version),
                    _version.version_description(min_version)
                )
            )


class DictElement(Element):

    # If turned off will not initialize undefined fields
    # by user.
    with_default = True

    def parse(self, **kwargs):
        return self.build_dict_result(self.with_default)


class DictNoDefaultElement(DictElement):
    with_default = False


class UnknownSchema(object):
    pass


class UnknownElement(Element):

    schema = UnknownSchema()
