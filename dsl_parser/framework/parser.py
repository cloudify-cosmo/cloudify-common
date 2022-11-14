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

import numbers

# a hack to allow networkx 1.11 to work with python 3.10: gcd was moved from
# fractions to math, but networkx attempts to import from fractions. Remove
# this  after we've either upgraded or removed networkx
import fractions
if not hasattr(fractions, 'gcd'):
    import math
    fractions.gcd = math.gcd


from networkx.algorithms import (
    descendants,
    recursive_simple_cycles,
)
from networkx.classes import DiGraph
from networkx.exception import NetworkXUnfeasible

from dsl_parser.framework import elements
from dsl_parser import (exceptions,
                        constants,
                        functions,
                        utils,
                        holder)
from dsl_parser.framework.requirements import Requirement, sibling_predicate

# Will mark elements that are being parsed, that there is no need to
# add namespace to them. This is used in case of shared elements
# via yaml anchor across the blueprint.
SKIP_NAMESPACE_FLAG = 'skip_namespace'


class SchemaAPIValidator(object):

    def validate(self, element_cls):
        self._traverse_element_cls(element_cls)

    def _traverse_element_cls(self, element_cls):
        try:
            if not issubclass(element_cls, elements.Element):
                raise exceptions.DSLParsingSchemaAPIException(1)
        except TypeError:
            raise exceptions.DSLParsingSchemaAPIException(1)
        self._traverse_schema(element_cls.schema)

    def _traverse_schema(self, schema, list_nesting=0):
        if isinstance(schema, dict):
            for key, value in schema.items():
                if not isinstance(key, str):
                    raise exceptions.DSLParsingSchemaAPIException(1)
                self._traverse_element_cls(value)
        elif isinstance(schema, list):
            if list_nesting > 0:
                raise exceptions.DSLParsingSchemaAPIException(1)
            if len(schema) == 0:
                raise exceptions.DSLParsingSchemaAPIException(1)
            for value in schema:
                self._traverse_schema(value, list_nesting + 1)
        elif isinstance(schema, elements.ElementType):
            if isinstance(schema, elements.Leaf):
                self._validate_leaf_type(schema.type)
            elif isinstance(schema, elements.Dict):
                self._traverse_element_cls(schema.type)
            elif isinstance(schema, elements.List):
                self._traverse_element_cls(schema.type)
            else:
                raise exceptions.DSLParsingSchemaAPIException(1)
        else:
            raise exceptions.DSLParsingSchemaAPIException(1)

    def _validate_leaf_type(self, schema_type):
        """Check that schema_type is valid for typechecking

        schema_type needs to be something that can go into
        isinstance() checks, ie. a class, type, or tuple of classes
        and types.

        Additionally, an empty schema is not allowed.
        """
        if not schema_type:
            raise exceptions.DSLParsingSchemaAPIException(1)
        try:
            isinstance(None, schema_type)
        except TypeError:
            raise exceptions.DSLParsingSchemaAPIException(1)


_schema_validator = SchemaAPIValidator()


class Context(object):

    def __init__(self,
                 value,
                 element_cls,
                 element_name,
                 inputs):
        self.namespace = None
        self.is_cloudify_type = False
        self.inputs = inputs or {}
        self.element_type_to_elements = {}
        self._root_element = None
        self._element_tree = DiGraph()
        self._element_graph = DiGraph()
        self._traverse_element_cls(element_cls=element_cls,
                                   name=element_name,
                                   value=value,
                                   parent_element=None,
                                   namespace=self.namespace)
        self._calculate_element_graph()
        self._remove_parsing_leftovers()

    def _remove_skip_namespace_flag(self, element):
        """
        This will traverse the element in search of skip namespace flag,
        which is leftover after applying namespace on intrinsic functions.
        """
        if isinstance(element, list):
            for item in element:
                self._remove_skip_namespace_flag(item.value)
            return
        elif not isinstance(element, dict):
            return

        for k, v in element.items():
            if hasattr(element[k], SKIP_NAMESPACE_FLAG):
                delattr(element[k], SKIP_NAMESPACE_FLAG)
            if isinstance(v, holder.Holder) or k.value == 'concat':
                self._remove_skip_namespace_flag(element[k].value)

    def _remove_parsing_leftovers(self):
        """
        Removing any leftover of the parsing process, so it will not interfere
        with future parsing actions. Which will happen when the blueprint is
        already uploaded and being imported in another blueprint, so the it'
        holder structure will be used again.
        """
        for element in self._element_tree:
            element.initial_value_holder.namespace = None

            # Cleaning 'skip namespace' flag
            if hasattr(element.initial_value_holder, SKIP_NAMESPACE_FLAG):
                self._remove_skip_namespace_flag(
                    element.initial_value_holder.value)
                delattr(element.initial_value_holder, SKIP_NAMESPACE_FLAG)

    @property
    def parsed_value(self):
        return self._root_element.value if self._root_element else None

    def child_elements(self, element):
        return list(self._element_tree.successors(element))

    def ancestors_iter(self, element):
        current_element = element
        while True:
            predecessors = list(self._element_tree.predecessors(
                current_element))
            if not predecessors:
                return
            if len(predecessors) > 1:
                raise exceptions.DSLParsingFormatException(
                    1, 'More than 1 parent found for {0}'
                       .format(element))
            current_element = predecessors[0]
            yield current_element

    def descendants(self, element):
        return descendants(self._element_tree, element)

    def _add_element(self, element, parent=None):
        element_type = type(element)
        if element_type not in self.element_type_to_elements:
            self.element_type_to_elements[element_type] = []
        self.element_type_to_elements[element_type].append(element)

        self._element_tree.add_node(element)
        if parent:
            self._element_tree.add_edge(parent, element)
        else:
            self._root_element = element

    def _init_context_variable(self, var_name, var_value, element_value):
        if hasattr(element_value, var_name):
            element_var_value = getattr(element_value, var_name)
            setattr(self, var_name, element_var_value or var_value)
        else:
            # In case of Python primitive types.
            setattr(self, var_name, var_value)

    def _traverse_element_cls(self,
                              element_cls,
                              name,
                              value,
                              parent_element,
                              namespace=None,
                              is_cloudify_type=False):
        if value:
            self._init_context_variable('namespace', namespace, value)
            self._init_context_variable('is_cloudify_type',
                                        is_cloudify_type,
                                        value)
        element = element_cls(name=name,
                              initial_value=value,
                              context=self)
        self._traverse_element(schema=element_cls.schema,
                               parent_element=element)
        self._add_element(element, parent=parent_element)

    def _traverse_element(self, schema, parent_element):
        if isinstance(schema, dict):
            self._traverse_dict_schema(
                schema=schema,
                parent_element=parent_element,
                namespace=parent_element.namespace,
                is_cloudify_type=parent_element.is_cloudify_type)
        elif isinstance(schema, elements.ElementType):
            self._traverse_element_type_schema(
                schema=schema,
                parent_element=parent_element,
                namespace=parent_element.namespace,
                is_cloudify_type=parent_element.is_cloudify_type)
        elif isinstance(schema, list):
            self._traverse_list_schema(schema=schema,
                                       parent_element=parent_element)
        elif isinstance(schema, elements.UnknownSchema):
            pass
        else:
            raise ValueError('Illegal state should have been identified'
                             ' by schema API validation')

    def _traverse_dict_schema(self, schema, parent_element, namespace,
                              is_cloudify_type):
        if not isinstance(parent_element.initial_value, dict):
            return

        parsed_names = set()
        for name, element_cls in schema.items():
            if name not in parent_element.initial_value_holder:
                value = None
            else:
                name, value = \
                    parent_element.initial_value_holder.get_item(name)
                parsed_names.add(name.value)
            self._traverse_element_cls(element_cls=element_cls,
                                       name=name,
                                       value=value,
                                       parent_element=parent_element,
                                       namespace=namespace,
                                       is_cloudify_type=is_cloudify_type)
        for k_holder, v_holder in parent_element.initial_value_holder.value.\
                items():
            if k_holder.value not in parsed_names:
                self._traverse_element_cls(element_cls=elements.UnknownElement,
                                           name=k_holder,
                                           value=v_holder,
                                           parent_element=parent_element,
                                           namespace=namespace,
                                           is_cloudify_type=is_cloudify_type)

    def _traverse_element_type_schema(self, schema, parent_element, namespace,
                                      is_cloudify_type):

        def set_namespace_node_intrinsic_functions(namespace_value,
                                                   func_parameters,
                                                   holder_func_parameters):
            """
            This will add namespace to get_property and get_attribute
            functions.
            """
            value = func_parameters[0]
            if value not in functions.AVAILABLE_NODE_TARGETS:
                namespaced_value = utils.generate_namespaced_value(
                    namespace_value, value)
                func_parameters[0] = namespaced_value
                holder_func_parameters[0].value = namespaced_value

        def handle_intrinsic_function_namespace(holder_element, element):
            """
            This will traverse the element in search of the key,
            and will run the set namespace function on the key's
            values only for the relevant intrinsic functions.
            """
            def traverse_list(holder_item, item):
                for holder_value, value in zip(holder_item.value, item):
                    handle_intrinsic_function_namespace(holder_value, value)

            if isinstance(element, list):
                traverse_list(holder_element, element)
                return
            elif not isinstance(element, dict):
                # There is no need to search for intrinsic functions, if
                # there is no namespace or if the element can not contain
                # them.
                return

            for k, v in element.items():
                holder_key, holder_value = holder_element.get_item(k)
                if hasattr(holder_value, SKIP_NAMESPACE_FLAG):
                    return
                if k == 'get_input' and not isinstance(v, list):
                    namespaced_value =\
                        utils.generate_namespaced_value(namespace, v)
                    element[k] = namespaced_value
                    holder_element.value[holder_key].value = namespaced_value
                    holder_value.skip_namespace = True
                if k == 'get_input' and isinstance(v, list):
                    if isinstance(v[0], str):
                        element[k][0] =\
                            utils.generate_namespaced_value(namespace, v[0])
                elif k == 'get_property' or k == 'get_attribute':
                    set_namespace_node_intrinsic_functions(
                        namespace,
                        v,
                        holder_element.value[holder_key].value)
                    holder_value.skip_namespace = True
                if isinstance(v, dict) or k == 'concat':
                    handle_intrinsic_function_namespace(
                        holder_element.value[holder_key], v)
                elif isinstance(v, list):
                    traverse_list(holder_element.value[holder_key], v)

        def should_add_namespace_to_string_leaf(element):
            if not isinstance(element._initial_value, str):
                return False
            is_premitive_type = (element._initial_value in
                                 constants.USER_PRIMITIVE_TYPES)
            overridable_cloudify_type = \
                utils.check_if_overridable_cloudify_type(
                    element._initial_value)
            should_skip_adding_namespace =\
                hasattr(element.initial_value_holder, SKIP_NAMESPACE_FLAG)
            return (not is_premitive_type and
                    not overridable_cloudify_type and
                    not should_skip_adding_namespace and
                    element.add_namespace_to_schema_elements)

        def set_leaf_namespace(element):
            """
            Will update, if necessary, leaf element namespace.
            Also will update it's holder, for also resolving the namespace
            in the holder object.
            """
            if should_add_namespace_to_string_leaf(element):
                namespaced_value = utils.generate_namespaced_value(
                        namespace, element._initial_value)
                element._initial_value = namespaced_value
                element.initial_value_holder.value = namespaced_value
            elif isinstance(element._initial_value, dict):
                handle_intrinsic_function_namespace(
                    element.initial_value_holder,
                    element._initial_value)

            # We need to use this flag, only for yaml level linking.
            element.initial_value_holder.skip_namespace = True

        def should_add_element_namespace(element_holder):
            # Preventing of adding namespace prefix to cloudify
            # basic types.
            overridable_cloudify_type =\
                utils.check_if_overridable_cloudify_type(element_holder.value)
            return (not overridable_cloudify_type and
                    not hasattr(element_holder, SKIP_NAMESPACE_FLAG))

        def set_element_namespace(element_namespace, element_holder):
            if (not element_namespace or
                    not should_add_element_namespace(element_holder)):
                return
            element_holder.value = utils.generate_namespaced_value(
                element_namespace, element_holder.value)

        if isinstance(schema, elements.Leaf):
            if not is_cloudify_type and namespace:
                set_leaf_namespace(parent_element)
            return

        element_cls = schema.type
        if isinstance(schema, elements.Dict):
            if not isinstance(parent_element.initial_value, dict):
                return
            for name_holder, value_holder in parent_element.\
                    initial_value_holder.value.items():
                current_namespace = value_holder.namespace or namespace
                current_is_cloudify_type = (value_holder.is_cloudify_type or
                                            is_cloudify_type)
                if (parent_element.add_namespace_to_schema_elements and
                        not value_holder.only_children_namespace and
                        not current_is_cloudify_type):
                    set_element_namespace(current_namespace, name_holder)
                self._traverse_element_cls(
                    element_cls=element_cls,
                    name=name_holder,
                    value=value_holder,
                    parent_element=parent_element,
                    namespace=current_namespace,
                    is_cloudify_type=current_is_cloudify_type)
        elif isinstance(schema, elements.List):
            if not isinstance(parent_element.initial_value, list):
                return
            for index, value_holder in enumerate(
                    parent_element.initial_value_holder.value):
                self._traverse_element_cls(element_cls=element_cls,
                                           name=index,
                                           value=value_holder,
                                           parent_element=parent_element,
                                           namespace=namespace,
                                           is_cloudify_type=is_cloudify_type)
        else:
            raise ValueError('Illegal state should have been identified'
                             ' by schema API validation')

    def _traverse_list_schema(self, schema, parent_element):
        for schema_item in schema:
            self._traverse_element(schema=schema_item,
                                   parent_element=parent_element)

    def _calculate_element_graph(self):
        self.element_graph = DiGraph(self._element_tree)
        for element_type, _elements in self.element_type_to_elements.items():
            requires = element_type.requires
            for requirement, requirement_values in requires.items():
                requirement_values = [
                    Requirement(r) if isinstance(r, str)
                    else r for r in requirement_values]
                if requirement == 'inputs':
                    continue
                if requirement == 'self':
                    requirement = element_type
                dependencies = self.element_type_to_elements.get(
                    requirement, [])
                predicates = [r.predicate for r in requirement_values
                              if r.predicate is not None]

                if not predicates:
                    dep = _BatchDependency(element_type, requirement)
                    for dependency in dependencies:
                        self.element_graph.add_edge(dep, dependency)
                    for element in _elements:
                        self.element_graph.add_edge(element, dep)
                    continue

                if predicates == [sibling_predicate]:
                    # If we don't do this, our time complexity is n**2 as
                    # we compare all 'default' elements to all 'type'
                    # elements (for example), when all we care about is if
                    # they are siblings.
                    for dependency in dependencies:
                        for element in dependency.parent().children():
                            if element in _elements:
                                self.element_graph.add_edge(element,
                                                            dependency)
                    continue

                for dependency in dependencies:
                    for element in _elements:
                        add_dependency = all(
                            predicate(element, dependency)
                            for predicate in predicates)
                        if add_dependency:
                            self.element_graph.add_edge(element, dependency)
        # we reverse the graph because only networkx 1.9.1 has the reverse
        # flag in the topological sort function, it is only used by it
        # so this should be good
        self.element_graph.reverse(copy=False)

    def elements_graph_topological_sort(self):
        try:
            return utils.topological_sort(self.element_graph, reverse=True)
        except NetworkXUnfeasible:
            # Cycle detected
            cycle = recursive_simple_cycles(self.element_graph)[0]
            names = [str(e.name) for e in cycle]
            names.append(str(names[0]))
            ex = exceptions.DSLParsingLogicException(
                exceptions.ERROR_CODE_CYCLE,
                'Parsing failed. Circular dependency detected: {0}'
                .format(' --> '.join(names)))
            ex.circular_dependency = names
            raise ex


class _BatchDependency(object):
    """Marker object to represent dependencies between types of elements.

    To force traversing the graph in order, we add edges between types
    of elements.
    This is used if all elements of one type must come before all
    elements of another type.
    """
    def __init__(self, dependent_type, dependency_type):
        self._dependent_type = dependent_type
        self._dependency_type = dependency_type


class Parser(object):

    def parse(self,
              value,
              element_cls,
              element_name=constants.ROOT_ELEMENT_VALUE,
              inputs=None,
              strict=True):
        context = Context(
            value=value,
            element_cls=element_cls,
            element_name=element_name,
            inputs=inputs)

        for element in context.elements_graph_topological_sort():

            if isinstance(element, _BatchDependency):
                continue
            try:
                self._validate_element_schema(element, strict=strict)
                self._process_element(element)
            except exceptions.DSLParsingException as e:
                if not e.element:
                    e.element = element
                raise
        return context.parsed_value

    @staticmethod
    def _validate_element_schema(element, strict):
        value = element.initial_value
        if element.required and value is None:
            raise exceptions.DSLParsingFormatException(
                1, "'{0}' key is required but it is currently missing"
                   .format(element.name))

        def validate_schema(schema):
            if isinstance(schema, (dict, elements.Dict)):
                if not isinstance(value, dict):
                    raise exceptions.DSLParsingFormatException(
                        1, _expected_type_message(value, dict))
                for key in value:
                    if not isinstance(key, str):
                        raise exceptions.DSLParsingFormatException(
                            1, "Dict keys must be strings but"
                               " found '{0}' of type '{1}'"
                               .format(key, _py_type_to_user_type(type(key))))

            if strict and isinstance(schema, dict):
                for key in value:
                    if key not in schema:
                        ex = exceptions.DSLParsingFormatException(
                            1, "'{0}' is not in schema. "
                               "Valid schema values: {1}"
                               .format(key, list(schema)))
                        for child_element in element.children():
                            if child_element.name == key:
                                ex.element = child_element
                                break
                        raise ex

            if (isinstance(schema, elements.List) and
                    not isinstance(value, list)):
                raise exceptions.DSLParsingFormatException(
                    1, _expected_type_message(value, list))

            if (isinstance(schema, elements.Leaf) and
                    not isinstance(value, schema.type)):
                raise exceptions.DSLParsingFormatException(
                    1, _expected_type_message(value, schema.type))
        if value is not None:
            if isinstance(element.schema, list):
                validated = False
                last_error = None
                for schema_item in element.schema:
                    try:
                        validate_schema(schema_item)
                    except exceptions.DSLParsingFormatException as e:
                        last_error = e
                    else:
                        validated = True
                        break
                if not validated:
                    if not last_error:
                        raise ValueError('Illegal state should have been '
                                         'identified by schema API validation')
                    else:
                        raise last_error
            else:
                validate_schema(element.schema)

    def _process_element(self, element):
        required_args = self._extract_element_requirements(element)
        element.validate(**required_args)
        element.value = element.parse(**required_args)
        element.provided = element.calculate_provided(**required_args)

    @staticmethod
    def _extract_element_requirements(element):
        context = element.context
        required_args = {}
        for required_type, requirements in element.requires.items():
            requirements = [Requirement(r) if isinstance(r, str)
                            else r for r in requirements]
            if not requirements:
                # only set required type as a logical dependency
                pass
            elif required_type == 'inputs':
                for input in requirements:
                    if input.name not in context.inputs and input.required:
                        raise exceptions.DSLParsingFormatException(
                            1, "Missing required input '{0}'. "
                               "Existing inputs: ".format(input.name))
                    required_args[input.name] = context.inputs.get(input.name)
            else:
                if required_type == 'self':
                    required_type = type(element)

                if (
                    len(requirements) == 1
                    and requirements[0].predicate == sibling_predicate
                ):
                    # Similar to the other siblings predicate check above,
                    # doing this saves a massive amount of time on larger
                    # blueprints by avoiding n**2 time complexity.
                    required_type_elements = [
                        child for child in element.parent().children()
                        if isinstance(child, required_type)]
                else:
                    required_type_elements = (
                        context.element_type_to_elements.get(required_type,
                                                             [])
                    )

                for requirement in requirements:
                    result = []
                    for required_element in required_type_elements:
                        if requirement.predicate and not requirement.predicate(
                                element, required_element):
                            continue
                        if requirement.parsed:
                            result.append(required_element.value)
                        else:
                            if (requirement.name not in
                                    required_element.provided):
                                provided = list(required_element.provided)
                                if requirement.required:
                                    raise exceptions.DSLParsingFormatException(
                                        1,
                                        "Required value '{0}' is not "
                                        "provided by '{1}'. Provided values "
                                        "are: {2}"
                                        .format(requirement.name,
                                                required_element.name,
                                                provided))
                                else:
                                    continue
                            result.append(required_element.provided[
                                requirement.name])

                    if len(result) != 1 and not requirement.multiple_results:
                        if requirement.required:
                            raise exceptions.DSLParsingFormatException(
                                1, "Expected exactly one result for "
                                   "requirement '{0}' but found {1}"
                                   .format(requirement.name,
                                           'none' if not result else result))
                        elif not result:
                            result = [None]
                        else:
                            raise ValueError('Illegal state')

                    if not requirement.multiple_results:
                        result = result[0]
                    required_args[requirement.name] = result

        return required_args


_parser = Parser()


def validate_schema_api(element_cls):
    _schema_validator.validate(element_cls)


def parse(value,
          element_cls,
          element_name=constants.ROOT_ELEMENT_VALUE,
          inputs=None,
          strict=True):
    validate_schema_api(element_cls)
    return _parser.parse(value=value,
                         element_cls=element_cls,
                         element_name=element_name,
                         inputs=inputs,
                         strict=strict)


def _expected_type_message(value, expected_type):
    return ("Expected '{0}' type but found '{1}' type"
            .format(_py_type_to_user_type(expected_type),
                    _py_type_to_user_type(type(value))))


def _py_type_to_user_type(_type):
    if isinstance(_type, tuple):
        return list(set(_py_type_to_user_type(t) for t in _type))
    elif issubclass(_type, str):
        return 'string'
    elif issubclass(_type, bool):
        return 'boolean'
    elif issubclass(_type, numbers.Integral):
        return 'integer'
    elif issubclass(_type, float):
        return 'float'
    elif issubclass(_type, dict):
        return 'dict'
    elif issubclass(_type, list):
        return 'list'
    else:
        raise ValueError('Unexpected type: {0}'.format(_type))
