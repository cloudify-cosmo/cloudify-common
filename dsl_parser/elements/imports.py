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

import os
import urllib

import networkx as nx

from dsl_parser import (exceptions,
                        constants,
                        version as _version,
                        utils)
from dsl_parser.holder import Holder
from dsl_parser.import_resolver.abstract_import_resolver import\
    is_remote_resource
from dsl_parser.framework.elements import (Element,
                                           Leaf,
                                           List)


MERGE_NO_OVERRIDE = set([
    constants.INTERFACES,
    constants.NODE_TYPES,
    constants.PLUGINS,
    constants.WORKFLOWS,
    constants.RELATIONSHIPS,
    constants.POLICY_TYPES,
    constants.GROUPS,
    constants.POLICY_TRIGGERS,
    constants.DATA_TYPES
])

MERGEABLE_FROM_DSL_VERSION_1_3 = [
    constants.INPUTS,
    constants.OUTPUTS,
    constants.NODE_TEMPLATES,
    constants.CAPABILITIES
]

DONT_OVERWRITE = set([
    constants.DESCRIPTION
])

IGNORE = set([
    constants.DSL_DEFINITIONS,
    constants.IMPORTS,
    _version.VERSION
])


class Import(Element):

    schema = Leaf(type=str)


class Imports(Element):

    schema = List(type=Import)


class ImportLoader(Element):

    schema = Leaf(type=str)


class ImportsLoader(Element):

    schema = List(type=ImportLoader)
    provides = ['resource_base']
    requires = {
        'inputs': ['main_blueprint_holder',
                   'resources_base_path',
                   'blueprint_location',
                   'version',
                   'resolver',
                   'validate_version']
    }

    resource_base = None
    imported = None

    def validate(self, **kwargs):
        imports = [i.value for i in self.children()]
        imports_set = set()
        for _import in imports:
            if _import in imports_set:
                raise exceptions.DSLParsingFormatException(
                    2, 'Duplicate imports')
            imports_set.add(_import)

    def parse(self,
              main_blueprint_holder,
              resources_base_path,
              blueprint_location,
              version,
              resolver,
              validate_version):
        if blueprint_location:
            blueprint_location = _dsl_location_to_url(
                dsl_location=blueprint_location,
                resources_base_path=resources_base_path)
            slash_index = blueprint_location.rfind('/')
            self.resource_base = blueprint_location[:slash_index]
        imports_list, parsed_blueprint = _combine_imports(
            parsed_dsl_holder=main_blueprint_holder,
            dsl_location=blueprint_location,
            resources_base_path=resources_base_path,
            version=version,
            resolver=resolver,
            validate_version=validate_version)
        self.imported = imports_list
        return parsed_blueprint

    def calculate_provided(self, **kwargs):
        return {
            'resource_base': self.resource_base,
            constants.IMPORTED: self.imported
        }


def _dsl_location_to_url(dsl_location, resources_base_path):
    if dsl_location is not None:
        dsl_location = _get_resource_location(dsl_location,
                                              resources_base_path)
        if dsl_location is None:
            ex = exceptions.DSLParsingLogicException(
                30, "Failed converting dsl "
                    "location to url: no suitable "
                    "location found "
                    "for dsl '{0}'"
                    .format(dsl_location))
            ex.failed_import = dsl_location
            raise ex
    return dsl_location


def _get_resource_location(resource_name,
                           resources_base_path,
                           current_resource_context=None):
    if is_remote_resource(resource_name):
        return resource_name

    if os.path.exists(resource_name):
        return 'file:{0}'.format(
            urllib.pathname2url(os.path.abspath(resource_name)))

    if current_resource_context:
        candidate_url = current_resource_context[
            :current_resource_context.rfind('/') + 1] + resource_name
        if utils.url_exists(candidate_url):
            return candidate_url

    if resources_base_path:
        full_path = os.path.join(resources_base_path, resource_name)
        return 'file:{0}'.format(
            urllib.pathname2url(os.path.abspath(full_path)))

    return None


def _extract_import_parts(import_url,
                          resources_base_path,
                          current_resource_context=None):
    """
    :param import_url: a string which is the import path
    :param resources_base_path: In case of a relative file path, this
                                is the base path.
    :param current_resource_context: Current import statement,
    :return: Will return a breakdown of the URL to
            (namespace, import_url). If the import is not
            namespaced, the returned namespace will be
            None.
    """
    namespace_op_location = utils.find_namespace_location(import_url)
    namespace = None

    if namespace_op_location is not -1:
        namespace = import_url[:namespace_op_location]
        import_url = import_url[namespace_op_location + 2:]

    return namespace, _get_resource_location(import_url,
                                             resources_base_path,
                                             current_resource_context)


def _combine_imports(parsed_dsl_holder, dsl_location,
                     resources_base_path, version, resolver,
                     validate_version):
    ordered_imports = _build_ordered_imports(parsed_dsl_holder,
                                             dsl_location,
                                             resources_base_path,
                                             resolver)
    holder_result = parsed_dsl_holder.copy()
    version_key_holder, version_value_holder = parsed_dsl_holder.get_item(
        _version.VERSION)
    holder_result.value = {}
    used_imports = []
    for imported in ordered_imports:
        import_url, namespace = imported['import']
        if import_url is not constants.ROOT_ELEMENT_VALUE:
            used_imports.append(import_url)
        parsed_imported_dsl_holder = imported['parsed']
        if validate_version:
            _validate_version(version.raw,
                              import_url,
                              parsed_imported_dsl_holder)
        _merge_parsed_into_combined(
            holder_result, parsed_imported_dsl_holder, version, namespace)
    holder_result.value[version_key_holder] = version_value_holder
    return used_imports, holder_result


def _build_ordered_imports(parsed_dsl_holder,
                           dsl_location,
                           resources_base_path,
                           resolver):

    def location(value):
        return value or constants.ROOT_ELEMENT_VALUE

    def is_parsed_resource(item):
        """
        Checking if the given item is in parsed yaml type.
        """
        return isinstance(item, Holder)

    def validate_namespace(namespace):
        """
        The namespace delimiter is not allowed in the namespace.
        """
        if namespace and constants.NAMESPACE_DELIMITER in namespace:
            raise exceptions.DSLParsingLogicException(
                212,
                'Invalid {0}: import\'s namespace cannot'
                'contain namespace delimiter'.format(namespace))

    def is_cloudify_basic_types(imported_holder):
        """
        Cloudify basic types can be recognized with the following rules
        at high assurance:
        - Has a node type named cloudify.nodes.Root.
        - Has a metadata field named cloudify_types.
        """
        _, metadata = imported_holder.get_item(constants.METADATA)
        _, node_types = imported_holder.get_item(constants.NODE_TYPES)
        if ((metadata and metadata.get_item('cloudify_types')[1]) or
                (node_types and
                 node_types.get_item('cloudify.nodes.Root')[1])):
            return True
        return False

    def validate_import(namespace, cloudify_basic_types):
        """
        Cloudify basic types can not be imported with namespace,
        due to that will disrupt core Cloudify types with the namespace prefix
        which will not allow proper functioning, like: added a prefix to
        install workflow.
        """
        if cloudify_basic_types and namespace:
            raise exceptions.DSLParsingLogicException(
                214,
                'Invalid import namespace {} cannot be used'
                ' on Cloudify basic types'.format(namespace))

    def build_ordered_imports_recursive(_current_parsed_dsl_holder,
                                        _current_import,
                                        context_namespace=None):
        imports_key_holder, imports_value_holder = _current_parsed_dsl_holder.\
            get_item(constants.IMPORTS)
        if not imports_value_holder:
            return

        for another_import in imports_value_holder.restore():
            namespace, import_url = _extract_import_parts(another_import,
                                                          resources_base_path,
                                                          _current_import)
            validate_namespace(namespace)
            current_namespace = namespace
            if context_namespace:
                if namespace:
                    current_namespace = utils.generate_namespaced_value(
                        context_namespace,
                        namespace)
                else:
                    # In case a namespace was added earlier in the import
                    # chain.
                    current_namespace = context_namespace
            if import_url is None:
                ex = exceptions.DSLParsingLogicException(
                    13, "Import failed: no suitable location found for "
                        "import '{0}'".format(another_import))
                ex.failed_import = another_import
                raise ex

            import_key = (import_url, current_namespace)
            import_context = (location(_current_import), context_namespace)
            if import_key in imports_graph:
                is_cloudify_types = imports_graph[import_key]['cloudify_types']
                if is_cloudify_types:
                    current_namespace = None
                imports_graph.add_graph_dependency(
                    import_url,
                    import_context,
                    current_namespace)
            else:
                imported_dsl = resolver.fetch_import(import_url)
                if not is_parsed_resource(imported_dsl):
                    imported_dsl = utils.load_yaml(
                        raw_yaml=imported_dsl,
                        error_message="Failed to parse import '{0}'"
                                      "(via '{1}')"
                                      .format(another_import, import_url),
                        filename=import_url)
                cloudify_basic_types = is_cloudify_basic_types(imported_dsl)
                validate_import(namespace, cloudify_basic_types)
                if cloudify_basic_types:
                    current_namespace = None
                imports_graph.add(
                    import_url,
                    imported_dsl,
                    cloudify_basic_types,
                    import_context,
                    current_namespace)
                build_ordered_imports_recursive(imported_dsl,
                                                import_url,
                                                current_namespace)

    imports_graph = ImportsGraph()
    imports_graph.add(location(dsl_location), parsed_dsl_holder)
    build_ordered_imports_recursive(parsed_dsl_holder, dsl_location)
    return imports_graph.topological_sort()


def _validate_version(dsl_version,
                      import_url,
                      parsed_imported_dsl_holder):
    version_key_holder, version_value_holder = parsed_imported_dsl_holder\
        .get_item(_version.VERSION)
    if version_value_holder and version_value_holder.value != dsl_version:
        raise exceptions.DSLParsingLogicException(
            28, "An import uses a different "
                "tosca_definitions_version than the one defined in "
                "the main blueprint's file: main blueprint's file "
                "version is '{0}', import with different version is "
                "'{1}', version of problematic import is '{2}'"
                .format(dsl_version,
                        import_url,
                        version_value_holder.value))


def _merge_parsed_into_combined(combined_parsed_dsl_holder,
                                parsed_imported_dsl_holder,
                                version,
                                namespace):
    merge_no_override = MERGE_NO_OVERRIDE.copy()
    if version['definitions_version'] > (1, 2):
        merge_no_override.update(MERGEABLE_FROM_DSL_VERSION_1_3)
    for key_holder, value_holder in parsed_imported_dsl_holder.value.\
            items():
        if key_holder.value in IGNORE:
            pass
        elif key_holder.value not in combined_parsed_dsl_holder:
            if isinstance(value_holder.value, dict) and namespace:
                # Propagate namespace down
                for v in value_holder.value.values():
                    v.namespace = namespace
            combined_parsed_dsl_holder.value[key_holder] = value_holder
        elif key_holder.value in DONT_OVERWRITE:
            pass
        elif key_holder.value in merge_no_override:
            _, to_dict = combined_parsed_dsl_holder.get_item(key_holder.value)
            _merge_into_dict_or_throw_on_duplicate(
                from_dict_holder=value_holder,
                to_dict_holder=to_dict,
                key_name=key_holder.value,
                namespace=namespace)
        else:
            if key_holder.value in MERGEABLE_FROM_DSL_VERSION_1_3:
                msg = ("Import failed: non-mergeable field: '{0}'. "
                       "{0} can be imported multiple times only from "
                       "cloudify_dsl_1_3 and above.")
            else:
                msg = "Import failed: non-mergeable field: '{0}'"
            raise exceptions.DSLParsingLogicException(
                3, msg.format(key_holder.value))


def _merge_namespaced_elements(key_holder, namespace, value_holder):
    if isinstance(value_holder.value, dict):
        for v in value_holder.value.values():
            v.namespace = namespace
    elif isinstance(value_holder.value, basestring):
        # In case of primitive type we a need a different way to mark
        # the sub elements with the namespace, but leaving the option
        # for the DSL element to not receive the namespace.
        value_holder.only_children_namespace = True
        value_holder.namespace = namespace
    if not utils.check_if_cloudify_type(key_holder.value):
        key_holder.value = utils.generate_namespaced_value(
            namespace, key_holder.value)


def _merge_into_dict_or_throw_on_duplicate(from_dict_holder,
                                           to_dict_holder,
                                           key_name,
                                           namespace):
    for key_holder, value_holder in from_dict_holder.value.items():
        if key_holder.value not in to_dict_holder or\
                to_dict_holder.value[key_holder].namespace != namespace:
            if namespace:
                _merge_namespaced_elements(key_holder, namespace, value_holder)
            to_dict_holder.value[key_holder] = value_holder
        else:
            raise exceptions.DSLParsingLogicException(
                4, "Import failed: Could not merge '{0}' due to conflict "
                   "on '{1}'".format(key_name, key_holder.value))


class ImportsGraph(object):

    def __init__(self):
        self._imports_tree = nx.DiGraph()
        self._imports_graph = nx.DiGraph()

    def add(self, import_url, parsed, cloudify_types=False,
            via_import=None, namespace=None):
        node_key = (import_url, namespace)
        if import_url not in self._imports_tree:
            self._imports_tree.add_node(
                node_key, parsed=parsed, cloudify_types=cloudify_types)
            self._imports_graph.add_node(
                node_key, parsed=parsed, cloudify_types=cloudify_types)
        if via_import:
            self._imports_tree.add_edge(node_key, via_import)
            self._imports_graph.add_edge(node_key, via_import)

    def add_graph_dependency(self, import_url, via_import, namespace):
        if via_import:
            self._imports_graph.add_edge((import_url, namespace), via_import)

    def topological_sort(self):
        return reversed(list(
            ({'import': i,
             'parsed': self._imports_tree.node[i]['parsed']}
             for i in nx.topological_sort(self._imports_tree))))

    def __contains__(self, item):
        return item in self._imports_tree

    def __getitem__(self, item):
        return self._imports_tree.node[item]
