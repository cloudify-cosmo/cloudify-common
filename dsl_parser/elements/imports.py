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
from collections import deque, OrderedDict
from typing import Iterable, Tuple, Optional

from urllib.request import pathname2url

from cloudify.exceptions import InvalidBlueprintImport

from dsl_parser import (exceptions,
                        constants,
                        version as _version,
                        utils)
from dsl_parser.holder import Holder
from dsl_parser.import_resolver.abstract_import_resolver import\
    is_remote_resource, AbstractImportResolver
from dsl_parser.framework.elements import (Dict,
                                           DictElement,
                                           Element,
                                           Leaf,
                                           List)
from dsl_parser.utils import get_function


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
    constants.CAPABILITIES,
    constants.BLUEPRINT_LABELS,
    constants.LABELS,
    constants.RESOURCE_TAGS,
]

DONT_OVERWRITE = set([
    constants.DESCRIPTION,
    constants.METADATA
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


class ImportPluginProperty(Element):
    schema = Leaf(type=(str, int, float, bool, list, dict))

    requires = {
        'inputs': ['validate_version', 'version'],
    }

    def validate(self, version, validate_version, **kwargs):
        if validate_version:
            self.validate_version(version.definitions_version, (1, 5))
        if isinstance(self.initial_value, (str, int, float, bool, list)):
            return
        if isinstance(self.initial_value, dict) \
                and get_function(self.initial_value):
            return
        raise exceptions.DSLParsingFormatException(
            exceptions.ERROR_INVALID_IMPORT_SPECS,
            'Properties of imported node should either be strings, integers, '
            'floats, booleans, lists or intrinsic functions')


class ImportPluginProperties(DictElement):
    schema = Dict(type=ImportPluginProperty)

    requires = {
        'inputs': ['validate_version', 'version'],
    }

    def validate(self, version, validate_version, **kwargs):
        if validate_version:
            self.validate_version(version.definitions_version, (1, 5))


class ImportLoader(Element):

    schema = [Leaf(type=str), Dict(type=ImportPluginProperties)]


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

    def validate(self, **kwargs):
        imports = [i.value for i in self.children()]
        imports_set = set()
        for _import in imports:
            if isinstance(_import, str):
                import_key = _import
            elif isinstance(_import, dict):
                import_key = _validate_import_dict(_import)
            else:
                raise exceptions.DSLParsingFormatException(
                    exceptions.ERROR_INVALID_IMPORT_SPECS,
                    'The import statement should be either a string '
                    'or a dictionary'
                )
            if import_key in imports_set:
                raise exceptions.DSLParsingFormatException(
                    2, 'Duplicate imports')
            imports_set.add(import_key)

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
        parsed_blueprint = _combine_imports(
            parsed_dsl_holder=main_blueprint_holder,
            dsl_location=blueprint_location,
            resources_base_path=resources_base_path,
            version=version,
            resolver=resolver,
            validate_version=validate_version)
        return parsed_blueprint

    def calculate_provided(self, **kwargs):
        return {
            'resource_base': self.resource_base
        }


def _dsl_location_to_url(dsl_location, resources_base_path):
    if dsl_location is not None:
        dsl_location = next(
            _get_resource_location(dsl_location, resources_base_path))
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
                           current_resource_context=None,
                           dsl_version=None):
    if is_remote_resource(resource_name):
        yield resource_name
        return

    for fn in _possible_resource_locations(resource_name, dsl_version):
        if os.path.exists(fn):
            yield f'file:{pathname2url(os.path.abspath(fn))}'

    if current_resource_context:
        candidate_url = current_resource_context[
            :current_resource_context.rfind('/') + 1] + resource_name
        yield candidate_url

    if resources_base_path:
        full_path = os.path.join(resources_base_path, resource_name)
        for fn in _possible_resource_locations(full_path, dsl_version):
            if os.path.exists(fn):
                yield f'file:{pathname2url(os.path.abspath(fn))}'

    for fn in _possible_resource_locations(resource_name, dsl_version):
        yield f'resource:{fn}'


def _possible_resource_locations(resource_name, dsl_version):
    if not dsl_version:
        return [resource_name]
    filename, ext = os.path.splitext(resource_name)
    return ['{0}_{1}{2}'.format(filename, dsl_version, ext), resource_name]


def _insert_imported_list(blueprint_holder, blueprints_imported):
    blueprint_holder.set_item(
        constants.IMPORTED_BLUEPRINTS,
        list(blueprints_imported.values())
    )
    blueprint_holder.set_item(
        constants.NAMESPACES_MAPPING,
        blueprints_imported,
    )


def _combine_imports(parsed_dsl_holder, dsl_location,
                     resources_base_path, version, resolver,
                     validate_version):
    ordered_imports, mapping = _build_ordered_imports(
        parsed_dsl_holder,
        dsl_location,
        resources_base_path,
        resolver)
    holder_result = parsed_dsl_holder.copy()
    version_key_holder, version_value_holder = parsed_dsl_holder.get_item(
        _version.VERSION)
    holder_result.value = {}
    _insert_imported_list(holder_result, mapping)
    for imported in ordered_imports:
        parsed_imported_dsl_holder = imported.parsed
        if validate_version:
            _validate_version(version.raw,
                              imported.url,
                              parsed_imported_dsl_holder)
        if imported.url != constants.ROOT_ELEMENT_VALUE:
            _validate_and_set_properties(
                parsed_imported_dsl_holder, imported.properties)
        is_cloudify_types = imported.is_cloudify_types
        _merge_parsed_into_combined(
            holder_result, parsed_imported_dsl_holder,
            version, imported.namespace, is_cloudify_types)
    holder_result.value[version_key_holder] = version_value_holder
    return holder_result


def _dsl_version(parsed_dsl_holder):
    version = parsed_dsl_holder.get_item('tosca_definitions_version')
    if version:
        version = version[1].value
    if version.startswith('cloudify_dsl_'):
        return version.replace('cloudify_dsl_', '', 1)


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
    - Has a metadata field named cloudify_types.
    - Has a node type named cloudify.nodes.Root (for backward capability).
    """
    _, metadata = imported_holder.get_item(constants.METADATA)
    _, node_types = imported_holder.get_item(constants.NODE_TYPES)
    if (metadata and metadata.get_item('cloudify_types')[1]) or node_types:
        root_type = node_types.get_item('cloudify.nodes.Root')[1]
        if root_type and not root_type.is_cloudify_type:
            # If it was already marked with the flag,
            # this means that this blueprint is using Cloudify basic types
            # and not equal to it.
            return True
    return False


def validate_import_namespace(namespace,
                              cloudify_basic_types,
                              context_namespace):
    """
    Cloudify basic types can not be imported with namespace,
    due to that will disrupt core Cloudify types with the namespace prefix
    which will not allow proper functioning, like: added a prefix to
    install workflow.
    """
    if cloudify_basic_types and namespace:
        if not context_namespace or namespace != context_namespace:
            raise exceptions.DSLParsingLogicException(
                214,
                'Invalid import namespace {0}: cannot be used'
                ' on Cloudify basic types.'.format(namespace))


def validate_blueprint_import_namespace(namespace, import_url):
    """
    Blueprint import must be used with namespace, this is enforced for
    enabling the use of scripts from the imported blueprint which needs
    the namespace for maintaining the path to the scripts.
    """
    if not namespace:
        raise exceptions.DSLParsingLogicException(
            213,
            'Invalid {0}: blueprint import cannot'
            'be used without namespace'.format(import_url))


def _normalize_plugin_import(plugin, imported_dsl, namespace):
    utils.remove_dsl_keys(
        imported_dsl,
        constants.PLUGIN_DSL_KEYS_NOT_FROM_YAML)
    for key in constants.PLUGIN_DSL_KEYS_READ_FROM_DB:
        if not plugin.get(key):
            continue
        value = plugin[key]
        if key in constants.PLUGIN_DSL_KEYS_ADD_VALUES_NODE:
            value = utils.add_values_node_description(value)
        _merge_into_dict_or_throw_on_duplicate(
            Holder.of({key: value}),
            imported_dsl,
            key,
            namespace)


class _ImportedDSL:
    """Represents one imported DSL file.

    This is only useful when fetching the imports, in order to keep track
    of which ones were already downloaded, and to traverse the import tree.
    """
    url: str
    parsed: Optional[Holder]
    namespace: Optional[str]
    properties: Optional[dict]

    def __init__(
        self,
        url,
        parsed=None,
        namespace=None,
        properties=None,
    ):
        self.url = url
        self.parsed = parsed
        self.namespace = namespace
        self.properties = properties

    @property
    def key(self) -> Tuple[str, Optional[str]]:
        return (self.url, self.namespace)

    @property
    def dsl_version(self) -> str:
        return _dsl_version(self.parsed)

    @property
    def is_cloudify_types(self) -> bool:
        return is_cloudify_basic_types(self.parsed)

    def get_imports(self) -> Iterable[Tuple[str, Optional[dict]]]:
        _, imports_value_holder = self.parsed.get_item(constants.IMPORTS)
        if not imports_value_holder:
            return
        for raw_import in imports_value_holder.restore():
            if isinstance(raw_import, dict):
                # This is actually guaranteed to be a dict of length 1
                yield list(raw_import.items())[0]
            else:
                yield raw_import, None


def _prefix_namespace(parent_namespace, namespace):
    validate_namespace(namespace)
    if parent_namespace and namespace:
        return utils.generate_namespaced_value(parent_namespace, namespace)
    else:
        return parent_namespace or namespace


def _split_import_namespace(import_url):
    namespace, _, resolved_url = \
        import_url.rpartition(constants.NAMESPACE_DELIMITER)
    if namespace:
        validate_namespace(namespace)
    else:
        namespace = None
    return namespace, resolved_url


def _fetch_import(
    original_import_url: str,
    resolver: AbstractImportResolver,
    parent: _ImportedDSL,
    resources_base_path: str,
    properties: dict,
    dsl_version: str,
    namespaces_mapping: dict,
    already_imported: dict,
) -> Optional[dict]:
    """Fetch and parse a single import

    Based on the import url, and all the additional required details,
    return the parsed DSL (or None if this DSL was already imported before).
    If the url cannot be resolved, an error is raised.
    """
    namespace, resolved_url = \
        _split_import_namespace(original_import_url)
    namespace = _prefix_namespace(parent.namespace, namespace)
    import_urls = _get_resource_location(
        resolved_url,
        resources_base_path,
        parent.url,
        dsl_version,
    )

    fetch_error = None
    for import_url in import_urls:
        next_item = _ImportedDSL(
            url=import_url,
            namespace=namespace,
            properties=properties,
        )
        if next_item.key in already_imported:
            # this was already seen! let's just check the namespace
            validate_import_namespace(
                namespace,
                already_imported[next_item.key].is_cloudify_types,
                parent.namespace,
            )
            return None

        if utils.is_blueprint_import(import_url):
            validate_blueprint_import_namespace(namespace, import_url)
            blueprint_id = \
                utils.remove_blueprint_import_prefix(import_url)
            if namespaces_mapping.get(namespace):
                raise exceptions.DSLParsingLogicException(
                    214,
                    f'Import failed {namespace}: can not use the same'
                    'namespace for importing blueprints'
                )
            namespaces_mapping[namespace] = blueprint_id

        # here we actually fetch and parse the DSL
        try:
            imported_dsl = resolver.fetch_import(
                import_url,
                dsl_version=dsl_version,
            )
        except Exception as exc:
            fetch_error = exc
            continue

        if not is_parsed_resource(imported_dsl):
            imported_dsl = utils.load_yaml(
                raw_yaml=imported_dsl,
                error_message="Failed to parse import '{0}'"
                              "(via '{1}')"
                              .format(original_import_url, import_url),
                filename=import_url)

        # if this was a plugin import, the DSL needs to be massaged a bit...
        try:
            plugin = resolver.retrieve_plugin(
                import_url,
                dsl_version=dsl_version,
            )
        except InvalidBlueprintImport:
            plugin = None
        if plugin:
            # If it is a plugin, then use labels and tags from the DB
            _normalize_plugin_import(plugin, imported_dsl, namespace)

        if utils.is_blueprint_import(import_url):
            namespaces_mapping[namespace] = blueprint_id

        next_item.parsed = imported_dsl
        validate_import_namespace(next_item.namespace,
                                  next_item.is_cloudify_types,
                                  parent.namespace)
        if next_item.is_cloudify_types:
            # Remove namespace data from import
            next_item.namespace = None
        return next_item

    if fetch_error:
        # if we weren't able to fetch the imported dsl, and we did see an
        # error in fetching, reraise that
        raise fetch_error
    ex = exceptions.DSLParsingLogicException(
        13, "Import failed: no suitable location found for "
            "import '{0}'".format(original_import_url))
    ex.failed_import = original_import_url
    raise ex


def _build_ordered_imports(parsed_dsl_holder,
                           dsl_location,
                           resources_base_path,
                           resolver):
    namespaces_mapping = {}
    root_dsl = _ImportedDSL(url=dsl_location, parsed=parsed_dsl_holder)

    # this is an OrderedDict and not just a list,
    ordered_imports = OrderedDict([
        (root_dsl.key, root_dsl),
    ])

    # BFS over the imports. Necessarily, the traversal order will be such
    # that parent imports come before child imports (i.e. if A import B,
    # A comes before B)
    to_visit = deque([root_dsl])
    while to_visit:
        parent = to_visit.popleft()

        for original_import_url, properties in parent.get_imports():
            next_item = _fetch_import(
                original_import_url,
                resolver,
                parent,
                resources_base_path,
                properties,
                root_dsl.dsl_version,
                namespaces_mapping,
                ordered_imports,
            )

            if next_item:
                ordered_imports[next_item.key] = next_item
                to_visit.append(next_item)

    return ordered_imports.values(), namespaces_mapping


def _validate_version(dsl_version,
                      import_url,
                      parsed_imported_dsl_holder):
    version_key_holder, version_value_holder = parsed_imported_dsl_holder\
        .get_item(_version.VERSION)
    if version_value_holder and \
            not _can_import_version(dsl_version, version_value_holder.value):
        raise exceptions.DSLParsingLogicException(
            28, "An import uses a different "
                "tosca_definitions_version than the one defined in "
                "the main blueprint's file: main blueprint's file "
                "version is '{0}', import with different version is "
                "'{1}', version of problematic import is '{2}'"
                .format(dsl_version,
                        import_url,
                        version_value_holder.value))


def _validate_and_set_properties(parsed_imported_dsl_holder, properties):
    _, plugins = parsed_imported_dsl_holder.get_item('plugins')
    if not plugins:
        return
    for plugin_name in plugins.keys():
        _, plugin = plugins.get_item(plugin_name)
        if plugin:
            _validate_and_set_plugin_properties(plugin, properties)


def _validate_and_set_plugin_properties(plugin_dsl_holder, properties):
    _, plugin_properties = plugin_dsl_holder.get_item('properties')
    if not plugin_properties:
        return
    invalid_types = []
    redundant_properties = list(properties.keys()) if properties else []
    for property_name in plugin_properties.keys():
        _, plugin_property = plugin_properties.get_item(property_name)
        _, property_type = plugin_property.get_item('type')
        if properties and property_name in properties:
            redundant_properties.remove(property_name)
            if _is_type_valid(properties[property_name], property_type.value):
                plugin_property.set_item('value', properties[property_name])
            elif get_function(properties[property_name]):
                plugin_property.set_item('value', properties[property_name])
            else:
                invalid_types.append(
                    f"Property {property_name}: value "
                    f"'{properties[property_name]}' should be of type "
                    f"{property_type.value}"
                )
    errors = []
    if invalid_types:
        errors.extend(invalid_types)
    if redundant_properties:
        errors.append(
            'Properties for imported plugin are redundant: '
            f'{redundant_properties}, consider updating import lines'
        )
    if errors:
        raise exceptions.DSLParsingLogicException(
            exceptions.ERROR_INVALID_IMPORT_PROPERTIES,
            '.  '.join(errors)
            )


def _is_type_valid(obj, type_name):
    if type_name == 'string':
        return isinstance(obj, (str, int))
    elif type_name == 'integer':
        return isinstance(obj, int)
    elif type_name == 'float':
        return isinstance(obj, (int, float))
    elif type_name == 'boolean':
        return isinstance(obj, bool)
    elif type_name == 'list':
        return isinstance(obj, list)
    else:
        raise exceptions.DSLParsingFormatException(
            exceptions.ERROR_INVALID_IMPORT_SPECS,
            'Properties should either be strings, integers, '
            'floats, booleans, or lists.')


def _can_import_version(version_orig, version_imported):
    """Accept importing a file which uses equal or earlier DSL version."""
    return _version.SUPPORTED_VERSIONS.index(version_orig) >= \
        _version.SUPPORTED_VERSIONS.index(version_imported)


def _mark_key_value_holder_items(value_holder, field_name, field_value):
    for v in value_holder.value.values():
        setattr(v, field_name, field_value)


def _merge_lists_with_no_duplicates(from_dict_holder, to_dict_holder):
    for value_holder in from_dict_holder.value:
        if value_holder not in to_dict_holder.value:
            to_dict_holder.value.append(value_holder)


def _insert_new_element(target_holder, key_holder, value_holder, namespace):
    if isinstance(value_holder.value, dict) and namespace:
        # At this level (second level) in the blueprint there are only dict
        # or string, and namespacing is only applied to dict elements.
        new_element = Holder({})
        _merge_into_dict_or_throw_on_duplicate(
            from_dict_holder=value_holder,
            to_dict_holder=new_element,
            key_name=key_holder.value,
            namespace=namespace)
        target_holder.value[key_holder] = new_element
    else:
        target_holder.value[key_holder] = value_holder


def _merge_parsed_into_combined(combined_parsed_dsl_holder,
                                parsed_imported_dsl_holder,
                                version,
                                namespace,
                                is_cloudify_types):
    merge_no_override = MERGE_NO_OVERRIDE.copy()
    if version['definitions_version'] > (1, 2):
        merge_no_override.update(MERGEABLE_FROM_DSL_VERSION_1_3)
    for key_holder, value_holder in parsed_imported_dsl_holder.value.items():
        if is_cloudify_types and isinstance(value_holder.value, dict):
            _mark_key_value_holder_items(value_holder,
                                         'is_cloudify_type',
                                         is_cloudify_types)
        if key_holder.value in IGNORE:
            pass
        elif key_holder.value == constants.IMPORTED_BLUEPRINTS:
            _, to_dict = combined_parsed_dsl_holder.get_item(key_holder.value)
            _merge_lists_with_no_duplicates(value_holder, to_dict)
        elif key_holder.value == constants.NAMESPACES_MAPPING:
            _, to_dict = combined_parsed_dsl_holder.get_item(key_holder.value)
            _merge_into_dict_or_throw_on_duplicate(
                from_dict_holder=value_holder,
                to_dict_holder=to_dict,
                key_name=key_holder.value,
                namespace=namespace)
        elif key_holder.value not in combined_parsed_dsl_holder:
            _insert_new_element(combined_parsed_dsl_holder,
                                key_holder,
                                value_holder,
                                namespace)
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


def _prepare_namespaced_elements(key_holder, namespace, value_holder):
    if isinstance(value_holder.value, dict):
        _mark_key_value_holder_items(value_holder, 'namespace', namespace)
    elif isinstance(value_holder.value, str):
        # In case of primitive type we a need a different way to mark
        # the sub elements with the namespace, but leaving the option
        # for the DSL element to not receive the namespace.
        value_holder.only_children_namespace = True

        value_holder.namespace = namespace
    if not utils.check_if_overridable_cloudify_type(key_holder.value):
        key_holder.value = utils.generate_namespaced_value(
            namespace, key_holder.value)


def _extend_list_with_namespaced_values(namespace,
                                        from_list_holder,
                                        to_list_holder):
    if namespace:
        for v in from_list_holder.value:
            v.namespace = from_list_holder.namespace
    to_list_holder.value.extend(from_list_holder.value)


def _merge_node_templates_relationships(
        key_holder, key_name, to_dict_holder, from_dict_holder):
    def only_relationships_inside(element_holder):
        return (constants.RELATIONSHIPS in element_holder and
                len(element_holder.value) == 1)

    if constants.RELATIONSHIPS in to_dict_holder:
        only_relationships_in_source = only_relationships_inside(
            from_dict_holder)
        required_node_template_field = 'type'

        if (len(to_dict_holder.value) == 1 and
                (required_node_template_field in from_dict_holder or
                 only_relationships_in_source)):
            # If the current parsed yaml contains only relationships element,
            # the user can only extend with more relationships or merge it
            # with required field to a set a node template.
            _extend_node_template(from_dict_holder, to_dict_holder)
            return
        elif only_relationships_in_source:
            _extend_list_with_namespaced_values(
                from_dict_holder[constants.RELATIONSHIPS].namespace,
                from_dict_holder[constants.RELATIONSHIPS],
                to_dict_holder[constants.RELATIONSHIPS])
            return
    raise exceptions.DSLParsingLogicException(
        4, "Import failed: Could not merge '{0}' due to conflict "
           "on '{1}'".format(key_name, key_holder.value))


def _extend_node_template(from_dict_holder, to_dict_holder):
    for key_holder, value_holder in from_dict_holder.value.items():
        if (isinstance(value_holder.value, dict) or
                isinstance(value_holder.value, str)):
            to_dict_holder.value[key_holder] = value_holder
        elif (isinstance(value_holder.value, list) and
              key_holder.value == constants.RELATIONSHIPS):
            _extend_list_with_namespaced_values(
                value_holder.namespace,
                value_holder,
                to_dict_holder.value[key_holder])


def _merge_workflows(key_holder, key_name, to_dict_holder, from_dict_holder):
    source = from_dict_holder.value
    target = to_dict_holder.value
    for key in from_dict_holder.value:
        if key not in target:
            target[key] = source[key]


def _merge_into_dict_or_throw_on_duplicate(from_dict_holder,
                                           to_dict_holder,
                                           key_name,
                                           namespace):
    def cloudify_type(element_key_holder, element_value_holder):
        """
        If the element (key+value) is marked with Cloudify basic type flag
        or the user overridden one.
        """
        return (element_value_holder.is_cloudify_type or
                utils.check_if_overridable_cloudify_type(
                    element_key_holder.value))

    for key_holder, value_holder in from_dict_holder.value.items():
        if namespace and not value_holder.is_cloudify_type:
            _prepare_namespaced_elements(key_holder, namespace, value_holder)
        _, to_value_holder = to_dict_holder.get_item(key_holder.value)
        if not to_value_holder or cloudify_type(key_holder, value_holder):
            # If it's a new value or cloudify basic type which can be
            # overwritten because it's the same.
            to_dict_holder.value[key_holder] = value_holder
        elif key_name == constants.NODE_TEMPLATES:
            _merge_node_templates_relationships(
                key_holder,
                key_name,
                to_dict_holder.value[key_holder],
                value_holder)
        elif key_name in constants.PLUGIN_DSL_KEYS_READ_FROM_DB:
            # Allow blueprint_labels, labels and resource_tags to be merged
            continue
        elif key_name == constants.WORKFLOWS:
            _merge_workflows(
                key_holder,
                key_name,
                to_dict_holder.value[key_holder],
                value_holder,
            )
        else:
            raise exceptions.DSLParsingLogicException(
                4, "Import failed: Could not merge '{0}' due to conflict "
                   "on '{1}'".format(key_name, key_holder.value))


def _validate_import_dict(_import):
    if len(_import) != 1:
        raise exceptions.DSLParsingFormatException(
            exceptions.ERROR_INVALID_IMPORT_SPECS,
            'Import with properties should be a single-entry dictionary')
    return list(_import.keys())[0]
