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
import contextlib
import importlib
import numbers
import sys
import re

from networkx import NetworkXError, NetworkXUnfeasible

import yaml.parser

from urllib.error import URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from dsl_parser.constants import (
    RESOLVER_IMPLEMENTATION_KEY,
    RESLOVER_PARAMETERS_KEY,
    NAMESPACE_DELIMITER
)
from dsl_parser.import_resolver.default_import_resolver import (
    DefaultImportResolver
)
from dsl_parser import (yaml_loader,
                        constants,
                        exceptions)


TEMPLATE_FUNCTIONS = {}


class ResolverInstantiationError(Exception):
    pass


def merge_schemas(overridden_schema,
                  overriding_schema,
                  data_types):
    merged = overriding_schema.copy()
    for key, overridden_property in overridden_schema.items():
        if key not in overriding_schema:
            merged[key] = overridden_property
        else:
            overriding_property = overriding_schema[key]
            overriding_type = overriding_property.get('type')
            overridden_type = overridden_property.get('type')
            overridden_default = overridden_property.get('default')
            overriding_initial_default = overriding_property.get(
                'initial_default')
            if (overriding_type is not None and
                overriding_type == overridden_type and
                overriding_type in data_types and
                overriding_type not in constants.USER_PRIMITIVE_TYPES and
                    overridden_default is not None):
                if overriding_initial_default is None:
                    overriding_initial_default = {}
                default_value = parse_value(
                    value=overriding_initial_default,
                    derived_value=overridden_default,
                    type_name=overridden_type,
                    data_types=data_types,
                    undefined_property_error_message='illegal state',
                    missing_property_error_message='illegal state',
                    node_name='illegal state',
                    path=[],
                    raise_on_missing_property=False)
                if default_value:
                    merged[key]['default'] = default_value
    return merged


def flatten_schema(schema):
    flattened_schema_props = {}
    for prop_key, prop in schema.items():
        if 'default' in prop:
            flattened_schema_props[prop_key] = prop['default']
    return flattened_schema_props


def _property_description(path, name=None):
    if not path:
        return name
    if name is not None:
        path = copy.copy(path)
        path.append(name)
    return '.'.join(path)


def merge_schema_and_instance_properties(
        instance_properties,
        schema_properties,
        data_types,
        undefined_property_error_message,
        missing_property_error_message,
        node_name,
        path=None,
        raise_on_missing_property=True):
    flattened_schema_props = flatten_schema(schema_properties)
    return _merge_flattened_schema_and_instance_properties(
        instance_properties=instance_properties,
        schema_properties=schema_properties,
        flattened_schema_properties=flattened_schema_props,
        data_types=data_types,
        undefined_property_error_message=undefined_property_error_message,
        missing_property_error_message=missing_property_error_message,
        node_name=node_name,
        path=path,
        raise_on_missing_property=raise_on_missing_property)


def _merge_flattened_schema_and_instance_properties(
        instance_properties,
        schema_properties,
        flattened_schema_properties,
        data_types,
        undefined_property_error_message,
        missing_property_error_message,
        node_name,
        path,
        raise_on_missing_property):
    path = path or []

    # validate instance properties don't
    # contain properties that are not defined
    # in the schema.
    for key in instance_properties:
        if key not in schema_properties:
            ex = exceptions.DSLParsingLogicException(
                exceptions.ERROR_UNDEFINED_PROPERTY,
                undefined_property_error_message.format(
                    node_name,
                    _property_description(path, key)))
            ex.property = key
            raise ex

    merged_properties = dict(flattened_schema_properties)
    merged_properties.update(instance_properties)

    use_external_resource = merged_properties.get('use_external_resource')
    if use_external_resource:
        if get_function(use_external_resource):
            raise exceptions.DSLParsingLogicException(
                exceptions.ERROR_INTRINSIC_FUNCTION_NOT_PERMITTED,
                "Only boolean values are allowed for `use_external_resource` "
                "property, don't use intrinsic functions.")

    result = {}
    for key, property_schema in schema_properties.items():
        if key not in merged_properties:
            required = property_schema.get('required', True) and \
                       not use_external_resource
            if required and raise_on_missing_property:
                ex = exceptions.DSLParsingLogicException(
                    exceptions.ERROR_MISSING_PROPERTY,
                    missing_property_error_message.format(
                        node_name,
                        _property_description(path, key)))
                ex.property = key
                raise ex
            else:
                continue
        prop_path = copy.copy(path)
        prop_path.append(key)
        result[key] = parse_value(
            value=merged_properties.get(key),
            derived_value=flattened_schema_properties.get(key),
            type_name=property_schema.get('type'),
            data_types=data_types,
            undefined_property_error_message=undefined_property_error_message,
            missing_property_error_message=missing_property_error_message,
            node_name=node_name,
            path=prop_path,
            raise_on_missing_property=raise_on_missing_property)
    return result


def parse_simple_type_value(value, type_name):
    if type_name == 'integer':
        if isinstance(value, numbers.Integral) and not isinstance(value, bool):
            return value, True
    elif type_name == 'float':
        if isinstance(value, numbers.Number) and not isinstance(
                value, bool):
            return value, True
    elif type_name == 'boolean':
        if isinstance(value, bool):
            return value, True
    elif type_name in ('string', 'textarea', 'deployment_id', 'blueprint_id',
                       'node_id', 'node_type', 'node_instance',
                       'capability_value', 'scaling_group', 'secret_key',
                       'operation_name', ):
        return value, True
    elif type_name == 'regex':
        if isinstance(value, str):
            try:
                re.compile(value)
                return value, True
            except re.error:
                return None, False
    elif type_name == 'list':
        if isinstance(value, (list, tuple)):
            return value, True
    elif type_name == 'dict':
        if isinstance(value, dict):
            return value, True
    return None, False


def parse_value(
        value,
        type_name,
        data_types,
        undefined_property_error_message,
        missing_property_error_message,
        node_name,
        path,
        derived_value=None,
        raise_on_missing_property=True):
    if type_name is None:
        return value
    elif get_function(value):
        # intrinsic function - not validated at the moment
        return value
    elif type_name in constants.USER_PRIMITIVE_TYPES:
        result, found = parse_simple_type_value(value, type_name)
        if found:
            return result
    elif type_name in data_types:
        if isinstance(value, dict):
            data_schema = data_types[type_name]['properties']
            flattened_data_schema = flatten_schema(data_schema)
            if isinstance(derived_value, dict):
                flattened_data_schema.update(derived_value)
            undef_msg = undefined_property_error_message
            return _merge_flattened_schema_and_instance_properties(
                instance_properties=value,
                schema_properties=data_schema,
                flattened_schema_properties=flattened_data_schema,
                data_types=data_types,
                undefined_property_error_message=undef_msg,
                missing_property_error_message=missing_property_error_message,
                node_name=node_name,
                path=path,
                raise_on_missing_property=raise_on_missing_property)
    else:
        raise RuntimeError(
            "Unexpected type defined in property schema for property '{0}'"
            " - unknown type is '{1}'".format(
                _property_description(path),
                type_name))

    prop_path = _property_description(path)
    if not prop_path:
        err_msg = "Property type validation failed in '{0}': the defined " \
                  "type is '{1}', yet it was assigned with the " \
                  "value '{2}'".format(node_name, type_name, value)
    else:
        err_msg = "Property type validation failed in '{0}': property " \
                  "'{1}' type is '{2}', yet it was assigned with the " \
                  "value '{3}'".format(node_name,
                                       _property_description(path),
                                       type_name,
                                       value)

    raise exceptions.DSLParsingLogicException(
        exceptions.ERROR_VALUE_DOES_NOT_MATCH_TYPE, err_msg)


def cast_to_type(value, type_name):
    """Try converting value to the specified type_name if possible."""

    if not isinstance(value, str):
        return value

    try:
        if type_name == 'integer':
            return int(value)
        if type_name == 'float':
            return float(value)
        if type_name == 'boolean':
            if value == 'true':
                return True
            if value == 'false':
                return False
    except ValueError:
        pass

    return value


def load_yaml(raw_yaml, error_message, filename=None):
    try:
        return yaml_loader.load(raw_yaml, filename)
    except yaml.parser.ParserError as ex:
        raise exceptions.DSLParsingFormatException(-1,
                                                   '{0}: Illegal yaml; {1}'
                                                   .format(error_message, ex))


def url_exists(url):
    try:
        request = Request(url)
        with contextlib.closing(urlopen(request)):
            return True
    except (ValueError, URLError):
        return False


def is_valid_url(url):
    # Checks whether a given string represents a valid
    # URL (syntax-wise).
    return urlparse(url).scheme != ''


def create_import_resolver(resolver_configuration):
    if resolver_configuration:
        resolver_class_path = resolver_configuration.get(
            RESOLVER_IMPLEMENTATION_KEY)
        parameters = resolver_configuration.get(RESLOVER_PARAMETERS_KEY, {})
        if parameters and not isinstance(parameters, dict):
            raise ResolverInstantiationError(
                'Invalid parameters supplied for the resolver ({0}): '
                'parameters must be a dictionary and not {1}'
                .format(resolver_class_path or 'DefaultImportResolver',
                        type(parameters).__name__))
        try:
            if resolver_class_path:
                # custom import resolver
                return get_class_instance(resolver_class_path, parameters)
            else:
                # default import resolver
                return DefaultImportResolver(**parameters)
        except Exception as ex:
            raise ResolverInstantiationError(
                'Failed to instantiate resolver ({0}). {1}'
                .format(resolver_class_path or 'DefaultImportResolver',
                        str(ex)))
    return DefaultImportResolver()


def get_class_instance(class_path, properties):
    """Returns an instance of a class from a string formatted as module:class
    the given *args, **kwargs are passed to the instance's __init__"""
    if not properties:
        properties = {}
    try:
        cls = get_class(class_path)
        instance = cls(**properties)
    except Exception as e:
        exc_type, exc, traceback = sys.exc_info()
        raise RuntimeError(f'Failed to instantiate {class_path}, error: {e}')\
            .with_traceback(traceback)

    return instance


def get_class(class_path):
    """Returns a class from a string formatted as module:class"""
    if not class_path:
        raise ValueError('class path is missing or empty')

    if not isinstance(class_path, str):
        raise ValueError('class path is not a string')

    class_path = class_path.strip()
    if ':' not in class_path or class_path.count(':') > 1:
        raise ValueError('Invalid class path, expected format: '
                         'module:class')

    class_path_parts = class_path.split(':')
    class_module_str = class_path_parts[0].strip()
    class_name = class_path_parts[1].strip()

    if not class_module_str or not class_name:
        raise ValueError('Invalid class path, expected format: '
                         'module:class')

    module = importlib.import_module(class_module_str)
    if not hasattr(module, class_name):
        raise ValueError('module {0}, does not contain class {1}'
                         .format(class_module_str, class_name))

    return getattr(module, class_name)


def generate_namespaced_value(namespace, value):
    return "{0}{1}{2}".format(namespace, NAMESPACE_DELIMITER, value)


def find_namespace_location(value):
    return value.find(NAMESPACE_DELIMITER)


def remove_namespace(value):
    value_namespace_format = '.*{0}'.format(NAMESPACE_DELIMITER)
    return re.sub(value_namespace_format, '', value)


def check_if_overridable_cloudify_type(type_name):
    """
    Checking if the type name has Cloudify prefix, which marks all
    the Cloudify basic types that users can override.
    For example: Install workflow should not be overridden.
    """
    return type_name.startswith(constants.CLOUDIFY_TYPE_PREFIX)


def is_blueprint_import(import_url):
    return import_url.startswith(constants.BLUEPRINT_IMPORT)


def is_plugin_import(import_url):
    return import_url.startswith(constants.PLUGIN_PREFIX)


def remove_blueprint_import_prefix(import_url):
    return import_url.replace(constants.BLUEPRINT_IMPORT, '')


def find_suffix_matches_in_list(sub_str, items):
    return [item for item in items if item.endswith(sub_str)]


def remove_dsl_keys(dsl_holder, keys_to_remove):
    """Remove `keys_to_remove` keys from `dsl_holder`."""
    for key in list(dsl_holder.value.keys()):
        if key.value in keys_to_remove:
            del dsl_holder.value[key]


def add_values_node_description(data):
    result = {}
    for k, v in data.items():
        result[k] = {'values': v}
    return result


def get_function(value):
    """Get a template function if the value represents it"""
    # functions use the syntax {function_name: args}, or
    # {function_name: args, 'type': type_name} so let's look for
    # dicts of length 1 where the only key was registered as a function
    # or length of 2 where the other key is 'type'
    if not isinstance(value, dict):
        return None
    if not 0 < len(value) < 3:
        return None
    result = None
    for k, v in value.items():
        if k == 'type':
            continue
        elif k in TEMPLATE_FUNCTIONS:
            result = (TEMPLATE_FUNCTIONS[k], v)
        else:
            return None
    return result


def topological_sort(G, nbunch=None, reverse=False):
    """Return a list of nodes in topological sort order (networkx==1.x algo)

    This is a copy of topological_sort function from networkx 1.11.  The new
    implementation (networkx==2.x.y) is not backward compatible:
      - it does not support nbunch parameter (esp. not in case len(nbunch) > 1)
      - the results are slightly different and not always deterministic, hence
        taking the first of the sorted elements does not behave as we would
        expect it to

    https://github.com/networkx/networkx/blob/cabefb75b1e116fc57d5b30e5d93b217d68b3a2e/networkx/algorithms/dag.py#L88-L168
    """
    if not G.is_directed():
        raise NetworkXError(
            "Topological sort not defined on undirected graphs.")

    # nonrecursive version
    seen = set()
    order = []
    explored = set()

    if nbunch is None:
        nbunch = G.nodes
    for v in nbunch:     # process all vertices in G
        if v in explored:
            continue
        fringe = [v]   # nodes yet to look at
        while fringe:
            w = fringe[-1]  # depth first search
            if w in explored:  # already looked down this branch
                fringe.pop()
                continue
            seen.add(w)     # mark as seen
            # Check successors for cycles and for new nodes
            new_nodes = []
            for n in G[w]:
                if n not in explored:
                    if n in seen:  # CYCLE !!
                        raise NetworkXUnfeasible("Graph contains a cycle.")
                    new_nodes.append(n)
            if new_nodes:   # Add new_nodes to fringe
                fringe.extend(new_nodes)
            else:           # No new nodes so w is fully explored
                explored.add(w)
                order.append(w)
                fringe.pop()    # done considering this node
    if reverse:
        return order
    else:
        return list(reversed(order))
