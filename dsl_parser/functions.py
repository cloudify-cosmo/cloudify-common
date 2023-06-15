#########
# Copyright (c) 2017-2019 Cloudify Platform Ltd. All rights reserved
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
#  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  * See the License for the specific language governing permissions and
#  * limitations under the License.

import abc
import collections
import json
import pkg_resources
from typing import Any, Dict, Union

from functools import wraps

from dsl_parser import exceptions, scan, constants
from dsl_parser.constants import (OUTPUTS,
                                  CAPABILITIES,
                                  NODE_INSTANCES,
                                  EVAL_FUNCS_PATH_PREFIX_KEY,
                                  EVAL_FUNCS_PATH_DEFAULT_PREFIX)
from dsl_parser.utils import (TEMPLATE_FUNCTIONS,
                              get_function,
                              parse_simple_type_value)


SELF = 'SELF'
SOURCE = 'SOURCE'
TARGET = 'TARGET'
AVAILABLE_NODE_TARGETS = [SELF, SOURCE, TARGET]

# Function eval types: Static cannot be evaluated at runtime,
# Hybrid can be evaluated both at runtime and statically,
# Runtime can be evaluated only at runtime.
STATIC_FUNC, HYBRID_FUNC, RUNTIME_FUNC = [0, 1, 2]


class RecursionLimit(object):
    def __init__(self, limit, args_to_str_func=None):
        """Initializes the recursion limit context manager. This context
        manager limits the recursion only for this specific function, unlike
        the sys.setrecursionlimit(limit) function, which sets the call stack
        depth.

        :param limit: recursion limit (inclusive).
        :param args_to_str_func: function that returns a string containing
            information about the last arguments used. Its signature should
            be the same as the signature of the limited function.
        """
        self.limit = limit
        self.args_to_str_func = args_to_str_func
        self.last_args = None
        self.last_kwargs = None
        self.calls_cnt = 0

    def set_last_args(self, last_args, last_kwargs):
        """Sets the last arguments that were used to call the function wrapped
        in this context manager.

        :param last_args: last arguments to set.
        :param last_kwargs: last keyword arguments to set.
        """
        self.last_args = last_args
        self.last_kwargs = last_kwargs

    def __enter__(self):
        self.calls_cnt += 1
        if self.calls_cnt > self.limit:
            msg = "The recursion limit ({0}) has been reached while " \
                  "evaluating the deployment. ".format(self.limit)
            if self.args_to_str_func and (self.last_args or self.last_kwargs):
                msg += self.args_to_str_func(
                    *self.last_args, **self.last_kwargs)
            raise exceptions.EvaluationRecursionLimitReached(msg)

    def __exit__(self, tp, value, tb):
        self.calls_cnt -= 1


def limit_recursion(limit, args_to_str_func=None):
    _recursion_limit_ctx = RecursionLimit(limit, args_to_str_func)

    def decorator(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            _recursion_limit_ctx.set_last_args(args, kwargs)
            with _recursion_limit_ctx:
                return f(*args, **kwargs)

        return wrapper
    return decorator


def register(fn=None, name=None, func_eval_type=HYBRID_FUNC):
    if fn is None:
        def partial(_fn):
            return register(_fn, name, func_eval_type)
        return partial
    TEMPLATE_FUNCTIONS[name] = fn
    fn.name = name
    fn.func_eval_type = func_eval_type
    return fn


def unregister(name):
    if name in TEMPLATE_FUNCTIONS:
        del TEMPLATE_FUNCTIONS[name]


def _register_entry_point_functions():
    for entry_point in pkg_resources.iter_entry_points(
            group='cloudify.tosca.ext.functions'):
        register(fn=entry_point.load(), name=entry_point.name)


def _convert_attribute_list_to_python_syntax_string(attr_list):
    """This converts the given attribute list to Python syntax. For example,
    calling this function with ['obj1', 'attr1', 7, 'attr2'] will output
    obj1.attr1[7].attr2.

    :param attr_list: the requested attributes/indices list.
    :return: string representing the requested attributes in Python syntax.
    """
    if not attr_list:
        return ''
    s = str(attr_list[0])
    for attr in attr_list[1:]:
        if isinstance(attr, int):
            s += '[{0}]'.format(attr)
        else:
            s += '.{0}'.format(attr)
    return s


_register_entry_point_functions()


def _contains_legal_nested_attribute_path_items(args):
    return all(get_function(x) or isinstance(x, (str, int))
               for x in args)


def _is_legal_nested_attribute_path(args):
    return isinstance(args, list) \
        and len(args) >= 2 \
        and _contains_legal_nested_attribute_path_items(args)


class Function(abc.ABC):
    name = 'function'
    func_eval_type = None

    def __init__(self, args, scope=None, context=None, path=None, raw=None,
                 return_type=None):
        """Initializes the instance.

        :param args: function argument. For Static functions, these arguments
            should be already evaluated by the time "evaluate" is called. For
            Runtime functions, the same is true for "evaluate_runtime". For
            Hybrid functions, it is true for both "evaluate" and
            "evaluate_runtime".
        :param scope: scope of the operation (string).
        :param context: scanner context (i.e. actual node template).
        :param path: current property path.
        :param raw: actual raw instance of the function, as parsed from the
            blueprint.
        """
        self.scope = scope
        self.context = context
        self.path = path
        self.raw = raw
        self.return_type = return_type
        self.parse_args(args)

    @abc.abstractmethod
    def parse_args(self, args):
        pass

    @abc.abstractmethod
    def validate(self, plan):
        pass

    @abc.abstractmethod
    def evaluate(self, handler):
        pass


@register(name='get_input', func_eval_type=HYBRID_FUNC)
class GetInput(Function):

    def __init__(self, args, **kwargs):
        self.input_value = None
        super(GetInput, self).__init__(args, **kwargs)

    def parse_args(self, args):
        def _is_valid_args_list(args_list):
            return isinstance(args_list, list) \
                and len(args_list) >= 1 \
                and _contains_legal_nested_attribute_path_items(args_list)

        if not isinstance(args, str) \
                and not get_function(args) \
                and not _is_valid_args_list(args):
            raise ValueError(
                "Illegal argument(s) passed to {0} function. Expected either "
                "a string, or a function, or a list [input_name\\function "
                "[, key1\\index1\\function [,...]]] "
                "but got {1}".format(self.name, args))
        self.input_value = args

    def validate(self, plan):
        input_value = self.input_value[0] \
            if isinstance(self.input_value, list) else self.input_value
        if get_function(input_value):
            return
        if input_value not in plan.inputs:
            raise exceptions.UnknownInputError(
                "get_input function references an "
                "unknown input '{0}'.".format(input_value))

    def evaluate(self, handler):
        if isinstance(self.input_value, list):
            if any(get_function(x) for x in self.input_value):
                raise exceptions.FunctionEvaluationError(
                    '{0}: found an unresolved argument: {1}'
                    .format(self.name, self.input_value))

            return_value = self._get_input_attribute(
                handler.get_input(self.input_value[0]))
        else:
            if get_function(self.input_value):
                raise exceptions.FunctionEvaluationError(
                    '{0}: found an unresolved argument: {1}'
                    .format(self.name, self.input_value))
            return_value = handler.get_input(self.input_value)

        if self.return_type:
            _, ok = parse_simple_type_value(return_value, self.return_type)
            if not ok:
                raise exceptions.InputEvaluationError(
                    "Value '{0}' of input '{1}' does not match data type "
                    "declared for '{2}': '{3}'."
                    .format(return_value, self.input_value, self.path,
                            self.return_type))
        return return_value

    def _get_input_attribute(self, root):
        value = root
        for index, attr in enumerate(self.input_value[1:]):
            if isinstance(value, dict):
                if attr not in value:
                    raise exceptions.InputEvaluationError(
                        "Input attribute '{0}' of '{1}', "
                        "doesn't exist.".format(
                            attr,
                            _convert_attribute_list_to_python_syntax_string(
                                self.input_value[:index + 1])))

                value = value[attr]
            elif isinstance(value, list):
                try:
                    value = value[attr]
                except TypeError:
                    raise exceptions.InputEvaluationError(
                        "Item in index {0} in the get_input arguments list "
                        "[{1}] is expected to be an int but got {2}.".format(
                            index,
                            ', '.join('{0}'.format(item)
                                      for item in self.input_value),
                            type(attr).__name__)
                    )
                except IndexError:
                    raise exceptions.InputEvaluationError(
                        "List size of '{0}' is {1} but index {2} is "
                        "retrieved.".format(
                            _convert_attribute_list_to_python_syntax_string(
                                self.input_value[:index + 1]),
                            len(value),
                            attr))
            else:
                raise exceptions.FunctionEvaluationError(
                    self.name,
                    "Object {0} has no attribute {1}".format(
                        _convert_attribute_list_to_python_syntax_string(
                            self.input_value[:index + 1]), attr))
        return value


@register(name='get_sys', func_eval_type=RUNTIME_FUNC)
class GetSys(Function):
    VALID_PROPERTIES = [('tenant', 'name'),
                        ('deployment', 'id'),
                        ('deployment', 'name'),
                        ('deployment', 'blueprint'),
                        ('deployment', 'owner')]

    def __init__(self, args, **kwargs):
        self.entity = None
        self.property = None
        super(GetSys, self).__init__(args, **kwargs)

    def parse_args(self, args):
        if isinstance(args, list) \
                and len(args) >= 2 \
                and _contains_legal_nested_attribute_path_items(args):
            self.entity, self.property = args[0], args[1]
        else:
            raise ValueError(
                "Illegal argument(s) passed to {0} function. Expected a "
                "2-element list identifying an entity and its requested "
                "property (e.g. [tenant, name] or [deployment, id]) "
                "but got {1}".format(self.name, args))

    def validate(self, plan):
        known_entities = set(p[0] for p in self.VALID_PROPERTIES)
        if get_function(self.entity):
            return
        if not isinstance(self.entity, str) \
                or self.entity not in known_entities:
            raise exceptions.UnknownSysEntityError(
                "{0} function unable to determine entity: {1}"
                .format(self.name, self.entity))
        if get_function(self.property):
            return
        if not isinstance(self.property, str) \
                or (self.entity, self.property) not in self.VALID_PROPERTIES:
            raise exceptions.UnknownSysPropertyError(
                "{0} function unable to determine property: {1} {2}"
                .format(self.name, self.entity, self.property))

    def evaluate(self, handler):
        return handler.get_sys(self.entity, self.property)


@register(name='get_consumers', func_eval_type=RUNTIME_FUNC)
class GetConsumers(Function):
    VALID_PROPERTIES = ['ids', 'names', 'count']

    def __init__(self, args, **kwargs):
        self.property = None
        super(GetConsumers, self).__init__(args, **kwargs)

    def parse_args(self, args):
        if isinstance(args, str) and args in self.VALID_PROPERTIES:
            self.property = args
        else:
            raise ValueError(
                "Illegal argument passed to {name} function. Expected a "
                "property string (one of: {props}), but got {args}".format(
                    name=self.name, props=', '.join(self.VALID_PROPERTIES),
                    args=args))

    def validate(self, plan):
        pass

    def evaluate(self, handler):
        return handler.get_consumers(self.property)


@register(name='get_property', func_eval_type=HYBRID_FUNC)
class GetProperty(Function):

    def __init__(self, args, **kwargs):
        self.node_name = None
        self.property_path = None
        super(GetProperty, self).__init__(args, **kwargs)

    def parse_args(self, args):
        if not _is_legal_nested_attribute_path(args):
            raise ValueError(
                'Illegal arguments passed to {0} function. Expected: '
                '[ node_name\\function, property_name\\function '
                '[, nested-property-1\\function, ... ] '
                'but got: {1}.'.format(self.name, args))
        self.node_name = args[0]
        self.property_path = args[1:]

    def validate(self, plan):
        # Try to evaluate only when the arguments don't contain any functions
        args = [self.node_name] + self.property_path
        if any(get_function(x) for x in args):
            return
        handler = _PlanEvaluationHandler(plan, runtime_only_evaluation=False)
        self.evaluate(handler)

    def get_node_template(self, handler):
        if self.node_name == SELF:
            if self.scope != scan.NODE_TEMPLATE_SCOPE:
                raise ValueError(
                    '{0} can only be used in a context of node template but '
                    'appears in {1}.'.format(SELF, self.scope))
            node = self.context
        elif self.node_name in [SOURCE, TARGET]:
            if self.scope != scan.NODE_TEMPLATE_RELATIONSHIP_SCOPE:
                raise ValueError(
                    '{0} can only be used within a relationship but is used '
                    'in {1}'.format(self.node_name, self.path))
            if self.node_name == SOURCE:
                # this is messy, see CY-1676 to fix it
                node = self.context.get('node_template') or \
                    handler.get_node(self.context['source_node'])
            else:
                target_node = self.context\
                    .get('relationship', {}).get('target_id')
                target_node = target_node or self.context['target_node']
                node = handler.get_node(target_node)
        else:
            node = handler.get_node(self.node_name)
            # we're getting a different node - switch the context to that, so
            # that the EvaluationHandler can use the new node as the context,
            # in case there's another get_property call in the result
            self.context = node
        self._get_property_value(node, handler)
        return node

    def _get_property_value(self, node_template, handler):
        return _get_property_value(node_template['name'],
                                   node_template.get('properties', {}),
                                   self.property_path,
                                   handler,
                                   self.context,
                                   self.path,
                                   func_name='get_property')

    def evaluate(self, handler):
        if get_function(self.node_name) or \
                any(get_function(x) for x in self.property_path):
            raise exceptions.FunctionEvaluationError(
                '{0}: found an unresolved argument in path: {1}, {2}'
                .format(self.name, self.node_name, self.property_path))
        return self._get_property_value(self.get_node_template(handler),
                                        handler)


@register(name='get_attribute', func_eval_type=RUNTIME_FUNC)
class GetAttribute(Function):

    def __init__(self, args, **kwargs):
        self.node_name = None
        self.attribute_path = None
        super(GetAttribute, self).__init__(args, **kwargs)

    def parse_args(self, args):
        if not _is_legal_nested_attribute_path(args):
            raise ValueError(
                'Illegal arguments passed to {0} function. Expected: '
                '[ node_name\\function, attribute_name\\function '
                '[, nested-attribute-1\\function, ... ] '
                'but got: {1}.'.format(self.name, args))
        self.node_name = args[0]
        self.attribute_path = args[1:]

    def validate(self, plan):
        _validate_no_functions_in_args(self.node_name, self.attribute_path,
                                       self.path, self.name, self.scope, plan)

    def evaluate(self, handler):
        try:
            node_instance = self._resolve_node_instance(handler)
        except exceptions.FunctionEvaluationError as ex:
            # Only in outputs scope we allow to continue when an error
            # occurred
            if not self.context.get('evaluate_outputs'):
                raise
            return '<ERROR: {0}>'.format(ex)

        node = handler.get_node(node_instance['node_id'])
        if self.context.get('self') and \
                self.context['self'] != node_instance['id']:
            self.context = self.context.copy()
            self.context['self'] = node_instance['id']
        value = _get_attribute_from_node_instance(
            node_instance, node, self.attribute_path, self.path,
            handler, self.context, 'get_attribute')
        return value

    def _resolve_node_instance(self, handler) -> Dict[str, Any]:
        if 'instance_id_hint' in self.context:
            # we're evaluating a node in the context of a specific instance,
            # and we were told exactly which instance is it, that is, unless
            # we're retrieving an attribute of another node, in which case we
            # should ignore the hint
            node_instance = handler.get_node_instance(
                self.context['instance_id_hint'])
            if node_instance['node_id'] == self.node_name:
                return node_instance

        if self.node_name in [SELF, SOURCE, TARGET]:
            node_instance_id = self._resolve_available_node_targets(handler)
            _validate_ref(node_instance_id, self.node_name, self.name,
                          self.path, self.attribute_path)
            return handler.get_node_instance(node_instance_id)

        return self._resolve_node_instance_by_name(handler)

    def _resolve_available_node_targets(self, handler):
        node_instance_id = self.context.get(self.node_name.lower())
        if node_instance_id:
            return node_instance_id

        if self.node_name == SELF:
            node_instance = self._resolve_node_instance_by_name(
                handler, node_id=self.context.get('id'))
            if node_instance:
                return node_instance.get('id')

    def _resolve_node_instance_by_name(self, handler, node_id=None):
        node_id = node_id or self.node_name
        node_instances = handler.get_node_instances(node_id)

        if len(node_instances) == 0:
            raise exceptions.FunctionEvaluationError(
                self.name,
                'Node {0} has no instances.'.format(self.node_name)
            )

        if len(node_instances) == 1:
            return node_instances[0]

        node_instance = self._try_resolve_node_instance_by_relationship(
            handler=handler,
            node_instances=node_instances)
        if node_instance:
            return node_instance

        node_instance = self._try_resolve_node_instance_by_scaling_group(
            handler=handler,
            node_instances=node_instances)
        if node_instance:
            return node_instance

        raise exceptions.FunctionEvaluationError(
            self.name,
            'More than one node instance found for node "{0}". Cannot '
            'resolve a node instance unambiguously.'
            .format(self.node_name))

    def _try_resolve_node_instance_by_relationship(
            self, handler, node_instances):
        self_instance_id = self.context.get('self')
        if not self_instance_id:
            return None
        self_instance = handler.get_node_instance(self_instance_id)
        self_instance_relationships = self_instance.get('relationships') or []
        node_instances_target_ids = set()
        for relationship in self_instance_relationships:
            if relationship['target_name'] == self.node_name:
                node_instances_target_ids.add(relationship['target_id'])
        if len(node_instances_target_ids) != 1:
            return None
        node_instance_target_id = node_instances_target_ids.pop()
        for node_instance in node_instances:
            if node_instance['id'] == node_instance_target_id:
                return node_instance
        else:
            raise RuntimeError('Illegal state')

    def _try_resolve_node_instance_by_scaling_group(
            self, handler, node_instances):

        def _parent_instance(_instance):
            _node = handler.get_node(_instance['node_id'])
            for relationship in _node.get('relationships') or []:
                if (constants.CONTAINED_IN_REL_TYPE in
                        relationship['type_hierarchy']):
                    target_name = relationship['target_id']
                    target_id = [
                        r['target_id'] for r in
                        _instance.get('relationships') or []
                        if r['target_name'] == target_name][0]
                    return handler.get_node_instance(target_id)
            return None

        def _containing_groups(_instance):
            result = [g['name'] for g in _instance.get('scaling_groups') or []]
            parent_instance = _parent_instance(_instance)
            if parent_instance:
                result += _containing_groups(parent_instance)
            return result

        def _minimal_shared_group(instance_a, instance_b):
            a_containing_groups = _containing_groups(instance_a)
            b_containing_groups = _containing_groups(instance_b)
            shared_groups = set(a_containing_groups) & set(b_containing_groups)
            if not shared_groups:
                return None
            for group in a_containing_groups:
                if group in shared_groups:
                    return group
            else:
                raise RuntimeError('Illegal state')

        def _group_instance(node_instance, group_name):
            for scaling_group in (node_instance['scaling_groups'] or []):
                if scaling_group['name'] == group_name:
                    return scaling_group['id']
            parent_instance = _parent_instance(node_instance)
            if not parent_instance:
                raise RuntimeError('Illegal state')
            return _group_instance(parent_instance, group_name)

        def _resolve_node_instance(context_instance_id):
            context_instance = handler.get_node_instance(context_instance_id)
            minimal_shared_group = _minimal_shared_group(context_instance,
                                                         node_instances[0])
            if not minimal_shared_group:
                return None

            context_group_instance = _group_instance(context_instance,
                                                     minimal_shared_group)
            result_node_instances = [
                i for i in node_instances if
                _group_instance(i, minimal_shared_group) ==
                context_group_instance]

            if len(result_node_instances) == 1:
                return result_node_instances[0]

            return None

        self_instance_id = self.context.get('self')
        source_instance_id = self.context.get('source')
        target_instance_id = self.context.get('target')
        if self_instance_id:
            return _resolve_node_instance(self_instance_id)
        elif source_instance_id:
            node_instance = _resolve_node_instance(source_instance_id)
            if node_instance:
                return node_instance
            node_instance = _resolve_node_instance(target_instance_id)
            if node_instance:
                return node_instance

        return None


def _validate_ref(ref, ref_name, name, path, attribute_path):
    if not ref:
        raise exceptions.FunctionEvaluationError(
            name,
            '{0} is missing in request context in {1} for '
            'attribute {2}'.format(ref_name,
                                   path,
                                   attribute_path))


def _get_attribute_from_node_instance(ni, node, attribute_path, path,
                                      handler, context, fn_name):
    value = _get_property_value(ni['node_id'],
                                ni.get('runtime_properties'),
                                attribute_path,
                                handler,
                                context,
                                path,
                                raise_if_not_found=False,
                                func_name=fn_name)

    if value is None and len(attribute_path) == 1:
        # specialcase some get_attribute parameters:
        # - node_instance_id returns the node instance ID
        # - node_instance_index returns the node instance index
        if attribute_path[0] == 'node_instance_id':
            value = ni['id']
        elif attribute_path[0] == 'node_instance_index':
            value = ni['index']
    # still nothing? look in node properties
    if value is None:
        value = _get_property_value(node['id'],
                                    node.get('properties', {}),
                                    attribute_path,
                                    handler,
                                    context,
                                    path,
                                    raise_if_not_found=False,
                                    func_name=fn_name)
    return value


def _validate_no_functions_in_args(node_name, attribute_path, path, name,
                                   scope, plan):
    # If any of the arguments are functions don't validate
    args = [node_name] + attribute_path
    if any(get_function(x) for x in args):
        return

    if node_name in [SELF, SOURCE, TARGET]:
        scope_invalidity = {
            scan.OUTPUTS_SCOPE: [SELF, SOURCE, TARGET],
            scan.NODE_TEMPLATE_RELATIONSHIP_SCOPE: [SELF],
            scan.NODE_TEMPLATE_SCOPE: [SOURCE, TARGET],
        }
        if node_name in scope_invalidity[scope]:
            raise ValueError(
                '{0} cannot be used with {1} function in {2}'.format(
                    node_name, name, path,
                )
            )
    else:
        if not any(node_name == template['id']
                   for template in plan.node_templates):
            raise KeyError(
                "{0} function node reference '{1}' does not exist.".format(
                    name, node_name))


@register(name='get_attributes_list', func_eval_type=RUNTIME_FUNC)
class GetAttributesList(Function):

    def __init__(self, args, **kwargs):
        self.node_name = None
        self.attribute_path = None
        super(GetAttributesList, self).__init__(args, **kwargs)

    def parse_args(self, args):
        if not _is_legal_nested_attribute_path(args):
            raise ValueError(
                'Illegal arguments passed to {0} function. Expected: '
                '[ node_name\\function, attribute_name\\function '
                '[, nested-attribute-1\\function, ... ] '
                'but got: {1}.'.format(self.name, args))
        self.node_name = args[0]
        self.attribute_path = args[1:]

    def validate(self, plan):
        _validate_no_functions_in_args(self.node_name, self.attribute_path,
                                       self.path, self.name, self.scope, plan)

    def evaluate(self, handler):
        if self.node_name in [SELF, SOURCE, TARGET]:
            node_id = self.context.get(self.node_name.lower())
            _validate_ref(node_id, self.node_name, self.name,
                          self.path, self.attribute_path)
        else:
            node_id = self.node_name
        node = handler.get_node(node_id)
        node_instances = handler.get_node_instances(node_id)

        results = []

        for ni in node_instances:
            results.append(_get_attribute_from_node_instance(
                ni, node, self.attribute_path, self.path,
                handler, self.context, 'get_attributes_list'
            ))

        return results


@register(name='get_attributes_dict', func_eval_type=RUNTIME_FUNC)
class GetAttributesDict(Function):

    def __init__(self, args, **kwargs):
        self.node_name = None
        self.attribute_path = None
        super(GetAttributesDict, self).__init__(args, **kwargs)

    def parse_args(self, args):
        self.node_name = args[0]
        self.attribute_paths = args[1:]
        for pos, attribute_path in enumerate(self.attribute_paths):
            # These must all be lists due to the way the attribute_path
            # processing functions work
            if not isinstance(attribute_path, list):
                self.attribute_paths[pos] = [attribute_path]

        for attribute_path in self.attribute_paths:
            check = [self.node_name] + attribute_path
            if not _is_legal_nested_attribute_path(check):
                raise ValueError(
                    'Illegal arguments passed to {0} function. Expected: '
                    '[ node_name\\function, list of attribute_name\\function '
                    'or [attribute_name\\function, '
                    'nested-attribute-1\\function], ... ] '
                    'but got: {1}.'.format(self.name, args))

        duplicate_or_ambiguous_check = collections.Counter([
            self._convert_to_identifier(attribute_path)
            for attribute_path in self.attribute_paths
        ])
        duplicated_or_ambiguous = [
            item for item in duplicate_or_ambiguous_check
            if duplicate_or_ambiguous_check[item] > 1
        ]
        if duplicated_or_ambiguous:
            raise exceptions.FunctionEvaluationError(
                'One or more duplicated or ambiguous attribute paths found: '
                '{}'.format(', '.join(duplicated_or_ambiguous))
            )

    def validate(self, plan):
        _validate_no_functions_in_args(self.node_name, self.attribute_paths,
                                       self.path, self.name, self.scope, plan)

    def evaluate(self, handler):
        if self.node_name in [SELF, SOURCE, TARGET]:
            node_id = self.context.get(self.node_name.lower())
            _validate_ref(node_id, self.node_name, self.name,
                          self.path, self.attribute_paths)
        else:
            node_id = self.node_name
        node = handler.get_node(node_id)
        node_instances = handler.get_node_instances(node_id)

        results = {}

        for ni in node_instances:
            ni_result = {}
            for attribute_path in self.attribute_paths:
                identifier = self._convert_to_identifier(attribute_path)
                ni_result[identifier] = _get_attribute_from_node_instance(
                    ni, node, attribute_path, self.path,
                    handler, self.context, 'get_attributes_dict'
                )
            results[ni['id']] = ni_result

        return results

    def _convert_to_identifier(self, attribute_path):
        if isinstance(attribute_path, list):
            return '.'.join(attribute_path)
        return attribute_path


@register(name='get_secret', func_eval_type=RUNTIME_FUNC)
class GetSecret(Function):
    def __init__(self, args, **kwargs):
        self.secret_id = None
        super(GetSecret, self).__init__(args, **kwargs)

    def parse_args(self, args):
        if (
            not (
                (isinstance(args, list) and len(args) > 1)
                or isinstance(args, str)
                or get_function(args)
            )
        ):
            raise exceptions.FunctionValidationError(
                "`get_secret` function argument should be a list with at "
                "least 2 elements, a string, or a dict (a function). Instead "
                "it is a {0} with the value: {1}.".format(type(args), args))
        self.secret_id = args

    def validate(self, plan):
        pass

    def evaluate(self, handler):
        if isinstance(self.secret_id, list):
            secret = handler.get_secret(self.secret_id[0])
            try:
                value = json.loads(secret)
            except ValueError as err:
                raise exceptions.FunctionEvaluationError(
                    'Could not parse secret "{secret_name}" as json. '
                    'Error was: {err}'.format(
                        secret_name=self.secret_id[0],
                        err=err,
                    )
                )
            if len(self.secret_id) > 1:
                for i in self.secret_id[1:]:
                    try:
                        value = value[i]
                    except (KeyError, TypeError, IndexError) as err:
                        raise exceptions.FunctionEvaluationError(
                            'Could not find "{key}" in nested lookup on '
                            'secret "{secret}". Error was: {err}'.format(
                                key=i,
                                secret=self.secret_id[0],
                                err=err,
                            )
                        )
            return value
        else:
            return handler.get_secret(self.secret_id)


@register(name='concat', func_eval_type=HYBRID_FUNC)
class Concat(Function):

    def __init__(self, args, **kwargs):
        self.separator = ''
        self.joined = args
        super(Concat, self).__init__(args, **kwargs)

    def parse_args(self, args):
        if not isinstance(args, list):
            raise ValueError(
                'Illegal arguments passed to {0} function. '
                'Expected: [arg1\\function1, arg2\\function2, ...]'
                'but got: {1}.'.format(self.name, args))

    def validate(self, plan):
        if plan.version.definitions_version < (1, 1):
            raise exceptions.FunctionValidationError(
                'Using {0} requires using dsl version 1_1 or '
                'greater, but found: {1} in {2}.'
                .format(self.name, plan.version, self.path))

    def evaluate(self, handler):
        for joined_value in self.joined:
            if parse(joined_value) != joined_value:
                return self.raw
        return self.join()

    def join(self):
        str_join = [str(elem) for elem in self.joined]
        return self.separator.join(str_join)


@register(name='merge', func_eval_type=HYBRID_FUNC)
class MergeDicts(Function):

    def __init__(self, args, **kwargs):
        self.merged = args
        super(MergeDicts, self).__init__(args, **kwargs)

    def parse_args(self, args):
        if not isinstance(args, list):
            raise ValueError(
                'Illegal arguments passed to {0} function. '
                'Expected: [arg1\\function1, arg2\\function2, ...]'
                'but got: {1}.'.format(self.name, args))

    def validate(self, plan):
        if plan.version.definitions_version < (1, 3):
            raise exceptions.FunctionValidationError(
                'Using {0} requires using dsl version 1_3 or '
                'greater, but found: {1} in {2}.'
                .format(self.name, plan.version, self.path))
        for merged_item in self.merged:
            if not isinstance(merged_item, dict):
                raise exceptions.FunctionValidationError(
                    'Encountered non-dict element for merge: {0}'
                    .format(merged_item))

    def evaluate(self, handler):
        for merged_value in self.merged:
            if parse(merged_value) != merged_value:
                return self.raw
        return self.join()

    def join(self):
        merged = {}
        for cur_dict in self.merged:
            merged.update(cur_dict)
        return merged


class InterDeploymentDependencyCreatingFunction(Function):
    @abc.abstractproperty
    def target_deployment(self):
        pass

    @property
    def function_identifier(self):
        return '{0}.{1}'.format(self.path, self.name)


@register(name='get_capability', func_eval_type=RUNTIME_FUNC)
class GetCapability(InterDeploymentDependencyCreatingFunction):
    def __init__(self, args, **kwargs):
        self.capability_path = None
        super(GetCapability, self).__init__(args, **kwargs)

    def parse_args(self, args):
        if not isinstance(args, list):
            raise ValueError(
                "`get_capability` function argument should be a list. Instead "
                "it is a {0} with the value: {1}.".format(type(args), args))
        if len(args) < 2:
            raise ValueError(
                "`get_capability` function argument should be a list with 2 "
                "elements at least - [ deployment ID, capability ID "
                "[, key/index[, key/index [...]]] ]. Instead it is: [{0}]"
                .format(','.join('{0}'.format(a) for a in args))
            )
        for arg_index, arg in enumerate(args):
            if not isinstance(arg, (str, int)) and not get_function(arg):
                raise ValueError(
                    "`get_capability` function arguments can't be complex "
                    "values; only strings/ints/functions are accepted. "
                    "Instead, the item with index {0} is {1} of type {2}"
                    .format(arg_index, arg, type(arg))
                )

        self.capability_path = args

    def evaluate(self, handler):
        return handler.get_capability(self.capability_path)

    def validate(self, plan):
        pass

    @property
    def target_deployment(self):
        first_arg = self.capability_path[0]
        target_deployment = None if get_function(first_arg) else first_arg
        return target_deployment, first_arg


@register(name='get_group_capability', func_eval_type=RUNTIME_FUNC)
class GetGroupCapability(Function):
    def __init__(self, args, **kwargs):
        self.capability_path = None
        super(GetGroupCapability, self).__init__(args, **kwargs)

    def parse_args(self, args):
        if not isinstance(args, list):
            raise ValueError(
                "`get_group_capability` function argument should be a list. "
                "Instead it is a {0} with the value: {1}."
                .format(type(args), args))
        if len(args) < 2:
            raise ValueError(
                "`get_group_capability` function argument should be a list "
                "with 2 elements at least - [ group ID, capability ID "
                "[, key/index[, key/index [...]]] ]. Instead it is: [{0}]"
                .format(','.join('{0}'.format(a) for a in args))
            )
        for arg_index, arg in enumerate(args):
            if arg_index == 1 and isinstance(arg, list):
                continue
            if not isinstance(arg, (str, int)) and not get_function(arg):
                raise ValueError(
                    "`get_group_capability` function arguments can't be "
                    "complex values; only strings/ints/functions are "
                    "accepted. Instead, the item with index {0} is {1} "
                    "of type {2}".format(arg_index, arg, type(arg))
                )
        self.capability_path = args

    def evaluate(self, handler):
        return handler.get_group_capability(self.capability_path)

    def validate(self, plan):
        pass


@register(name='get_label', func_eval_type=RUNTIME_FUNC)
class GetLabel(Function):
    def __init__(self, args, **kwargs):
        self.label_key = None
        self.values_list_index = None
        super(GetLabel, self).__init__(args, **kwargs)

    def parse_args(self, args):
        if isinstance(args, list) and len(args) == 2:
            if not (isinstance(args[0], str) or get_function(args[0])):
                raise exceptions.FunctionValidationError(
                    '`get_label`',
                    "the <label-key> should be a string or a dict "
                    "(a function). Instead, it is a {0} with the value: "
                    "{1}.".format(type(args[0]), args[0])
                )
            if not (isinstance(args[1], int) or args[1].isdigit()):
                raise exceptions.FunctionValidationError(
                    '`get_label`',
                    "the <label-values list index> should be a number "
                    "(in string or int form). Instead, it is a {0} with the "
                    "value: {1}.".format(type(args[1]), args[1])
                )
            self.label_key = args[0]
            self.values_list_index = int(args[1])
        elif isinstance(args, str):
            self.label_key = args
        elif get_function(args):
            self.label_key = args
        else:
            raise exceptions.FunctionValidationError(
                '`get_label`',
                "`get_label` function argument should be a list of 2 "
                "elements ([<label-key>, <label-values list index>]), a "
                "string (<label-key>), or a dict (a function). Instead, "
                "it is a {0} with the value: {1}.".format(type(args), args)
            )

    def validate(self, plan):
        pass

    def evaluate(self, handler):
        return handler.get_label(self.label_key, self.values_list_index)


@register(name='get_environment_capability', func_eval_type=RUNTIME_FUNC)
class GetEnvironmentCapability(Function):
    def __init__(self, args, **kwargs):
        self.capability_path = None
        super(GetEnvironmentCapability, self).__init__(args, **kwargs)

    def parse_args(self, args):
        if isinstance(args, str):
            self.capability_path = [args]
        elif isinstance(args, list):
            if len(args) < 2:
                raise exceptions.FunctionValidationError(
                    '`get_environment_capability`',
                    "`get_environment_capability` function arguments can't be"
                    " list with only one item; only string or list with"
                    " more than one item are accepted. Instead, it is a {0}"
                    " with the value: {1}.".format(type(args), args)
                )
            else:
                self.capability_path = args
        else:
            raise exceptions.FunctionValidationError(
                '`get_environment_capability`',
                "`get_environment_capability` function argument should be a "
                "string or list with more than one item are accepted."
                " Instead, it is a {0} with the value: {1}.".format(type(
                    args), args)
            )
        for arg_index, arg in enumerate(args):
            if not isinstance(arg, (str, int)) and not get_function(arg):
                raise ValueError(
                    "`get_environment_capability` function arguments"
                    " can't be complex values; only strings/ints/functions"
                    " are accepted. Instead, the item with index"
                    " {0} is {1} of type {2}".format(arg_index, arg, type(arg))
                )

    def validate(self, plan):
        pass

    def evaluate(self, handler):
        return handler.get_environment_capability(self.capability_path)


class ValidateArgumentMixin(object):
    def _validate_argument(self,
                           argument_name,
                           argument_type=str,
                           validation_only=False):
        exception_cls = exceptions.FunctionValidationError if validation_only \
            else exceptions.FunctionEvaluationError
        value = getattr(self, argument_name)
        if get_function(value):
            if validation_only:
                return
            raise exception_cls(
                self.name,
                'argument `{0}` should not be a function ({1})'
                .format(argument_name, value))
        if not isinstance(value, argument_type):
            raise exception_cls(
                self.name,
                'argument `{0}` should be of type {1} but is {2} ({3})'
                .format(argument_name, argument_type, type(value), value))


@register(name='string_find', func_eval_type=HYBRID_FUNC)
class StringFind(Function, ValidateArgumentMixin):
    def __init__(self, *args, **kwargs):
        self.haystack = None
        self.needle = None
        super(StringFind, self).__init__(*args, **kwargs)

    def parse_args(self, args):
        if not isinstance(args, list) or len(args) != 2:
            raise exceptions.FunctionValidationError(
                self.name, 'should be called with exactly two parameters')
        self.haystack, self.needle = args[0], args[1]

    def validate(self, plan):
        self._validate_argument('haystack', validation_only=True)
        self._validate_argument('needle', validation_only=True)

    def evaluate(self, handler):
        self._validate_argument('haystack')
        self._validate_argument('needle')
        return self.haystack.find(self.needle)


@register(name='string_replace', func_eval_type=HYBRID_FUNC)
class StringReplace(Function, ValidateArgumentMixin):
    def __init__(self, *args, **kwargs):
        self.haystack = None
        self.needle = None
        self.replacement = None
        self.count = None
        super(StringReplace, self).__init__(*args, **kwargs)

    def parse_args(self, args):
        if not isinstance(args, list) or not 3 <= len(args) <= 4:
            raise exceptions.FunctionValidationError(
                self.name, 'should be called with three or four parameters')
        self.haystack, self.needle, self.replacement = \
            args[0], args[1], args[2]
        if len(args) > 3:
            self.count = args[3]

    def validate(self, plan):
        self._validate_argument('haystack', validation_only=True)
        self._validate_argument('needle', validation_only=True)
        self._validate_argument('replacement', validation_only=True)
        if self.count is not None:
            self._validate_argument('count',
                                    argument_type=int,
                                    validation_only=True)

    def evaluate(self, handler):
        self._validate_argument('haystack')
        self._validate_argument('needle')
        self._validate_argument('replacement')
        if self.count is not None:
            self._validate_argument('count', argument_type=int)
            return self.haystack.replace(self.needle, self.replacement,
                                         self.count)
        return self.haystack.replace(self.needle, self.replacement)


@register(name='string_split', func_eval_type=HYBRID_FUNC)
class StringSplit(Function, ValidateArgumentMixin):
    def __init__(self, *args, **kwargs):
        self.input = None
        self.separator = None
        self.index = None
        super(StringSplit, self).__init__(*args, **kwargs)

    def parse_args(self, args):
        if not isinstance(args, list) or not 2 <= len(args) <= 3:
            raise exceptions.FunctionValidationError(
                self.name, 'should be called with two or three parameters')
        self.input, self.separator = args[0], args[1]
        if len(args) == 3:
            self.index = args[2]

    def validate(self, plan):
        self._validate_argument('input', validation_only=True)
        self._validate_argument('separator', validation_only=True)
        if self.index is not None:
            self._validate_argument('index',
                                    argument_type=int,
                                    validation_only=True)

    def evaluate(self, handler):
        self._validate_argument('input')
        self._validate_argument('separator')
        if self.index:
            self._validate_argument('index', argument_type=int)
        result = self.input.split(self.separator)
        if self.index is not None:
            return result[self.index]
        return result


@register(name='string_lower', func_eval_type=HYBRID_FUNC)
class StringLower(Function, ValidateArgumentMixin):
    def __init__(self, *args, **kwargs):
        self.input = None
        super(StringLower, self).__init__(*args, **kwargs)

    def parse_args(self, args):
        if isinstance(args, str) \
                or (isinstance(args, dict) and len(args) == 1):
            self.input = args
        else:
            raise exceptions.FunctionValidationError(
                self.name, 'should be called with exactly one parameter')

    def validate(self, plan):
        self._validate_argument('input', validation_only=True)

    def evaluate(self, handler):
        self._validate_argument('input')
        return self.input.lower()


@register(name='string_upper', func_eval_type=HYBRID_FUNC)
class StringUpper(Function, ValidateArgumentMixin):
    def __init__(self, *args, **kwargs):
        self.input = None
        super(StringUpper, self).__init__(*args, **kwargs)

    def parse_args(self, args):
        if isinstance(args, str) \
                or (isinstance(args, dict) and len(args) == 1):
            self.input = args
        else:
            raise exceptions.FunctionValidationError(
                self.name, 'should be called with exactly one parameter')

    def validate(self, plan):
        self._validate_argument('input', validation_only=True)

    def evaluate(self, handler):
        self._validate_argument('input')
        return self.input.upper()


def _get_property_value(node_name,
                        properties,
                        property_path,
                        handler,
                        context,
                        context_path='',
                        raise_if_not_found=True,
                        func_name=None):
    """Extracts a property's value according to the provided property path

    :param node_name: Node name the property belongs to (for logging).
    :param properties: Properties dict.
    :param property_path: Property path as list.
    :param context_path: Context path (for logging).
    :param raise_if_not_found: Whether to raise an error if property not found.
    "param func_name: Originating function name (for raising error).
    :return: Property value.
    """
    def str_list(li):
        return [str(item) for item in li]

    def evaluate_property_value(v):
        if not handler.runtime_only_evaluation:
            return v
        return handler(v, None, context, context_path)

    value = properties
    for p in property_path:
        if get_function(value):
            value = [value, p]
        elif isinstance(value, dict):
            if p not in value:
                if raise_if_not_found:
                    raise exceptions.FunctionEvaluationError(
                        func_name,
                        "Node template property '{0}.properties.{1}' "
                        "referenced from '{2}' doesn't exist."
                        .format(node_name, '.'.join(str_list(property_path)),
                                context_path)
                    )
                return None
            else:
                value = evaluate_property_value(value[p])
        elif isinstance(value, list):
            try:
                value = evaluate_property_value(value[p])
            except TypeError:
                raise exceptions.FunctionEvaluationError(
                    func_name,
                    "Node template property '{0}.properties.{1}' "
                    "referenced from '{2}' is expected {3} to be an int "
                    "but it is a {4}."
                    .format(node_name, '.'.join(str_list(property_path)),
                            context_path, p, type(p).__name__)
                )
            except IndexError:
                if raise_if_not_found:
                    raise exceptions.FunctionEvaluationError(
                        func_name,
                        "Node template property '{0}.properties.{1}' "
                        "referenced from '{2}' index is out of range. Got {3}"
                        " but list size is {4}."
                        .format(node_name, '.'.join(str_list(property_path)),
                                context_path, p, len(value))
                    )
                return None
        else:
            if raise_if_not_found:
                raise exceptions.FunctionEvaluationError(
                    func_name,
                    "Node template property '{0}.properties.{1}' "
                    "referenced from '{2}' doesn't exist."
                    .format(node_name, '.'.join(str_list(property_path)),
                            context_path)
                )
            return None

    return value


def parse(raw_function, scope=None, context=None, path=None):
    f = get_function(raw_function)
    if f:
        return_type = raw_function.get('type', None)
        return f[0](f[1],
                    scope=scope,
                    context=context,
                    path=path,
                    raw=raw_function,
                    return_type=return_type)
    return raw_function


def evaluate_node_functions(node, storage, instance_context=None):
    handler = runtime_evaluation_handler(storage)
    if instance_context:
        # instance_context means we're evaluating node functions in the
        # context of a specific node instance - the ID of that instance is
        # given as this "context". Let's pass it through to the functions,
        # so that e.g. get_attribute can decide which instance to use.
        node['instance_id_hint'] = instance_context
    scan.scan_node_template(node, handler, replace=True)
    return node


def evaluate_node_instance_functions(instance, storage):
    handler = runtime_evaluation_handler(storage)
    scan.scan_properties(
        instance.get('runtime_properties', {}),
        handler,
        scope=scan.NODE_TEMPLATE_SCOPE,
        context=instance,
        path='{0}.{1}.runtime_properties'.format(
            NODE_INSTANCES,
            instance['id']),
        replace=True)
    return instance


def evaluate_functions(payload, context, storage):
    """Evaluate functions in payload.

    :param payload: The payload to evaluate.
    :param context: Context used during evaluation.
    :param storage: Storage backend for runtime function evaluation
    :return: payload.
    """
    scope = None
    if 'source' in context and 'target' in context:
        scope = scan.NODE_TEMPLATE_RELATIONSHIP_SCOPE
    elif 'self' in context:
        scope = scan.NODE_TEMPLATE_SCOPE
    handler = runtime_evaluation_handler(storage)
    scan.scan_properties(payload,
                         handler,
                         scope=scope,
                         context=context,
                         path=context.pop(
                             EVAL_FUNCS_PATH_PREFIX_KEY,
                             EVAL_FUNCS_PATH_DEFAULT_PREFIX),
                         replace=True)
    return payload


def evaluate_capabilities(capabilities, storage):
    """Evaluates capabilities definition containing intrinsic functions.

    :param capabilities: The dict of capabilities to evaluate
    :param storage: Storage backend for runtime function evaluation
    :return: Capabilities dict.
    """
    capabilities = _clean_invalid_secrets(capabilities, storage)
    return evaluate_functions(
        payload=capabilities,
        context={EVAL_FUNCS_PATH_PREFIX_KEY: CAPABILITIES},
        storage=storage)


def evaluate_outputs(outputs_def, storage):
    """Evaluates an outputs definition containing intrinsic functions.

    :param outputs_def: Outputs definition.
    :param storage: Storage backend for runtime function evaluation
    :return: Outputs dict.
    """
    outputs = _clean_invalid_secrets(outputs_def, storage)
    return evaluate_functions(
        payload=outputs,
        context={'evaluate_outputs': True,
                 EVAL_FUNCS_PATH_PREFIX_KEY: OUTPUTS},
        storage=storage)


def _clean_invalid_secrets(data_dict, storage):
    clean_dict = {}
    for k, v in data_dict.items():
        value = v['value']
        clean_dict[k] = value
        if not isinstance(value, dict) or 'get_secret' not in value:
            continue
        secret_key = value['get_secret']
        if isinstance(value['get_secret'], list):
            secret_key = secret_key[0]
        if not isinstance(secret_key, str):
            continue
        try:
            storage.secret_method(secret_key)
        except Exception as e:
            if hasattr(e, 'status_code') and e.status_code == 404:
                del clean_dict[k]
                continue
            else:
                raise
    return clean_dict


def _args_to_str_func(handler, v, scope, context, path):
    """Used to display extra information when recursion limit is reached.
    This is the message-creating function used with an _EvaluationHandler,
    so it takes the same arguments as that.
    """
    return "Limit was reached with the following path - {0}".format(path)


class _EvaluationHandler(object):
    # if this is True, replace nested functions with their return values
    # (if this is False, only go through the plan, and don't mutate it)
    scan_replace = True

    @limit_recursion(50, args_to_str_func=_args_to_str_func)
    def __call__(self, v, scope, context, path):
        evaluated_value = v
        scanned = False
        while True:
            scan.scan_properties(
                evaluated_value,
                self,
                scope=scope,
                context=context,
                path=path,
                replace=self.scan_replace)
            func = parse(evaluated_value,
                         scope=scope,
                         context=context,
                         path=path)
            if not isinstance(func, Function):
                break
            previous_evaluated_value = evaluated_value
            evaluated_value = self.evaluate_function(func)
            context = func.context
            if scanned and previous_evaluated_value == evaluated_value:
                break
            scanned = True
        return evaluated_value

    def evaluate_function(self, func):
        return func.evaluate(self)

    @property
    def runtime_only_evaluation(self):
        return False


class _PlanEvaluationHandler(_EvaluationHandler):
    def __init__(self, plan, runtime_only_evaluation):
        self._plan = plan
        self._runtime_only_evaluation = runtime_only_evaluation

    def evaluate_function(self, func):
        if not self._runtime_only_evaluation and \
                func.func_eval_type in (HYBRID_FUNC, STATIC_FUNC):
            return_value = func.evaluate(self)
        else:
            if 'operation' in func.context:
                func.context['operation']['has_intrinsic_functions'] = True
            return_value = func.raw
        return return_value

    def get_input(self, input_name):
        try:
            return self._plan.inputs[input_name]
        except KeyError:
            raise exceptions.UnknownInputError(
                "get_input function references an "
                "unknown input '{0}'.".format(input_name))

    def get_node(self, node_name):
        for n in self._plan.node_templates:
            if n['name'] == node_name:
                return n
        else:
            raise KeyError('Node {0} does not exist'.format(node_name))

    def get_sys(self, entity, prop):
        raise exceptions.FunctionEvaluationError(
            "`get_sys` should be evaluated at runtime only")

    def get_consumers(self, prop):
        raise exceptions.FunctionEvaluationError(
            "`get_consumers` should be evaluated at runtime only")

    @property
    def runtime_only_evaluation(self):
        return self._runtime_only_evaluation


class _RuntimeEvaluationHandler(_EvaluationHandler):
    def __init__(self, storage):
        self._storage = storage
        self._nodes_cache = {}
        self._node_to_node_instances = {}
        self._node_instances_cache = {}
        self._secrets_cache = {}
        self._capabilities_cache = {}
        self._labels_cache = {}
        self._group_capabilities_cache = {}
        self._environment_capabilities_cache = {}

    def get_input(self, input_name):
        return self._storage.get_input(input_name)

    def get_node(self, node_id):
        if node_id not in self._nodes_cache:
            node = self._storage.get_node(node_id)
            self._nodes_cache[node_id] = node
        return self._nodes_cache[node_id]

    def get_node_instances(self, node_id):
        if node_id not in self._node_to_node_instances:
            node_instances = self._storage.get_node_instances(node_id)
            self._node_to_node_instances[node_id] = node_instances
            for node_instance in node_instances:
                self._node_instances_cache[node_instance['id']] = node_instance
        return self._node_to_node_instances[node_id]

    def get_node_instance(self, node_instance_id):
        if node_instance_id not in self._node_instances_cache:
            node_instance = self._storage.get_node_instance(node_instance_id)
            self._node_instances_cache[node_instance_id] = node_instance
        return self._node_instances_cache[node_instance_id]

    def get_secret(self, secret_key):
        if secret_key not in self._secrets_cache:
            secret = self._storage.get_secret(secret_key)
            self._secrets_cache[secret_key] = secret.value
        return self._secrets_cache[secret_key]

    def get_capability(self, capability_path):
        capability_id = _convert_attribute_list_to_python_syntax_string(
            capability_path)
        if capability_id not in self._capabilities_cache:
            capability = self._storage.get_capability(capability_path)
            self._capabilities_cache[capability_id] = capability
        return self._capabilities_cache[capability_id]

    def get_group_capability(self, capability_path):
        capability_id = _convert_attribute_list_to_python_syntax_string(
            capability_path)
        if capability_id not in self._group_capabilities_cache:
            capability = self._storage.get_group_capability(capability_path)
            self._group_capabilities_cache[capability_id] = capability
        return self._group_capabilities_cache[capability_id]

    def get_label(self, label_key, values_list_index):
        labels_cache_entry = (label_key if values_list_index is None else
                              label_key + '_' + str(values_list_index))
        if labels_cache_entry not in self._labels_cache:
            label_values = self._storage.get_label(label_key,
                                                   values_list_index)
            self._labels_cache[labels_cache_entry] = label_values
        return self._labels_cache[labels_cache_entry]

    def get_environment_capability(self, capability_path):
        capability_id = _convert_attribute_list_to_python_syntax_string(
            capability_path)
        if capability_id not in self._environment_capabilities_cache:
            capability = self._storage.get_environment_capability(
                capability_path
            )
            self._environment_capabilities_cache[capability_id] = capability
        return self._environment_capabilities_cache[capability_id]

    def get_sys(self, entity, prop):
        return self._storage.get_sys(entity, prop)

    def get_consumers(self, prop):
        return self._storage.get_consumers(prop)

    @property
    def runtime_only_evaluation(self):
        return True


class IDDSpec(object):
    """Description of an inter-deployment-dependency.

    Instances of IDDSpec describe a single dependency, based on an
    IDDCreatingFunction.
    Based on IDDSpec, we will be able to render actual instances of
    inter-deployment-dependencies, containing links to actual node instances.
    """
    def __init__(
        self,
        scope: str,
        context: Dict[str, str],
        target_deployment: Union[str, Dict],
        function_identifier: str,
    ):
        """Create an IDDSpec; this should only be created by IDDFindingHandler

        :param scope: func.scope of the creating function ("node_template")
        :param context: a dict containing optionally the keys:
            - "node_name" - the node id - for node_template IDDs
            - "target_node_name", "source_node_name" - the relationship
              source and target node names - for relationship IDDs
        :param target_deployment: the IDDCreatingFunction's target_deployment -
            either a string target deployment id (if a literal was provided),
            or a function representation (arbitrarily-nested dict)
        :param function_identifier: the path of the IDDCreatingFunction,
            for example "outputs.out1.value.get_capability"
        """
        self.scope = scope
        self.context = context
        self.target_deployment = target_deployment
        self.function_identifier = function_identifier


class IDDFindingHandler(_EvaluationHandler):
    """An EvaluationHandler that goes through all functions, and stores IDDs.

    Instead of recursively evaluating the functions, only recursively examine
    them, and the functions that create inter-deployment-dependencies are
    registered in the .dependencies list on this instance.
    """
    scan_replace = False

    def __init__(self, plan):
        self._plan = plan
        self.dependencies = []

    def evaluate_function(self, func):
        if not isinstance(func, InterDeploymentDependencyCreatingFunction):
            return
        context = {}
        if func.scope == 'node_template':
            context['node_name'] = func.context['name']
        elif func.scope == 'node_template_relationship':
            context = {
                'source_node_name': func.context['node_template']['name'],
                'target_node_name': func.context['relationship']['target_id'],
                'relationship_type': func.context['relationship']['type'],
            }
        dependency = IDDSpec(
            func.scope,
            context,
            func.target_deployment,
            func.function_identifier,
        )
        self.dependencies.append(dependency)


class SecretFindingHandler(_EvaluationHandler):
    """An EvaluationHandler that finds secret names used in the plan."""
    scan_replace = False

    def __init__(self, plan):
        self._plan = plan
        self.secrets = set()

    def evaluate_function(self, func):
        if not isinstance(func, GetSecret):
            return
        secret_name = func.secret_id
        if isinstance(secret_name, list):
            secret_name = secret_name[0]
        if isinstance(secret_name, str):
            self.secrets.add(secret_name)


def plan_evaluation_handler(plan, runtime_only_evaluation=False):
    return _PlanEvaluationHandler(plan, runtime_only_evaluation)


def runtime_evaluation_handler(storage):
    return _RuntimeEvaluationHandler(storage)


def validate_functions(plan):
    def handler(v, scope, context, path):
        func = parse(v, scope=scope, context=context, path=path)
        if isinstance(func, Function):
            func.validate(plan)
        scan.scan_properties(
            v,
            handler,
            scope=scope,
            context=context,
            path=path,
            replace=False)
        return v

    scan.scan_service_template(plan, handler, replace=False)


def find_requirements(plan):
    required_parent_capabilities = []
    secrets = []

    def handler(v, scope, context, path):
        func = parse(v, scope=scope, context=context, path=path)
        if isinstance(func, GetEnvironmentCapability):
            required_parent_capabilities.append(func.capability_path)
        elif isinstance(func, GetSecret):
            secrets.append(func.secret_id)
        scan.scan_properties(
            v,
            handler,
            scope=scope,
            context=context,
            path=path,
            replace=False)
        return v

    scan.scan_service_template(plan, handler, replace=False)

    return {
        'parent_capabilities': required_parent_capabilities,
        'secrets': secrets,
    }


def get_nested_attribute_value_of_capability(root, path):
    value = root
    for index, attr in enumerate(path[2:]):
        if isinstance(value, dict):
            if attr not in value:
                raise exceptions.FunctionEvaluationError(
                    'get_capability',
                    "Attribute '{0}' doesn't exist in '{1}' "
                    "in deployment '{2}'.".format(
                        attr,
                        _convert_attribute_list_to_python_syntax_string(
                            path[1:index + 2]),
                        path[0]
                    )
                )

            value = value[attr]
        elif isinstance(value, list):
            try:
                value = value[attr]
            except TypeError:
                raise exceptions.FunctionEvaluationError(
                    'get_capability',
                    "Item in index {0} in the get_capability arguments "
                    "list [{1}] is expected to be an int "
                    "but got {2}.".format(
                        index + 2,
                        ', '.join('{0}'.format(item) for item in path),
                        type(attr).__name__))
            except IndexError:
                raise exceptions.FunctionEvaluationError(
                    'get_capability',
                    "List size of '{0}' is {1}, in "
                    "deployment '{2}', but index {3} is "
                    "retrieved.".format(
                        _convert_attribute_list_to_python_syntax_string(
                            path[1:index + 2]),
                        len(value),
                        path[0],
                        attr))
        else:
            raise exceptions.FunctionEvaluationError(
                'get_capability',
                "Object {0}, in capability '{1}' in deployment '{2}', "
                "has no attribute {3}".format(
                    _convert_attribute_list_to_python_syntax_string(
                        path[2:index + 2]),
                    path[1],
                    path[0],
                    attr))
    return {'value': value}
