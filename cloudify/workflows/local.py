########
# Copyright (c) 2014 GigaSpaces Technologies Ltd. All rights reserved
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

import abc
import os
import tempfile
import copy
import importlib
import shutil
import json
import time
import threading
import weakref

from abc import ABC
from datetime import datetime
from contextlib import contextmanager
from io import StringIO

from cloudify_rest_client.nodes import Node
from cloudify_rest_client.blueprints import Blueprint
from cloudify_rest_client.deployments import Deployment
from cloudify_rest_client.executions import Execution
from cloudify_rest_client.node_instances import NodeInstance
from cloudify_rest_client.deployment_updates import DeploymentUpdate

from cloudify import dispatch, utils
from cloudify.workflows.workflow_context import (
    DEFAULT_LOCAL_TASK_THREAD_POOL_SIZE)

try:
    from dsl_parser.constants import HOST_TYPE
    from dsl_parser import parser as dsl_parser, tasks as dsl_tasks
    from dsl_parser import functions as dsl_functions
    _import_error = None
except ImportError as e:
    _import_error = str(e)
    dsl_parser = None
    dsl_tasks = None
    dsl_functions = None
    HOST_TYPE = None


class _Environment(object):
    def __init__(self, storage):
        self.storage = storage
        self.storage.env = self

    @property
    def plan(self):
        return self.storage.plan

    @property
    def name(self):
        return self.storage.name

    @property
    def created_at(self):
        return self.storage.created_at

    def outputs(self):
        return dsl_functions.evaluate_outputs(
            outputs_def=self.plan['outputs'], storage=self.storage)

    def evaluate_functions(self, payload, context):
        return dsl_functions.evaluate_functions(
            payload=payload, context=context, storage=self.storage)

    def execute(self,
                workflow,
                parameters=None,
                allow_custom_parameters=False,
                task_retries=-1,
                task_retry_interval=30,
                subgraph_retries=0,
                task_thread_pool_size=DEFAULT_LOCAL_TASK_THREAD_POOL_SIZE):
        workflows = self.plan['workflows']
        workflow_name = workflow
        if workflow_name not in workflows:
            raise ValueError("'{0}' workflow does not exist. "
                             "existing workflows are: [{1}]"
                             .format(workflow_name,
                                     ', '.join(workflows)))

        workflow = workflows[workflow_name]
        execution_id = utils.uuid4()
        ctx = {
            'type': 'workflow',
            'local': True,
            'deployment_id': self.name,
            'blueprint_id': self.name,
            'execution_id': execution_id,
            'workflow_id': workflow_name,
            'storage': self.storage,
            'task_retries': task_retries,
            'task_retry_interval': task_retry_interval,
            'subgraph_retries': subgraph_retries,
            'local_task_thread_pool_size': task_thread_pool_size,
            'task_name': workflow['operation']
        }
        merged_parameters = _merge_and_validate_execution_parameters(
            workflow, workflow_name, parameters, allow_custom_parameters)
        self.storage.store_execution(execution_id, ctx, merged_parameters)
        try:
            rv = dispatch.dispatch(__cloudify_context=ctx,
                                   **merged_parameters)
            self.storage.execution_ended(execution_id)
            return rv
        except Exception as e:
            self.storage.execution_ended(execution_id, e)
            raise


def init_env(blueprint_path,
             name='local',
             inputs=None,
             storage=None,
             ignored_modules=None,
             provider_context=None,
             resolver=None,
             validate_version=True):
    if storage is None:
        storage = InMemoryStorage()

    storage.create_blueprint(
        name,
        blueprint_path,
        provider_context=provider_context,
        resolver=resolver,
        validate_version=validate_version
    )
    storage.create_deployment(
        name,
        blueprint_name=name,
        inputs=inputs,
        ignored_modules=ignored_modules,
    )
    deployment_storage = storage.load(name)
    return _Environment(storage=deployment_storage)


def load_env(name, storage, resolver=None):
    deployment_storage = storage.load(name)
    if deployment_storage is None:
        return None
    return _Environment(storage=deployment_storage)


def _parse_plan(blueprint_plan, inputs, ignored_modules):
    if dsl_parser is None:
        raise ImportError('cloudify-dsl-parser must be installed to '
                          'execute local workflows. '
                          '(e.g. "pip install cloudify-dsl-parser") [{0}]'
                          .format(_import_error))
    plan = dsl_tasks.prepare_deployment_plan(
        blueprint_plan,
        inputs=inputs)
    nodes = [Node(node) for node in plan['nodes']]
    node_instances = [NodeInstance(instance)
                      for instance in plan['node_instances']]
    _prepare_nodes_and_instances(nodes, node_instances, ignored_modules)
    return plan, nodes, node_instances


def _validate_node(node):
    if HOST_TYPE in node['type_hierarchy']:
        install_agent_prop = node.properties.get('install_agent')
        if install_agent_prop:
            raise ValueError("'install_agent': true is not supported "
                             "(it is True by default) "
                             "when executing local workflows. "
                             "The 'install_agent' property "
                             "must be set to false for each node of type {0}."
                             .format(HOST_TYPE))


def _prepare_nodes_and_instances(nodes, node_instances, ignored_modules):

    def scan(parent, name, node):
        for operation in parent.get(name, {}).values():
            if not operation['operation']:
                continue
            _get_module_method(operation['operation'],
                               tpe=name,
                               node_name=node.id,
                               ignored_modules=ignored_modules)

    for node in nodes:
        scalable = node['capabilities']['scalable']['properties']
        node.update(dict(
            number_of_instances=scalable['current_instances'],
            deploy_number_of_instances=scalable['default_instances'],
            min_number_of_instances=scalable['min_instances'],
            max_number_of_instances=scalable['max_instances'],
        ))
        if 'relationships' not in node:
            node['relationships'] = []
        scan(node, 'operations', node)
        _validate_node(node)
        for relationship in node['relationships']:
            scan(relationship, 'source_operations', node)
            scan(relationship, 'target_operations', node)

    for node_instance in node_instances:
        node_instance['version'] = 0
        node_instance['runtime_properties'] = {}
        node_instance['system_properties'] = {}
        node_instance['node_id'] = node_instance['name']
        if 'relationships' not in node_instance:
            node_instance['relationships'] = []


def _get_module_method(module_method_path, tpe, node_name,
                       ignored_modules=None):
    ignored_modules = ignored_modules or []
    split = module_method_path.split('.')
    module_name = '.'.join(split[:-1])
    if module_name in ignored_modules:
        return None
    method_name = split[-1]
    try:
        module = importlib.import_module(module_name)
    except ImportError:
        raise ImportError('mapping error: No module named {0} '
                          '[node={1}, type={2}]'
                          .format(module_name, node_name, tpe))
    try:
        return getattr(module, method_name)
    except AttributeError:
        raise AttributeError("mapping error: {0} has no attribute '{1}' "
                             "[node={2}, type={3}]"
                             .format(module.__name__, method_name,
                                     node_name, tpe))


def _try_convert_from_str(string, target_type):
    if target_type == str:
        return string
    if target_type == bool:
        if string.lower() == 'true':
            return True
        if string.lower() == 'false':
            return False
        return string
    try:
        return target_type(string)
    except ValueError:
        return string


def _merge_and_validate_execution_parameters(
        workflow, workflow_name, execution_parameters=None,
        allow_custom_parameters=False):

    merged_parameters = {}
    workflow_parameters = workflow.get('parameters', {})
    execution_parameters = execution_parameters or {}

    missing_mandatory_parameters = set()

    allowed_types = {
        'integer': int,
        'float': float,
        'string': (str, bytes),
        'boolean': bool
    }
    wrong_types = {}

    for name, param in workflow_parameters.items():

        if 'type' in param and name in execution_parameters:

            # check if need to convert from string
            if (isinstance(execution_parameters[name], (str, bytes)) and
                    param['type'] in allowed_types and
                    param['type'] != 'string'):
                execution_parameters[name] = \
                    _try_convert_from_str(
                        execution_parameters[name],
                        allowed_types[param['type']])

            # validate type
            if not isinstance(execution_parameters[name],
                              allowed_types.get(param['type'], object)):
                wrong_types[name] = param['type']

        if 'default' not in param:
            if name not in execution_parameters:
                if param.get('required', True):
                    missing_mandatory_parameters.add(name)
                continue
            merged_parameters[name] = execution_parameters[name]
        else:
            merged_parameters[name] = execution_parameters[name] if \
                name in execution_parameters else param['default']

    if missing_mandatory_parameters:
        raise ValueError(
            'Workflow "{0}" must be provided with the following '
            'parameters to execute: {1}'
            .format(workflow_name, ','.join(missing_mandatory_parameters)))

    if wrong_types:
        error_message = StringIO()
        for param_name, param_type in wrong_types.items():
            error_message.write('Parameter "{0}" must be of type {1}\n'.
                                format(param_name, param_type))
        raise ValueError(error_message.getvalue())

    custom_parameters = dict(
        (k, v) for (k, v) in execution_parameters.items()
        if k not in workflow_parameters)

    if not allow_custom_parameters and custom_parameters:
        raise ValueError(
            'Workflow "{0}" does not have the following parameters '
            'declared: {1}. Remove these parameters or use '
            'the flag for allowing custom parameters'
            .format(workflow_name, ','.join(custom_parameters)))

    merged_parameters.update(custom_parameters)
    return merged_parameters


class DeploymentStorage(object):
    def __init__(self, storage, deployment):
        self.name = deployment['id']
        self.blueprint_name = deployment['blueprint_id']
        self.deployment = deployment
        self._storage = storage
        self._main_instances_lock = threading.RLock()
        self._instance_locks = weakref.WeakValueDictionary()

    @contextmanager
    def _lock(self, instance_id):
        with self._main_instances_lock:
            instance_lock = self._instance_locks.get(instance_id)
            if instance_lock is None:
                instance_lock = threading.RLock()
                self._instance_locks[instance_id] = instance_lock
        with instance_lock:
            yield

    def get_workdir(self):
        return self._storage.get_workdir(self.name)

    @property
    def plan(self):
        return self.deployment['plan']

    def get_blueprint(self, blueprint_name=None):
        if blueprint_name is None:
            blueprint_name = self.blueprint_name
        return self._storage.get_blueprint(blueprint_name)

    def get_resource(self, resource_path):
        resource_path = os.path.join(
            self.get_blueprint()['resources'],
            resource_path,
        )
        with open(resource_path, 'rb') as f:
            return f.read()

    def download_resource(self, resource_path, target_path=None):
        if not target_path:
            suffix = '-{0}'.format(os.path.basename(resource_path))
            target_path = tempfile.mktemp(suffix=suffix)
        resource = self.get_resource(resource_path)
        with open(target_path, 'wb') as f:
            f.write(resource)
        return target_path

    def get_node_instance(self, node_instance_id, evaluate_functions=False):
        with self._lock(node_instance_id):
            instance = self._storage.load_instance(self.name, node_instance_id)
        if instance is None:
            raise KeyError('Instance {0} does not exist'
                           .format(node_instance_id))
        instance = copy.deepcopy(instance)
        if evaluate_functions:
            dsl_functions.evaluate_node_instance_functions(
                instance, self)
        return NodeInstance(instance)

    def store_instance(self, instance):
        with self._lock(instance['id']):
            return self._storage.store_instance(self.name, instance)

    def update_node_instance(self,
                             node_instance_id,
                             version,
                             runtime_properties=None,
                             system_properties=None,
                             state=None,
                             relationships=None,
                             force=False):
        with self._lock(node_instance_id):
            instance = self.get_node_instance(node_instance_id)
            instance_version = instance.get('version', 0)
            if not force and state is None and version != instance_version:
                raise StorageConflictError(
                    f'version {version} does not match current version of node'
                    f'instance {node_instance_id} which is {instance_version}'
                )
            else:
                instance['version'] = instance_version + 1
            if runtime_properties is not None:
                instance['runtime_properties'] = runtime_properties
            if system_properties is not None:
                instance['system_properties'] = system_properties
            if state is not None:
                instance['state'] = state
            return self.store_instance(instance)

    def get_node(self, node_id, evaluate_functions=False):
        node = self._storage.load_node(self.name, node_id)
        if node is None:
            raise KeyError('Node {0} does not exist'.format(node_id))
        node = copy.deepcopy(node)
        if evaluate_functions:
            dsl_functions.evaluate_node_functions(node, self)
        return node

    def get_nodes(self, evaluate_functions=False):
        nodes = copy.deepcopy(
            list(self._storage.get_nodes(self.name).values()))
        if not evaluate_functions:
            return nodes
        for node in nodes:
            dsl_functions.evaluate_node_functions(node, self)
        return nodes

    def get_node_instances(self, node_id=None, evaluate_functions=False,
                           deployment_id=None):
        if deployment_id is None:
            deployment_id = self.name
        instances = copy.deepcopy(self._storage.get_node_instances(
            deployment_id, node_id=node_id))
        if evaluate_functions:
            for instance in instances:
                dsl_functions.evaluate_node_instance_functions(
                    instance, self)
        return [NodeInstance(inst) for inst in instances]

    def get_executions(self):
        return self._storage.get_executions(self.name)

    def get_execution(self, execution_id):
        for execution in self.get_executions():
            if execution['id'] == execution_id:
                return execution

    def store_execution(self, execution_id, ctx, parameters,
                        status=Execution.PENDING):
        executions = self.get_executions()
        start_time = datetime.now()
        executions.append({
            'id': execution_id,
            'workflow_id': ctx['workflow_id'],
            'deployment_id': ctx['deployment_id'],
            'blueprint_id': ctx['blueprint_id'],
            'is_dry_run': False,
            'status': status,
            'status_display': self._get_status_display(status),
            'parameters': parameters,
            'created_at': start_time,
            'started_at': start_time
        })
        self._storage.store_executions(self.name, executions)

    def execution_ended(self, execution_id, error=None):
        ended_at = datetime.now()
        if error:
            status = Execution.FAILED
        else:
            status = Execution.TERMINATED
        executions = self.get_executions()
        for execution in executions:
            if execution['id'] == execution_id:
                execution.update({
                    'status': status,
                    'status_display': self._get_status_display(status),
                    'ended_at': ended_at,
                    'error': utils.format_exception(error) if error else None
                })
        self._storage.store_executions(self.name, executions)

    def _get_status_display(self, status):
        return {
            Execution.TERMINATED: 'completed'
        }.get(status, status)

    def get_provider_context(self):
        bp = self.get_blueprint()
        return copy.deepcopy(bp['provider_context'])

    def get_input(self, input_name):
        return self.deployment['inputs'][input_name]

    def get_environment_capability(self):
        raise NotImplementedError()

    def get_secret(self):
        raise NotImplementedError()

    def get_label(self):
        raise NotImplementedError()

    def __getattr__(self, name):
        return getattr(self._storage, name)


class _Storage(ABC):
    def create_blueprint(self, name, blueprint_path, provider_context=None,
                         resolver=None, validate_version=True):
        plan = dsl_parser.parse_from_path(
            dsl_file_path=blueprint_path,
            resolver=resolver,
            validate_version=validate_version)

        blueprint_filename = os.path.basename(os.path.abspath(blueprint_path))
        blueprint = {
            'id': name,
            'plan': plan,
            'resources': os.path.dirname(os.path.abspath(blueprint_path)),
            'blueprint_filename': blueprint_filename,
            'blueprint_path': blueprint_path,
            'created_at': datetime.utcnow(),
            'provider_context': provider_context or {}
        }
        self.store_blueprint(name, blueprint)

    def create_deployment(self, name, blueprint_name, inputs=None,
                          ignored_modules=None):
        blueprint = self.get_blueprint(blueprint_name)
        deployment_plan, nodes, node_instances = _parse_plan(
            blueprint['plan'], inputs, ignored_modules)
        workflows = []
        for wf_name, wf in deployment_plan.get('workflows', {}).items():
            wf = wf.copy()
            wf['name'] = wf_name
            workflows.append(wf)
        deployment = {
            'id': name,
            'blueprint_id': blueprint['id'],
            'plan': deployment_plan,
            'nodes': {n.id: n for n in nodes},
            'created_at': datetime.utcnow(),
            'inputs': inputs or {},
            'workflows': workflows,
            'scaling_groups': deployment_plan['scaling_groups'],
        }
        self.store_deployment(name, deployment)
        for instance in node_instances:
            self.store_instance(name, NodeInstance(instance))
        return self.load(name)

    def load(self, name):
        dep = self.get_deployment(name)
        if dep is None:
            return None
        return DeploymentStorage(self, dep)

    @abc.abstractmethod
    def blueprint_ids(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_blueprint(self, name):
        raise NotImplementedError()

    @abc.abstractmethod
    def store_blueprint(self, name, blueprint):
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_blueprint(self, name):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_deployment(self, name):
        raise NotImplementedError()

    @abc.abstractmethod
    def set_deployment_attributes(self, name, **attrs):
        raise NotImplementedError()

    @abc.abstractmethod
    def store_deployment(self, name, deployment):
        raise NotImplementedError()

    @abc.abstractmethod
    def remove_deployment(self, name):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_nodes(self):
        raise NotImplementedError()

    @abc.abstractmethod
    def load_node(self, deployment_id, node_id):
        raise NotImplementedError()

    @abc.abstractmethod
    def update_node(self, deployment_id, node_id, **attrs):
        raise NotImplementedError()

    @abc.abstractmethod
    def delete_node(self, deployment_id, node_id):
        raise NotImplementedError()

    @abc.abstractmethod
    def create_nodes(self, deployment_id, nodes):
        raise NotImplementedError()

    @abc.abstractmethod
    def load_instance(self, deployment_id, node_instance_id):
        raise NotImplementedError()

    @abc.abstractmethod
    def store_instance(self, deployment_id, node_instance):
        raise NotImplementedError()

    @abc.abstractmethod
    def create_node_instances(self, deployment_id, node_instances):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_node_instances(self, deployment_id, node_id=None):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_workdir(self, deployment_id):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_executions(self, deployment_id):
        raise NotImplementedError()

    @abc.abstractmethod
    def store_executions(self, deployment_id, executions):
        raise NotImplementedError()

    def create_deployment_update(self, deployment_id, update_id, update):
        dep_update = {
            'id': update_id,
            'deployment_id': deployment_id,
            'old_blueprint_id': None,
            'new_blueprint_id': None,
            'old_inputs': {},
            'new_inputs': {},
            'steps': [],
            'runtime_only_evaluation': True,
        }
        dep_update.update(update)
        self.store_deployment_update(deployment_id, update_id, dep_update)
        return self.get_deployment_update(deployment_id, update_id)

    @abc.abstractmethod
    def store_deployment_update(self, deployment_id, update_id, update):
        raise NotImplementedError()

    @abc.abstractmethod
    def get_deployment_update(self, deployment_id, update_id):
        raise NotImplementedError()

    @abc.abstractmethod
    def set_deployment_update_attributes(
            self, deployment_id, update_id, **attrs):
        raise NotImplementedError()


class InMemoryStorage(_Storage):
    def __init__(self):
        super(InMemoryStorage, self).__init__()
        self._node_instances = {}
        self._executions = {}
        self._blueprints = {}
        self._deployments = {}
        self._deployment_updates = {}

    def store_blueprint(self, name, blueprint):
        self._blueprints[name] = blueprint

    def blueprint_ids(self):
        raise list(self._blueprints.keys())

    def get_blueprint(self, name):
        return Blueprint(self._blueprints[name])

    def remove_blueprint(self, name):
        del self._blueprints[name]

    def get_deployment(self, name):
        return Deployment(self._deployments[name])

    def set_deployment_attributes(self, name, **attrs):
        self._deployments[name].update(attrs)

    def store_deployment(self, name, deployment):
        self._deployments[name] = deployment
        self._node_instances[name] = {}

    def remove_deployment(self, name):
        del self._deployments[name]
        self._executions.pop(name, None)
        self._node_instances.pop(name, None)

    def load_node(self, deployment_id, node_id):
        dep = self.get_deployment(deployment_id)
        return Node(dep['nodes'][node_id])

    def update_node(self, deployment_id, node_id, **attrs):
        dep = self.get_deployment(deployment_id)
        dep['nodes'][node_id].update(attrs)

    def delete_node(self, deployment_id, node_id):
        dep = self.get_deployment(deployment_id)
        del dep['nodes'][node_id]

    def create_nodes(self, deployment_id, nodes):
        dep = self.get_deployment(deployment_id)
        for node in nodes:
            node_id = node['id']
            dep['nodes'][node_id] = node

    def load_instance(self, deployment_id, node_instance_id):
        return self._node_instances.get(deployment_id).get(node_instance_id)

    def store_instance(self, deployment_id, node_instance):
        instance_id = node_instance['id']
        self._node_instances[deployment_id][instance_id] = node_instance
        return node_instance

    def create_node_instances(self, deployment_id, node_instances):
        stored = []
        for instance in node_instances:
            stored_ni = self.store_instance(deployment_id, instance)
            stored.append(stored_ni)
        return stored

    def get_nodes(self, deployment_id):
        dep = self.get_deployment(deployment_id)
        return {node_id: Node(n) for node_id, n in dep['nodes'].items()}

    def get_node_instances(self, deployment_id, node_id=None):
        instances = list(self._node_instances.get(deployment_id, {}).values())
        if node_id:
            instances = [i for i in instances if i.node_id == node_id]
        return [NodeInstance(inst) for inst in instances]

    def get_workdir(self, deployment_id):
        raise NotImplementedError('get_workdir is not implemented by memory '
                                  'storage')

    def get_executions(self, deployment_id):
        return self._executions.get(deployment_id, [])

    def store_executions(self, deployment_id, executions):
        self._executions[deployment_id] = executions

    def store_deployment_update(self, deployment_id, update_id, update):
        self._deployment_updates[(deployment_id, update_id)] = update

    def get_deployment_update(self, deployment_id, update_id):
        return DeploymentUpdate(
            self._deployment_updates[(deployment_id, update_id)])

    def set_deployment_update_attributes(
            self, deployment_id, update_id, **attrs):
        self._deployment_updates[(deployment_id, update_id)].update(attrs)


class FileStorage(_Storage):
    def __init__(self, storage_dir='/tmp/cloudify-workflows'):
        super(FileStorage, self).__init__()
        self._root_storage_dir = os.path.join(storage_dir)

    def store_blueprint(self, name, blueprint):
        blueprint_dir = os.path.join(
            self._root_storage_dir, 'blueprints', name)
        os.makedirs(blueprint_dir)

        def ignore(src, names):
            return names if os.path.abspath(resources_target) == src \
                else set()
        resources_source = blueprint['resources']
        resources_target = os.path.join(blueprint_dir, 'resources')
        shutil.copytree(resources_source, resources_target, ignore=ignore)
        blueprint['resources'] = resources_target

        with open(os.path.join(blueprint_dir, 'blueprint.json'), 'w') as f:
            json.dump(blueprint, f, indent=4, cls=JSONEncoderWithDatetime)

    def blueprint_ids(self):
        return os.listdir(os.path.join(self._root_storage_dir, 'blueprints'))

    def get_blueprint(self, name):
        blueprint_dir = os.path.join(
            self._root_storage_dir, 'blueprints', name)
        try:
            with open(os.path.join(blueprint_dir, 'blueprint.json')) as f:
                return Blueprint(json.load(f, object_hook=load_datetime))
        except IOError:
            return None

    def remove_blueprint(self, name):
        blueprint_dir = os.path.join(
            self._root_storage_dir, 'blueprints', name)
        shutil.rmtree(blueprint_dir)

    def get_deployment(self, name):
        deployment_dir = os.path.join(
            self._root_storage_dir, 'deployments', name)
        try:
            with open(os.path.join(deployment_dir, 'deployment.json')) as f:
                return Deployment(json.load(f, object_hook=load_datetime))
        except IOError:
            return None

    def set_deployment_attributes(self, name, **attrs):
        deployment_storage = os.path.join(
            self._root_storage_dir, 'deployments', name, 'deployment.json')
        deployment = self.get_deployment(name)
        deployment.update(attrs)
        with open(deployment_storage, 'w') as f:
            json.dump(deployment, f, indent=4, cls=JSONEncoderWithDatetime)

    def store_deployment(self, name, deployment):
        deployment_dir = os.path.join(
            self._root_storage_dir, 'deployments', name)
        os.makedirs(deployment_dir)
        os.mkdir(os.path.join(deployment_dir, 'node-instances'))
        os.mkdir(os.path.join(deployment_dir, 'nodes'))
        os.mkdir(os.path.join(deployment_dir, 'workdir'))
        os.mkdir(os.path.join(deployment_dir, 'updates'))

        with open(os.path.join(deployment_dir, 'deployment.json'), 'w') as f:
            json.dump(deployment, f, indent=4, cls=JSONEncoderWithDatetime)

    def remove_deployment(self, name):
        deployment_dir = os.path.join(
            self._root_storage_dir, 'deployments', name)
        shutil.rmtree(deployment_dir)

    @contextmanager
    def payload(self):
        payload_path = os.path.join(self._root_storage_dir, 'payload')
        try:
            with open(payload_path, 'r') as f:
                payload = json.load(f, object_hook=load_datetime)
        except IOError:
            payload = {}
        yield payload
        with open(payload_path, 'w') as f:
            json.dump(payload, f, indent=4, cls=JSONEncoderWithDatetime)
            f.write(os.linesep)

    def load_node(self, deployment_id, node_id):
        dep = self.get_deployment(deployment_id)
        return Node(dep['nodes'][node_id])

    def update_node(self, deployment_id, node_id, **attrs):
        dep = self.get_deployment(deployment_id)
        dep['nodes'][node_id].update(attrs)
        self.set_deployment_attributes(deployment_id, nodes=dep['nodes'])

    def delete_node(self, deployment_id, node_id):
        dep = self.get_deployment(deployment_id)
        del dep['nodes'][node_id]
        self.set_deployment_attributes(deployment_id, nodes=dep['nodes'])

    def create_nodes(self, deployment_id, nodes):
        dep = self.get_deployment(deployment_id)
        for node in nodes:
            node_id = node['id']
            dep['nodes'][node_id] = node
        self.set_deployment_attributes(deployment_id, nodes=dep['nodes'])

    def load_instance(self, deployment_id, node_instance_id):
        with open(self._instance_path(deployment_id, node_instance_id)) as f:
            return NodeInstance(json.load(f, object_hook=load_datetime))

    def store_instance(self, deployment_id, instance):
        with open(self._instance_path(deployment_id, instance.id), 'w') as f:
            f.write(json.dumps(instance, cls=JSONEncoderWithDatetime))
        return NodeInstance(instance)

    def create_node_instances(self, deployment_id, node_instances):
        stored = []
        for instance in node_instances:
            stored_ni = self.store_instance(deployment_id, instance)
            stored.append(stored_ni)
        return stored

    def _instance_path(self, deployment_id, node_instance_id):
        return os.path.join(
            self._root_storage_dir,
            'deployments',
            deployment_id,
            'node-instances',
            '{0}.json'.format(node_instance_id)
        )

    def get_nodes(self, deployment_id):
        return {
            node_id: Node(n) for node_id, n in
            self.get_deployment(deployment_id)['nodes'].items()
        }

    def get_node_instances(self, deployment_id, node_id=None):
        instances = [self.load_instance(deployment_id, instance_id)
                     for instance_id in self._instance_ids(deployment_id)]
        if node_id:
            instances = [i for i in instances if i.node_id == node_id]
        return [NodeInstance(inst) for inst in instances]

    def _instance_ids(self, deployment_id):
        instances_dir = os.path.join(
            self._root_storage_dir,
            'deployments',
            deployment_id,
            'node-instances',
        )
        return [os.path.splitext(fn)[0] for fn in os.listdir(instances_dir)]

    def get_workdir(self, deployment_id):
        return os.path.join(
            self._root_storage_dir,
            'deployments',
            deployment_id,
            'workdir',
        )

    def get_executions(self, deployment_id):
        executions_path = os.path.join(
            self._root_storage_dir,
            'deployments',
            deployment_id,
            'executions.json',
        )
        try:
            with open(executions_path) as f:
                return json.load(f, object_hook=load_datetime)
        except (IOError, ValueError):
            return []

    def store_executions(self, deployment_id, executions):
        executions_path = os.path.join(
            self._root_storage_dir,
            'deployments',
            deployment_id,
            'executions.json',
        )
        with open(executions_path, 'w') as f:
            json.dump(executions, f, indent=4, cls=JSONEncoderWithDatetime)

    def store_deployment_update(self, deployment_id, update_id, update):
        updates_dir = os.path.join(
            self._root_storage_dir,
            'deployments',
            deployment_id,
            'updates',
        )
        if not os.path.exist(updates_dir):
            os.makedirs(updates_dir)
        update_path = os.path.join(updates_dir, '{0}.json'.format(update_id))
        with open(update_path, 'w') as f:
            json.dump(update, f, indent=4, cls=JSONEncoderWithDatetime)

    def get_deployment_update(self, deployment_id, update_id):
        update_path = os.path.join(
            self._root_storage_dir,
            'deployments',
            deployment_id,
            'updates',
            update_id,
        )
        try:
            with open(update_path) as f:
                return DeploymentUpdate(
                    json.load(f, object_hook=load_datetime))
        except IOError:
            return None

    def set_deployment_update_attributes(
            self, deployment_id, update_id, **attrs):
        dep_up = self.get_deployment_update(deployment_id, update_id)
        if dep_up is None:
            raise RuntimeError('Dep-update {0} not found'.format(update_id))
        dep_up.update(attrs)
        self.create_deployment_update(deployment_id, update_id, dep_up)


class JSONEncoderWithDatetime(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return {'__datetime__': time.mktime(obj.timetuple())}
        return super(JSONEncoderWithDatetime, self).default(obj)


def load_datetime(input_dict):
    if '__datetime__' in input_dict:
        return datetime.fromtimestamp(input_dict['__datetime__'])
    return input_dict


class StorageConflictError(Exception):
    pass
