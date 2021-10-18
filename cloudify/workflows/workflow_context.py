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
from __future__ import absolute_import

import functools
import copy
import json
import uuid
import threading
import logging
import pika

from proxy_tools import proxy

from cloudify import amqp_client, context
from cloudify._compat import queue
from cloudify.manager import (get_bootstrap_context,
                              get_rest_client,
                              download_resource)
from cloudify.workflows.tasks import (TASK_FAILED,
                                      TASK_SUCCEEDED,
                                      TASK_RESCHEDULED,
                                      RemoteWorkflowTask,
                                      LocalWorkflowTask,
                                      NOPLocalWorkflowTask,
                                      DryRunLocalWorkflowTask,
                                      DEFAULT_TOTAL_RETRIES,
                                      DEFAULT_RETRY_INTERVAL,
                                      DEFAULT_SEND_TASK_EVENTS,
                                      DEFAULT_SUBGRAPH_TOTAL_RETRIES,
                                      GetNodeInstanceStateTask,
                                      SetNodeInstanceStateTask,
                                      SendNodeEventTask,
                                      SendWorkflowEventTask,
                                      UpdateExecutionStatusTask)
from cloudify.constants import MGMTWORKER_QUEUE
from cloudify import utils, logs, exceptions
from cloudify.state import current_workflow_ctx
from cloudify.workflows import events
from cloudify.error_handling import deserialize_known_exception
from cloudify.workflows.tasks_graph import TaskDependencyGraph
from cloudify.logs import (CloudifyWorkflowLoggingHandler,
                           CloudifyWorkflowNodeLoggingHandler,
                           SystemWideWorkflowLoggingHandler,
                           init_cloudify_logger,
                           send_workflow_event,
                           send_sys_wide_wf_event)
from cloudify.models_states import DeploymentModificationState

from cloudify.utils import is_agent_alive


try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict


DEFAULT_LOCAL_TASK_THREAD_POOL_SIZE = 8


class CloudifyWorkflowRelationshipInstance(object):
    """
    A node instance relationship instance

    :param ctx: a CloudifyWorkflowContext instance
    :param node_instance: a CloudifyWorkflowNodeInstance instance
    :param nodes_and_instances: a WorkflowNodesAndInstancesContainer instance
    :param relationship_instance: A relationship dict from a NodeInstance
           instance (of the rest client model)
    """

    def __init__(self, ctx, node_instance, nodes_and_instances,
                 relationship_instance):
        self.ctx = ctx
        self.node_instance = node_instance
        self._nodes_and_instances = nodes_and_instances
        self._relationship_instance = relationship_instance
        self._relationship = node_instance.node.get_relationship(
            relationship_instance['target_name'])

    @property
    def target_id(self):
        """The relationship target node id"""
        return self._relationship_instance.get('target_id')

    @property
    def target_node_instance(self):
        """
        The relationship's target node CloudifyWorkflowNodeInstance instance
        """
        return self._nodes_and_instances.get_node_instance(self.target_id)

    @property
    def relationship(self):
        """The relationship object for this relationship instance"""
        return self._relationship

    def execute_source_operation(self,
                                 operation,
                                 kwargs=None,
                                 allow_kwargs_override=False,
                                 send_task_events=DEFAULT_SEND_TASK_EVENTS):
        """
        Execute a node relationship source operation

        :param operation: The node relationship operation
        :param kwargs: optional kwargs to be passed to the called operation
        """
        return self.ctx._execute_operation(
            operation,
            node_instance=self.node_instance,
            related_node_instance=self.target_node_instance,
            operations=self.relationship.source_operations,
            kwargs=kwargs,
            allow_kwargs_override=allow_kwargs_override,
            send_task_events=send_task_events)

    def execute_target_operation(self,
                                 operation,
                                 kwargs=None,
                                 allow_kwargs_override=False,
                                 send_task_events=DEFAULT_SEND_TASK_EVENTS):
        """
        Execute a node relationship target operation

        :param operation: The node relationship operation
        :param kwargs: optional kwargs to be passed to the called operation
        """
        return self.ctx._execute_operation(
            operation,
            node_instance=self.target_node_instance,
            related_node_instance=self.node_instance,
            operations=self.relationship.target_operations,
            kwargs=kwargs,
            allow_kwargs_override=allow_kwargs_override,
            send_task_events=send_task_events)


class CloudifyWorkflowRelationship(object):
    """
    A node relationship

    :param ctx: a CloudifyWorkflowContext instance
    :param node: a CloudifyWorkflowNode instance
    :param nodes_and_instances: a WorkflowNodesAndInstancesContainer instance
    :param relationship: a relationship dict from a Node instance (of the
           rest client mode)
    """

    def __init__(self, ctx, node, nodes_and_instances, relationship):
        self.ctx = ctx
        self.node = node
        self._nodes_and_instances = nodes_and_instances
        self._relationship = relationship

    @property
    def target_id(self):
        """The relationship target node id"""
        return self._relationship.get('target_id')

    @property
    def target_node(self):
        """The relationship target node WorkflowContextNode instance"""
        return self._nodes_and_instances.get_node(self.target_id)

    @property
    def source_operations(self):
        """The relationship source operations"""
        return self._relationship.get('source_operations', {})

    @property
    def target_operations(self):
        """The relationship target operations"""
        return self._relationship.get('target_operations', {})

    @property
    def properties(self):
        return self._relationship.get('properties', {})

    def is_derived_from(self, other_relationship):
        """
        :param other_relationship: a string like
               cloudify.relationships.contained_in
        """
        return other_relationship in self._relationship["type_hierarchy"]


class CloudifyWorkflowNodeInstance(object):
    """
    A plan node instance

    :param ctx: a CloudifyWorkflowContext instance
    :param node: a CloudifyWorkflowContextNode instance
    :param node_instance: a NodeInstance (rest client response model)
    :param nodes_and_instances: a WorkflowNodesAndInstancesContainer instance
    """

    def __init__(self, ctx, node, node_instance, nodes_and_instances):
        self.ctx = ctx
        self._node = node
        self._node_instance = node_instance
        # Directly contained node instances. Filled in the context's __init__()
        self._contained_instances = []
        self._relationship_instances = OrderedDict(
            (relationship_instance['target_id'],
                CloudifyWorkflowRelationshipInstance(
                    self.ctx, self, nodes_and_instances,
                    relationship_instance))
            for relationship_instance in node_instance.relationships)

        # adding the node instance to the node instances map
        node._node_instances[self.id] = self

        self._logger = None

    def set_state(self, state):
        """Set the node-instance state

        :param state: The new node-instance state
        :return: a state-setting workflow task
        """
        # We don't want to alter the state of the instance during a dry run
        if self.ctx.dry_run:
            return NOPLocalWorkflowTask(self.ctx)

        return self.ctx._process_task(SetNodeInstanceStateTask(
            node_instance_id=self.id, state=state,
            workflow_context=self.ctx,
        ))

    def get_state(self):
        """Get the node-instance state

        :return: The node-instance state
        """
        return self.ctx._process_task(GetNodeInstanceStateTask(
            node_instance_id=self.id,
            workflow_context=self.ctx
        ))

    def send_event(self, event, additional_context=None):
        """Sends a workflow node event.

        :param event: The event
        :param additional_context: additional context to be added to the
               context
        """
        return self.ctx._process_task(SendNodeEventTask(
            node_instance_id=self.id,
            event=event,
            additional_context=additional_context,
            workflow_context=self.ctx,
        ))

    def execute_operation(self,
                          operation,
                          kwargs=None,
                          allow_kwargs_override=False,
                          send_task_events=DEFAULT_SEND_TASK_EVENTS):
        """
        Execute a node operation

        :param operation: The node operation
        :param kwargs: optional kwargs to be passed to the called operation
        """
        return self.ctx._execute_operation(
            operation=operation,
            node_instance=self,
            operations=self.node.operations,
            kwargs=kwargs,
            allow_kwargs_override=allow_kwargs_override,
            send_task_events=send_task_events)

    @property
    def id(self):
        """The node instance id"""
        return self._node_instance.id

    @property
    def state(self):
        """The node instance state"""
        return self._node_instance.state

    @property
    def node_id(self):
        """The node id (this instance is an instance of that node)"""
        return self._node_instance.node_id

    @property
    def relationships(self):
        """The node relationships"""
        return iter(self._relationship_instances.values())

    @property
    def node(self):
        """The node object for this node instance"""
        return self._node

    @property
    def modification(self):
        """Modification enum (None, added, removed)"""
        return self._node_instance.get('modification')

    @property
    def scaling_groups(self):
        return self._node_instance.get('scaling_groups', [])

    @property
    def logger(self):
        """A logger for this workflow node"""
        if self._logger is None:
            self._logger = self._init_cloudify_logger()
        return self._logger

    def _init_cloudify_logger(self):
        logger_name = '{0}-{1}'.format(self.ctx.execution_id, self.id)
        logging_handler = self.ctx.internal.handler.get_node_logging_handler(
            self)
        return init_cloudify_logger(logging_handler, logger_name)

    @property
    def contained_instances(self):
        """
        Returns node instances directly contained in this instance (children)
        """
        return self._contained_instances

    def _add_contained_node_instance(self, node_instance):
        self._contained_instances.append(node_instance)

    def get_contained_subgraph(self):
        """
        Returns a set containing this instance and all nodes that are
        contained directly and transitively within it
        """
        result = set([self])
        for child in self.contained_instances:
            result.update(child.get_contained_subgraph())
        return result


class CloudifyWorkflowNode(object):
    """
    A plan node instance

    :param ctx: a CloudifyWorkflowContext instance
    :param node: a Node instance (rest client response model)
    :param nodes_and_instances: a WorkflowNodesAndInstancesContainer instance
    """

    def __init__(self, ctx, node, nodes_and_instances):
        self.ctx = ctx
        self._node = node
        self._relationships = OrderedDict(
            (relationship['target_id'], CloudifyWorkflowRelationship(
                self.ctx, self, nodes_and_instances, relationship))
            for relationship in node.relationships)
        self._node_instances = {}

    @property
    def id(self):
        """The node id"""
        return self._node.id

    @property
    def type(self):
        """The node type"""
        return self._node.type

    @property
    def type_hierarchy(self):
        """The node type hierarchy"""
        return self._node.type_hierarchy

    @property
    def properties(self):
        """The node properties"""
        return self._node.properties

    @property
    def plugins_to_install(self):
        """
        The plugins to install in this node. (Only relevant for host nodes)
        """
        return self._node.get('plugins_to_install', [])

    @property
    def plugins(self):
        """
        The plugins associated with this node
        """
        return self._node.get('plugins', [])

    @property
    def host_id(self):
        return self._node.host_id

    @property
    def host_node(self):
        return self.ctx.get_node(self.host_id)

    @property
    def number_of_instances(self):
        return self._node.number_of_instances

    @property
    def relationships(self):
        """The node relationships"""
        return iter(self._relationships.values())

    @property
    def operations(self):
        """The node operations"""
        return self._node.operations

    @property
    def instances(self):
        """The node instances"""
        return iter(self._node_instances.values())

    def get_relationship(self, target_id):
        """Get a node relationship by its target id"""
        return self._relationships.get(target_id)


class _WorkflowContextBase(object):

    def __init__(self, ctx, remote_ctx_handler_cls):
        self._context = ctx = ctx or {}
        self._local_task_thread_pool_size = ctx.get(
            'local_task_thread_pool_size',
            DEFAULT_LOCAL_TASK_THREAD_POOL_SIZE)
        self._task_retry_interval = ctx.get('task_retry_interval',
                                            DEFAULT_RETRY_INTERVAL)
        self._task_retries = ctx.get('task_retries',
                                     DEFAULT_TOTAL_RETRIES)
        self._subgraph_retries = ctx.get('subgraph_retries',
                                         DEFAULT_SUBGRAPH_TOTAL_RETRIES)
        self._logger = None

        if self.local:
            storage = ctx.pop('storage')
            handler = LocalCloudifyWorkflowContextHandler(self, storage)
        else:
            handler = remote_ctx_handler_cls(self)

        self._internal = CloudifyWorkflowContextInternal(self, handler)
        # is this execution being resumed? set to True if at the beginning
        # of handling the execution, the status was already STARTED
        self.resume = False
        # all amqp Handler instances used by this workflow
        self.amqp_handlers = set()

    def cleanup(self, finished=True):
        self.internal.handler.cleanup(finished)

    def graph_mode(self):
        """
        Switch the workflow context into graph mode

        :return: A task dependency graph instance
        """
        if self.internal.task_graph.tasks:
            raise RuntimeError('Cannot switch to graph mode when tasks have '
                               'already been executed')

        self.internal.graph_mode = True
        return self.internal.task_graph

    @property
    def bootstrap_context(self):
        return self.internal._bootstrap_context

    @property
    def internal(self):
        return self._internal

    @property
    def execution_id(self):
        """The execution id"""
        return self._context.get('execution_id')

    @property
    def workflow_id(self):
        """The workflow id"""
        return self._context.get('workflow_id')

    @property
    def rest_token(self):
        """REST service token"""
        return self._context.get('rest_token')

    @property
    def rest_host(self):
        return self._context.get('rest_host')

    @property
    def execution_token(self):
        """The token of the current execution"""
        return self._context.get('execution_token')

    @property
    def bypass_maintenance(self):
        """If true, all requests sent bypass maintenance mode."""
        return self._context.get('bypass_maintenance', False)

    @property
    def tenant_name(self):
        """Cloudify tenant name"""
        return self.tenant.get('name')

    @property
    def local(self):
        """Is the workflow running in a local or remote context"""
        return self._context.get('local', False)

    @property
    def dry_run(self):
        return self._context.get('dry_run', False)

    @property
    def wait_after_fail(self):
        return self._context.get('wait_after_fail', 600)

    @property
    def logger(self):
        """A logger for this workflow"""
        if self._logger is None:
            self._logger = self._init_cloudify_logger()
        return self._logger

    @property
    def tenant(self):
        """Cloudify tenant"""
        return self._context.get('tenant', {})

    @property
    def execution_creator_username(self):
        return self._context.get('execution_creator_username')

    def _init_cloudify_logger(self):
        logger_name = self.execution_id
        logging_handler = self.internal.handler.get_context_logging_handler()
        return init_cloudify_logger(logging_handler, logger_name)

    def download_resource(self, resource_path, target_path=None):
        """Downloads a blueprint/deployment resource to target_path.

        This mirrors ctx.download_resource, but for workflow contexts.
        See CloudifyContext.download_resource.
        """
        return self._internal.handler.download_deployment_resource(
            resource_path=resource_path,
            target_path=target_path)

    def send_event(self, event, event_type='workflow_stage',
                   args=None, additional_context=None):
        """Sends a workflow event

        :param event: The event
        :param event_type: The event type
        :param args: additional arguments that may be added to the message
        :param additional_context: additional context to be added to the
               context
        """
        return self._process_task(SendWorkflowEventTask(
            event=event,
            event_type=event_type,
            event_args=args,
            additional_context=additional_context,
            workflow_context=self,
        ))

    def _execute_operation(self,
                           operation,
                           node_instance,
                           operations,
                           related_node_instance=None,
                           kwargs=None,
                           allow_kwargs_override=False,
                           send_task_events=DEFAULT_SEND_TASK_EVENTS):
        kwargs = kwargs or {}
        op_struct = operations.get(operation, {})
        if not op_struct.get('operation'):
            return NOPLocalWorkflowTask(self)
        plugin_name = op_struct['plugin']
        # could match two plugins with different executors, one is enough
        # for our purposes (extract package details)
        plugin = [p for p in node_instance.node.plugins
                  if p['name'] == plugin_name][0]
        operation_mapping = op_struct['operation']
        has_intrinsic_functions = op_struct['has_intrinsic_functions']
        operation_properties = op_struct.get('inputs', {})
        operation_executor = op_struct['executor']
        operation_total_retries = op_struct['max_retries']
        operation_retry_interval = op_struct['retry_interval']
        operation_timeout = op_struct.get('timeout', None)
        operation_timeout_recoverable = op_struct.get('timeout_recoverable',
                                                      None)
        task_name = operation_mapping
        if operation_total_retries is None:
            total_retries = self.internal.get_task_configuration()[
                'total_retries']
        else:
            total_retries = operation_total_retries

        if plugin and plugin['package_name']:
            plugin = self.internal.handler.get_plugin(plugin)

        node_context = {
            'node_id': node_instance.id,
            'node_name': node_instance.node_id,
            'plugin': {
                'name': plugin_name,
                'package_name': plugin.get('package_name'),
                'package_version': plugin.get('package_version'),
                'visibility': plugin.get('visibility'),
                'tenant_name': plugin.get('tenant_name'),
                'source': plugin.get('source')
            },
            'operation': {
                'name': operation,
                'retry_number': 0,
                'max_retries': total_retries
            },
            'has_intrinsic_functions': has_intrinsic_functions,
            'host_id': node_instance._node_instance.host_id,
            'executor': operation_executor
        }
        # central deployment agents run on the management worker
        # so we pass the env to the dispatcher so it will be on a per
        # operation basis
        if operation_executor == 'central_deployment_agent':
            agent_context = self.bootstrap_context.get('cloudify_agent', {})
            node_context['execution_env'] = agent_context.get('env', {})

        if related_node_instance is not None:
            relationships = [rel.target_id
                             for rel in node_instance.relationships]
            node_context['related'] = {
                'node_id': related_node_instance.id,
                'node_name': related_node_instance.node_id,
                'is_target': related_node_instance.id in relationships
            }

        final_kwargs = self._merge_dicts(merged_from=kwargs,
                                         merged_into=operation_properties,
                                         allow_override=allow_kwargs_override)

        return self.execute_task(
            task_name,
            local=self.local,
            kwargs=final_kwargs,
            node_context=node_context,
            send_task_events=send_task_events,
            total_retries=total_retries,
            retry_interval=operation_retry_interval,
            timeout=operation_timeout,
            timeout_recoverable=operation_timeout_recoverable)

    @staticmethod
    def _merge_dicts(merged_from, merged_into, allow_override=False):
        result = copy.copy(merged_into)
        for key, value in merged_from.items():
            if not allow_override and key in merged_into:
                raise RuntimeError('Duplicate definition of {0} in operation'
                                   ' properties and in kwargs. To allow '
                                   'redefinition, pass '
                                   '"allow_kwargs_override" to '
                                   '"execute_operation"'.format(key))
            result[key] = value
        return result

    def update_execution_status(self, new_status):
        """Updates the execution status to new_status.

        Note that the workflow status gets automatically updated before and
        after its run (whether the run succeeded or failed)
        """
        return self._process_task(UpdateExecutionStatusTask(
            status=new_status,
            workflow_context=self,
        ))

    def _build_cloudify_context(self,
                                task_id,
                                task_name,
                                node_context,
                                timeout,
                                timeout_recoverable):
        node_context = node_context or {}
        context = {
            '__cloudify_context': '0.3',
            'type': 'operation',
            'task_id': task_id,
            'task_name': task_name,
            'execution_id': self.execution_id,
            'workflow_id': self.workflow_id,
            'tenant': self.tenant,
            'timeout': timeout,
            'timeout_recoverable': timeout_recoverable
        }
        context.update(node_context)
        context.update(self.internal.handler.operation_cloudify_context)
        return context

    def execute_task(self,
                     task_name,
                     local=True,
                     task_queue=None,
                     task_target=None,
                     kwargs=None,
                     node_context=None,
                     send_task_events=DEFAULT_SEND_TASK_EVENTS,
                     total_retries=None,
                     retry_interval=None,
                     timeout=None,
                     timeout_recoverable=None):
        """
        Execute a task

        :param task_name: the task named
        :param kwargs: optional kwargs to be passed to the task
        :param node_context: Used internally by node.execute_operation
        """
        # Should deepcopy cause problems here, remove it, but please make
        # sure that WORKFLOWS_WORKER_PAYLOAD is not global in manager repo
        kwargs = copy.deepcopy(kwargs) or {}
        task_id = str(uuid.uuid4())
        cloudify_context = self._build_cloudify_context(
            task_id,
            task_name,
            node_context,
            timeout,
            timeout_recoverable)
        kwargs['__cloudify_context'] = cloudify_context

        if self.dry_run:
            return DryRunLocalWorkflowTask(
                local_task=lambda: None,
                workflow_context=self,
                name=task_name,
                kwargs=kwargs
            )

        if local:
            # oh sweet circular dependency
            from cloudify import dispatch
            return self.local_task(local_task=dispatch.dispatch,
                                   info=task_name,
                                   name=task_name,
                                   kwargs=kwargs,
                                   task_id=task_id,
                                   send_task_events=send_task_events,
                                   total_retries=total_retries,
                                   retry_interval=retry_interval)
        else:
            return self.remote_task(task_queue=task_queue,
                                    task_target=task_target,
                                    kwargs=kwargs,
                                    cloudify_context=cloudify_context,
                                    task_id=task_id,
                                    send_task_events=send_task_events,
                                    total_retries=total_retries,
                                    retry_interval=retry_interval)

    def local_task(self,
                   local_task,
                   node=None,
                   info=None,
                   kwargs=None,
                   task_id=None,
                   name=None,
                   send_task_events=DEFAULT_SEND_TASK_EVENTS,
                   override_task_config=False,
                   total_retries=None,
                   retry_interval=None):
        """
        Create a local workflow task

        :param local_task: A callable implementation for the task
        :param node: A node if this task is called in a node context
        :param info: Additional info that will be accessed and included
                     in log messages
        :param kwargs: kwargs to pass to the local_task when invoked
        :param task_id: The task id
        """
        global_task_config = self.internal.get_task_configuration()
        if hasattr(local_task, 'workflow_task_config'):
            decorator_task_config = local_task.workflow_task_config
        else:
            decorator_task_config = {}
        invocation_task_config = dict(
            local_task=local_task,
            node=node,
            info=info,
            kwargs=kwargs,
            send_task_events=send_task_events,
            task_id=task_id,
            name=name)
        if total_retries is not None:
            invocation_task_config['total_retries'] = total_retries
        if retry_interval is not None:
            invocation_task_config['retry_interval'] = retry_interval

        final_task_config = {}
        final_task_config.update(global_task_config)
        if override_task_config:
            final_task_config.update(decorator_task_config)
            final_task_config.update(invocation_task_config)
        else:
            final_task_config.update(invocation_task_config)
            final_task_config.update(decorator_task_config)

        return self._process_task(LocalWorkflowTask(
            workflow_context=self,
            **final_task_config))

    def remote_task(self,
                    kwargs,
                    cloudify_context,
                    task_id,
                    task_queue=None,
                    task_target=None,
                    send_task_events=DEFAULT_SEND_TASK_EVENTS,
                    total_retries=None,
                    retry_interval=None):
        """
        Create a remote workflow task

        :param cloudify_context: A dict for creating the CloudifyContext
                                 used by the called task
        :param task_id: The task id
        """
        task_configuration = self.internal.get_task_configuration()
        if total_retries is not None:
            task_configuration['total_retries'] = total_retries
        if retry_interval is not None:
            task_configuration['retry_interval'] = retry_interval
        return self._process_task(
            RemoteWorkflowTask(kwargs=kwargs,
                               cloudify_context=cloudify_context,
                               task_target=task_target,
                               task_queue=task_queue,
                               workflow_context=self,
                               task_id=task_id,
                               send_task_events=send_task_events,
                               **task_configuration))

    def _process_task(self, task):
        if self.internal.graph_mode:
            return task
        else:
            self.internal.task_graph.add_task(task)
            return task.apply_async()

    def get_operations(self, graph_id):
        return self.internal.handler.get_operations(graph_id)

    def update_operation(self, operation_id, state,
                         result=None, exception=None):
        return self.internal.handler.update_operation(
            operation_id, state, result, exception)

    def get_tasks_graph(self, name):
        return self.internal.handler.get_tasks_graph(self.execution_id, name)

    def store_tasks_graph(self, name, operations=None):
        return self.internal.handler.store_tasks_graph(
            self.execution_id, name, operations=operations)

    def store_operation(self, task, dependencies, graph_id):
        return self.internal.handler.store_operation(
            graph_id=graph_id, dependencies=dependencies, **task.dump())

    def remove_operation(self, operation_id):
        return self.internal.handler.remove_operation(operation_id)

    def get_execution(self, execution_id=None):
        """
        Ge the execution object for the current execution
        :param execution_id: The Id of the execution object
        :return: Instance of `Execution` object which holds all the needed info
        """
        if not execution_id:
            execution_id = self.execution_id
        return self.internal.handler.get_execution(execution_id)


class WorkflowNodesAndInstancesContainer(object):

    def __init__(self, workflow_context, raw_nodes, raw_node_instances):
        self.workflow_context = workflow_context
        self._nodes = dict(
            (node.id, CloudifyWorkflowNode(workflow_context, node, self))
            for node in raw_nodes)

        self._node_instances = dict(
            (instance.id, CloudifyWorkflowNodeInstance(
                workflow_context, self._nodes[instance.node_id], instance,
                self))
            for instance in raw_node_instances)

        for inst in self._node_instances.values():
            for rel in inst.relationships:
                if rel.relationship.is_derived_from(
                        "cloudify.relationships.contained_in"):
                    rel.target_node_instance._add_contained_node_instance(inst)

    @property
    def nodes(self):
        return iter(self._nodes.values())

    @property
    def node_instances(self):
        return iter(self._node_instances.values())

    def get_node(self, node_id):
        """
        Get a node by its id

        :param node_id: The node id
        :return: a CloudifyWorkflowNode instance for the node or None if
                 not found
        """
        return self._nodes.get(node_id)

    def get_node_instance(self, node_instance_id):
        """
        Get a node instance by its id

        :param node_instance_id: The node instance id
        :return: a CloudifyWorkflowNode instance for the node or None if
                 not found
        """
        return self._node_instances.get(node_instance_id)

    def refresh_node_instances(self):
        raw_node_instances = self.internal.handler.get_node_instances()
        self._node_instances = dict(
            (instance.id, CloudifyWorkflowNodeInstance(
                self, self._nodes[instance.node_id], instance,
                self))
            for instance in raw_node_instances)


class CloudifyWorkflowContext(
    _WorkflowContextBase,
    WorkflowNodesAndInstancesContainer
):
    """
    A context used in workflow operations

    :param ctx: a cloudify_context workflow dict
    """

    def __init__(self, ctx):
        self.blueprint = context.BlueprintContext(ctx)
        self.deployment = WorkflowDeploymentContext(ctx, self)

        with current_workflow_ctx.push(self):
            # Not using super() here, because
            # WorkflowNodesAndInstancesContainer's __init__() needs some data
            # to be prepared before calling it. It would be possible to
            # overcome this by using kwargs + super(...).__init__() in
            # _WorkflowContextBase, but the way it is now is self-explanatory.
            _WorkflowContextBase.__init__(self, ctx,
                                          RemoteCloudifyWorkflowContextHandler)

            raw_nodes = self.internal.handler.get_nodes()
            raw_node_instances = self.internal.handler.get_node_instances()
            WorkflowNodesAndInstancesContainer.__init__(self, self, raw_nodes,
                                                        raw_node_instances)

    def _build_cloudify_context(self, *args):
        context = super(
            CloudifyWorkflowContext,
            self
        )._build_cloudify_context(*args)
        context.update({
            'blueprint_id': self.blueprint.id,
            'deployment_id': self.deployment.id
        })
        return context


class CloudifySystemWideWorkflowContext(_WorkflowContextBase):

    def __init__(self, ctx):
        with current_workflow_ctx.push(self):
            super(CloudifySystemWideWorkflowContext, self).__init__(
                ctx,
                SystemWideWfRemoteContextHandler
            )
        self._dep_contexts = None

    class _ManagedCloudifyWorkflowContext(CloudifyWorkflowContext):
        def __enter__(self):
            self.internal.start_local_tasks_processing()

        def __exit__(self, *args, **kwargs):
            self.internal.stop_local_tasks_processing()

    @property
    def deployments_contexts(self):
        if self.local:
            raise RuntimeError(
                'deployment_contexts do not exist in local workflows')
        if self._dep_contexts is None:
            self._dep_contexts = {}

            deployments_list = \
                self.internal.handler.rest_client.deployments.list(
                    _include=['id', 'blueprint_id'],
                    _get_all_results=True
                )
            for dep in deployments_list:
                # Failure to deepcopy will cause snapshot restore context hack
                # to be reset just before it's needed.
                dep_ctx = copy.deepcopy(self._context)
                dep_ctx['tenant']['name'] = self.tenant_name
                dep_ctx['deployment_id'] = dep.id
                dep_ctx['blueprint_id'] = dep.blueprint_id

                def lazily_loaded_ctx(dep_ctx):
                    def lazy_ctx():
                        if not hasattr(lazy_ctx, '_cached_ctx'):
                            lazy_ctx._cached_ctx = \
                                self._ManagedCloudifyWorkflowContext(dep_ctx)
                        return lazy_ctx._cached_ctx

                    return proxy(lazy_ctx)

                self._dep_contexts[dep.id] = lazily_loaded_ctx(dep_ctx)
        return self._dep_contexts


class CloudifyWorkflowContextInternal(object):

    def __init__(self, workflow_context, handler):
        self.workflow_context = workflow_context
        self.handler = handler
        self._bootstrap_context = None
        self._graph_mode = False
        # the graph is always created internally for events to work properly
        # when graph mode is turned on this instance is returned to the user.
        subgraph_task_config = self.get_subgraph_task_configuration()
        self._task_graph = TaskDependencyGraph(
            workflow_context=workflow_context,
            default_subgraph_task_config=subgraph_task_config)

        # local task processing
        thread_pool_size = self.workflow_context._local_task_thread_pool_size
        self.local_tasks_processor = LocalTasksProcessing(
            self.workflow_context,
            thread_pool_size=thread_pool_size)

    def get_task_configuration(self):
        bootstrap_context = self._get_bootstrap_context()
        workflows = bootstrap_context.get('workflows', {})
        total_retries = workflows.get(
            'task_retries',
            self.workflow_context._task_retries)
        retry_interval = workflows.get(
            'task_retry_interval',
            self.workflow_context._task_retry_interval)
        return dict(total_retries=total_retries,
                    retry_interval=retry_interval)

    def get_subgraph_task_configuration(self):
        bootstrap_context = self._get_bootstrap_context()
        workflows = bootstrap_context.get('workflows', {})
        subgraph_retries = workflows.get(
            'subgraph_retries',
            self.workflow_context._subgraph_retries
        )
        return dict(total_retries=subgraph_retries)

    def _get_bootstrap_context(self):
        if self._bootstrap_context is None:
            self._bootstrap_context = self.handler.bootstrap_context
        return self._bootstrap_context

    @property
    def task_graph(self):
        return self._task_graph

    @property
    def graph_mode(self):
        return self._graph_mode

    @graph_mode.setter
    def graph_mode(self, graph_mode):
        self._graph_mode = graph_mode

    def send_task_event(self, state, task, event=None):
        send_task_event_func = self.handler.get_send_task_event_func(task)
        events.send_task_event(state, task, send_task_event_func, event)

    def send_workflow_event(self,
                            event_type,
                            message=None,
                            args=None,
                            additional_context=None):
        self.handler.send_workflow_event(event_type=event_type,
                                         message=message,
                                         args=args,
                                         additional_context=additional_context)

    def start_local_tasks_processing(self):
        self.local_tasks_processor.start()

    def stop_local_tasks_processing(self):
        self.local_tasks_processor.stop()

    def add_local_task(self, task):
        self.local_tasks_processor.add_task(task)


class LocalTasksProcessing(object):

    def __init__(self, workflow_ctx, thread_pool_size=1):
        self._local_tasks_queue = queue.Queue()
        self._local_task_processing_pool = []
        self.workflow_ctx = workflow_ctx
        self._is_local_context = workflow_ctx.local
        for i in range(thread_pool_size):
            name = 'Task-Processor-{0}'.format(i + 1)
            thread = threading.Thread(target=self._process_local_task,
                                      name=name, args=(workflow_ctx, ))
            thread.daemon = True
            self._local_task_processing_pool.append(thread)
        self.stopped = False

    def start(self):
        for thread in self._local_task_processing_pool:
            thread.start()

    def stop(self):
        self.stopped = True

    def add_task(self, task):
        self._local_tasks_queue.put(task)

    def _process_local_task(self, workflow_ctx):
        # see CFY-1442
        with current_workflow_ctx.push(workflow_ctx):
            while not self.stopped:
                try:
                    task = self._local_tasks_queue.get(timeout=1)
                    task()
                # may seem too general, but daemon threads are just great.
                # anyway, this is properly unit tested, so we should be good.
                except Exception:
                    pass


class _WorkflowTaskHandler(object):
    def __init__(self, workflow_ctx):
        self._logger = logging.getLogger('dispatch')
        self.workflow_ctx = workflow_ctx
        workflow_ctx.amqp_handlers.add(self)
        self._queue_name = 'execution_responses_{0}'.format(
            workflow_ctx.execution_id)
        self._connection = None
        self._responses = {}
        self._tasks = {}
        self._bound = set()

    def wait_for_task(self, task):
        if task.id in self._responses:
            response, delivery_tag = self._responses.pop(task.id)
            self._task_callback(task, response)
            self._connection.channel_method(
                'basic_ack', delivery_tag=delivery_tag)
        else:
            self._tasks[task.id] = task

    def register(self, connection, channel):
        self._connection = connection
        channel.exchange_declare(exchange=MGMTWORKER_QUEUE,
                                 auto_delete=False,
                                 durable=True,
                                 exchange_type='direct')
        channel.queue_declare(
            queue=self._queue_name, durable=True, auto_delete=False)

        channel.basic_consume(
            queue=self._queue_name, on_message_callback=self.process)

    def process(self, channel, method, properties, body):
        try:
            response = json.loads(body.decode('utf-8'))
        except ValueError:
            self._logger.error('Error parsing response: %s', body)
            channel.basic_ack(method.delivery_tag)
            return
        if properties.correlation_id in self._tasks:
            task = self._tasks.pop(properties.correlation_id)
            self._task_callback(task, response)
            channel.basic_ack(method.delivery_tag)
        else:
            self._responses[properties.correlation_id] = \
                (response, method.delivery_tag)

    def publish(self, target, message, correlation_id, routing_key):
        if target not in self._bound:
            self._bound.add(target)
            self._connection.channel_method(
                'queue_bind',
                queue=self._queue_name, exchange=target,
                routing_key=self._queue_name)
        self._connection.publish({
            'exchange': target,
            'body': json.dumps(message),
            'properties': pika.BasicProperties(
                reply_to=self._queue_name,
                correlation_id=correlation_id),
            'routing_key': routing_key,
        })
        if self._task_deletes_exchange(message):
            self._clear_bound_exchanges_cache()

    def _task_deletes_exchange(self, message):
        """Does this task delete an amqp exchange?

        Agent delete tasks are going to delete the agent's amqp exchange,
        so we'll have to bind it again, if the agent is reinstalled
        (eg. in a deployment-update workflow) - so we'll bust the ._bound
        cache in that case.
        """
        try:
            name = message['cloudify_task']['kwargs'][
                '__cloudify_context']['operation']['name']
            return name == 'cloudify.interfaces.cloudify_agent.delete'
        except (KeyError, TypeError):
            return False

    def _clear_bound_exchanges_cache(self):
        for handler in self.workflow_ctx.amqp_handlers:
            handler._bound.clear()

    def delete_queue(self):
        self._connection.channel_method(
            'queue_delete', queue=self._queue_name, if_empty=True, wait=True)

    def _task_callback(self, task, response):
        self._logger.debug('[%s] Response received - %s', task.id, response)
        try:
            if not response or task.is_terminated:
                return

            error = response.get('error')
            if error:
                exception = deserialize_known_exception(error)
                if isinstance(exception, exceptions.OperationRetry):
                    state = TASK_RESCHEDULED
                else:
                    state = TASK_FAILED
                self._set_task_state(task, state, exception=exception)
                task.async_result.result = exception
            else:
                state = TASK_SUCCEEDED
                result = response.get('result')
                self._set_task_state(task, state, result=result)
                task.async_result.result = result
        except Exception:
            self._logger.error('Error occurred while processing task',
                               exc_info=True)
            raise

    def _set_task_state(self, task, state, **kwargs):
        with current_workflow_ctx.push(task.workflow_context):
            task.set_state(state, **kwargs)


class _TaskDispatcher(object):
    def __init__(self, workflow_ctx):
        self.workflow_ctx = workflow_ctx
        self._logger = logging.getLogger('dispatch')
        self._clients = {}

    def cleanup(self):
        for client, handler in self._clients.values():
            handler.delete_queue()
            client.close()

    def get_client(self, target):
        if target == MGMTWORKER_QUEUE:
            if None not in self._clients:
                client = amqp_client.get_client()
                handler = _WorkflowTaskHandler(self.workflow_ctx)
                client.add_handler(handler)
                client.consume_in_thread()
                self._clients[None] = (client, handler)
            client, handler = self._clients[None]
        else:
            tenant = utils.get_tenant()
            if tenant.rabbitmq_vhost not in self._clients:
                client = amqp_client.get_client(
                    amqp_user=tenant.rabbitmq_username,
                    amqp_pass=tenant.rabbitmq_password,
                    amqp_vhost=tenant.rabbitmq_vhost
                )
                handler = _WorkflowTaskHandler(self.workflow_ctx)
                client.add_handler(handler)
                client.consume_in_thread()
                self._clients[tenant.rabbitmq_vhost] = (client, handler)
            client, handler = self._clients[tenant.rabbitmq_vhost]
        return client, handler

    def send_task(self, task, target, queue):
        client, handler = self.get_client(target)
        if target != MGMTWORKER_QUEUE and \
                not is_agent_alive(target, client, connect=False):
            raise exceptions.RecoverableError(
                'Timed out waiting for agent: {0}'.format(target))

        message = {
            'id': task.id,
            'cloudify_task': {'kwargs': task.kwargs}
        }
        handler.publish(queue, message, routing_key='operation',
                        correlation_id=task.id)
        self._logger.debug('Task [%s] sent', task.id)

    def wait_for_result(self, task, target):
        client, handler = self.get_client(target)
        handler.wait_for_task(task)


# Local/Remote Handlers

class CloudifyWorkflowContextHandler(object):

    def __init__(self, workflow_ctx):
        self.workflow_ctx = workflow_ctx

    def cleanup(self, finished):
        pass

    def get_context_logging_handler(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_node_logging_handler(self, workflow_node_instance):
        raise NotImplementedError('Implemented by subclasses')

    @property
    def bootstrap_context(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_send_task_event_func(self, task):
        raise NotImplementedError('Implemented by subclasses')

    @property
    def operation_cloudify_context(self):
        raise NotImplementedError('Implemented by subclasses')

    def send_workflow_event(self, event_type, message=None, args=None,
                            additional_context=None):
        raise NotImplementedError('Implemented by subclasses')

    def download_deployment_resource(self,
                                     resource_path,
                                     target_path=None):
        raise NotImplementedError('Implemented by subclasses')

    def start_deployment_modification(self, nodes):
        raise NotImplementedError('Implemented by subclasses')

    def finish_deployment_modification(self, modification):
        raise NotImplementedError('Implemented by subclasses')

    def rollback_deployment_modification(self, modification):
        raise NotImplementedError('Implemented by subclasses')

    def list_deployment_modifications(self, status):
        raise NotImplementedError('Implemented by subclasses')

    def scaling_groups(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_operations(self, graph_id):
        raise NotImplementedError('Implemented by subclasses')

    def get_tasks_graph(self, execution_id, name):
        raise NotImplementedError('Implemented by subclasses')

    def update_operation(self, operation_id, state,
                         result=None, exception=None):
        raise NotImplementedError('Implemented by subclasses')

    def store_tasks_graph(self, execution_id, name, operations):
        raise NotImplementedError('Implemented by subclasses')

    def store_operation(self, graph_id, dependencies,
                        id, name, type, parameters, **kwargs):
        raise NotImplementedError('Implemented by subclasses')

    def remove_operation(self, operation_id):
        raise NotImplementedError('Implemented by subclasses')

    def get_execution(self, execution_id):
        raise NotImplementedError('Implemented by subclasses')

    def get_nodes(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_node_instances(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_plugin(self, plugin_spec):
        raise NotImplementedError('Implemented by subclasses')


class RemoteContextHandler(CloudifyWorkflowContextHandler):
    def __init__(self, *args, **kwargs):
        super(RemoteContextHandler, self).__init__(*args, **kwargs)
        self._dispatcher = _TaskDispatcher(self.workflow_ctx)
        self.rest_client = get_rest_client()
        self._plugins_cache = {}

    def cleanup(self, finished):
        if finished:
            self._dispatcher.cleanup()

    @property
    def bootstrap_context(self):
        return get_bootstrap_context()

    def get_send_task_event_func(self, task):
        return events.send_task_event_func_remote

    def send_task(self, *args, **kwargs):
        return self._dispatcher.send_task(*args, **kwargs)

    def wait_for_result(self, *args, **kwargs):
        return self._dispatcher.wait_for_result(*args, **kwargs)

    @property
    def operation_cloudify_context(self):
        return {'local': False,
                'bypass_maintenance': utils.get_is_bypass_maintenance(),
                'rest_token': utils.get_rest_token(),
                'workflow_parameters': utils.get_workflow_parameters(),
                'execution_token': utils.get_execution_token(),
                'execution_creator_username':
                    utils.get_execution_creator_username()}

    def download_deployment_resource(self,
                                     blueprint_id,
                                     deployment_id,
                                     tenant_name,
                                     resource_path,
                                     target_path=None):
        logger = self.workflow_ctx.logger
        return download_resource(blueprint_id=blueprint_id,
                                 deployment_id=deployment_id,
                                 tenant_name=tenant_name,
                                 resource_path=resource_path,
                                 target_path=target_path,
                                 logger=logger)

    def get_operations(self, graph_id):
        ops = []
        offset = 0
        while True:
            operations = self.rest_client.operations.list(
                graph_id, _offset=offset)
            ops += operations.items
            if len(ops) < operations.metadata.pagination.total:
                offset += operations.metadata.pagination.size
            else:
                break
        return ops

    def update_operation(self, operation_id, state,
                         result=None, exception=None):
        exception_causes = None
        exception_text = None
        try:
            json.dumps(result)
        except ValueError:
            # it's not serializable! just store a null (as is back-compatible)
            result = None
        if exception is not None:
            exception_text = str(exception)
            exception_causes = getattr(exception, 'causes', None)
        self.rest_client.operations.update(
            operation_id, state=state, result=result,
            exception=exception_text, exception_causes=exception_causes)

    def get_tasks_graph(self, execution_id, name):
        graphs = self.rest_client.tasks_graphs.list(execution_id, name)
        if graphs:
            return graphs[0]

    def store_tasks_graph(self, execution_id, name, operations):
        return self.rest_client.tasks_graphs.create(
            execution_id, name, operations)

    def store_operation(self, graph_id, dependencies,
                        id, name, type, parameters, **kwargs):
        self.rest_client.operations.create(
            operation_id=id,
            graph_id=graph_id,
            name=name,
            type=type,
            dependencies=dependencies,
            parameters=parameters)

    def remove_operation(self, operation_id):
        self.rest_client.operations.delete(operation_id)

    def get_execution(self, execution_id):
        return self.rest_client.executions.get(execution_id)

    def get_nodes(self):
        if self.workflow_ctx.workflow_id in ('create_deployment_environment',
                                             'delete_deployment_environment'):
            # If creating a deployment environment, there are clearly
            # no nodes/instances yet.
            # If deleting it we don't care about the nodes/instances,
            # and trying to retrieve them might cause problems if
            # deployment environment creation had a really bad time.
            return []
        dep = self.workflow_ctx.deployment
        return self.rest_client.nodes.list(
            deployment_id=dep.id,
            _get_all_results=True,
            evaluate_functions=dep.runtime_only_evaluation
        )

    def get_node_instances(self):
        if self.workflow_ctx.workflow_id in ('upload_blueprint',
                                             'create_deployment_environment',
                                             'delete_deployment_environment'):
            return []
        dep = self.workflow_ctx.deployment
        return self.rest_client.node_instances.list(
            deployment_id=dep.id,
            _get_all_results=True,
        )

    def get_plugin(self, plugin):
        key = (plugin.get('package_name'), plugin.get('package_version'))
        if key not in self._plugins_cache:
            filter_plugin = {'package_name': plugin.get('package_name'),
                             'package_version': plugin.get('package_version')}
            managed_plugins = self.rest_client.plugins.list(**filter_plugin)
            if managed_plugins:
                plugin['visibility'] = managed_plugins[0]['visibility']
                plugin['tenant_name'] = managed_plugins[0]['tenant_name']
            self._plugins_cache[key] = plugin
        return self._plugins_cache[key]


class RemoteCloudifyWorkflowContextHandler(RemoteContextHandler):
    _scaling_groups = None

    def get_node_logging_handler(self, workflow_node_instance):
        return CloudifyWorkflowNodeLoggingHandler(
            workflow_node_instance, out_func=logs.manager_log_out)

    def get_context_logging_handler(self):
        return CloudifyWorkflowLoggingHandler(self.workflow_ctx,
                                              out_func=logs.manager_log_out)

    def download_deployment_resource(self,
                                     resource_path,
                                     target_path=None):
        return super(RemoteCloudifyWorkflowContextHandler, self) \
            .download_deployment_resource(
                blueprint_id=self.workflow_ctx.blueprint.id,
                deployment_id=self.workflow_ctx.deployment.id,
                tenant_name=self.workflow_ctx.tenant_name,
                resource_path=resource_path,
                target_path=target_path)

    def start_deployment_modification(self, nodes):
        deployment_id = self.workflow_ctx.deployment.id
        modification = self.rest_client.deployment_modifications.start(
            deployment_id=deployment_id,
            nodes=nodes,
            context={
                'blueprint_id': self.workflow_ctx.blueprint.id,
                'deployment_id': deployment_id,
                'execution_id': self.workflow_ctx.execution_id,
                'workflow_id': self.workflow_ctx.workflow_id,
            })
        return Modification(self.workflow_ctx, modification)

    def finish_deployment_modification(self, modification):
        self.rest_client.deployment_modifications.finish(modification.id)

    def rollback_deployment_modification(self, modification):
        self.rest_client.deployment_modifications.rollback(modification.id)

    def list_deployment_modifications(self, status):
        deployment_id = self.workflow_ctx.deployment.id
        modifications = self.rest_client.deployment_modifications.list(
            deployment_id=deployment_id,
            status=status)
        return [Modification(self.workflow_ctx, m) for m in modifications]

    def send_workflow_event(self, event_type, message=None, args=None,
                            additional_context=None):
        send_workflow_event(self.workflow_ctx,
                            event_type=event_type,
                            message=message,
                            args=args,
                            additional_context=additional_context,
                            out_func=logs.manager_event_out)

    @property
    def scaling_groups(self):
        if not self._scaling_groups:
            deployment_id = self.workflow_ctx.deployment.id
            deployment = self.rest_client.deployments.get(
                deployment_id, _include=['scaling_groups'])
            self._scaling_groups = deployment['scaling_groups']
        return self._scaling_groups


class SystemWideWfRemoteContextHandler(RemoteContextHandler):

    def get_context_logging_handler(self):
        return SystemWideWorkflowLoggingHandler(self.workflow_ctx,
                                                out_func=logs.manager_log_out)

    def send_workflow_event(self, event_type, message=None, args=None,
                            additional_context=None):
        send_sys_wide_wf_event(self.workflow_ctx,
                               event_type=event_type,
                               message=message,
                               args=args,
                               additional_context=additional_context,
                               out_func=logs.manager_event_out)


class LocalCloudifyWorkflowContextHandler(CloudifyWorkflowContextHandler):

    def __init__(self, workflow_ctx, storage):
        super(LocalCloudifyWorkflowContextHandler, self).__init__(
            workflow_ctx)
        self.storage = storage
        self._send_task_event_func = None

    def get_context_logging_handler(self):
        return CloudifyWorkflowLoggingHandler(self.workflow_ctx,
                                              out_func=logs.stdout_log_out)

    def get_node_logging_handler(self, workflow_node_instance):
        return CloudifyWorkflowNodeLoggingHandler(workflow_node_instance,
                                                  out_func=logs.stdout_log_out)

    @property
    def bootstrap_context(self):
        return {}

    def get_send_task_event_func(self, task):
        return events.send_task_event_func_local

    @property
    def operation_cloudify_context(self):
        return {'local': True,
                'storage': self.storage}

    def send_workflow_event(self, event_type, message=None, args=None,
                            additional_context=None):
        send_workflow_event(self.workflow_ctx,
                            event_type=event_type,
                            message=message,
                            args=args,
                            additional_context=additional_context,
                            out_func=logs.stdout_event_out)

    def download_deployment_resource(self,
                                     resource_path,
                                     target_path=None):
        return self.storage.download_resource(resource_path=resource_path,
                                              target_path=target_path)

    @property
    def scaling_groups(self):
        return self.storage.plan.get('scaling_groups', {})

    # resumable workflows operations - not implemented for local
    def get_tasks_graph(self, execution_id, name):
        pass

    def update_operation(self, operation_id, state,
                         result=None, exception=None):
        pass

    def store_tasks_graph(self, execution_id, name, operations):
        pass

    def store_operation(self, graph_id, dependencies,
                        id, name, type, parameters, **kwargs):
        pass

    def remove_operation(self, operation_id):
        pass

    def get_execution(self, execution_id):
        return self.storage.get_execution(execution_id)

    def get_nodes(self):
        return self.storage.get_nodes()

    def get_node_instances(self):
        return self.storage.get_node_instances()

    def get_plugin(self, plugin):
        return plugin


class Modification(object):

    def __init__(self, workflow_ctx, modification):
        self._raw_modification = modification
        self.workflow_ctx = workflow_ctx
        node_instances = modification.node_instances
        added_raw_nodes = []
        seen_ids = set()
        for instance in node_instances.added_and_related:
            if instance.node_id not in seen_ids:
                added_raw_nodes.append(
                    workflow_ctx.get_node(instance.node_id)._node)
                seen_ids.add(instance.node_id)

        added_raw_node_instances = node_instances.added_and_related
        self._added = ModificationNodes(self,
                                        added_raw_nodes,
                                        added_raw_node_instances)

        removed_raw_nodes = []
        seen_ids = set()
        for instance in node_instances.removed_and_related:
            if instance.node_id not in seen_ids:
                removed_raw_nodes.append(
                    workflow_ctx.get_node(instance.node_id)._node)
                seen_ids.add(instance.node_id)

        removed_raw_node_instances = node_instances.removed_and_related
        self._removed = ModificationNodes(self,
                                          removed_raw_nodes,
                                          removed_raw_node_instances)

    @property
    def added(self):
        """
        :return: Added and related nodes
        :rtype: ModificationNodes
        """
        return self._added

    @property
    def removed(self):
        """
        :return: Removed and related nodes
        :rtype: ModificationNodes
        """
        return self._removed

    @property
    def id(self):
        return self._raw_modification.id

    def finish(self):
        """Finish deployment modification process"""
        self.workflow_ctx.internal.handler.finish_deployment_modification(
            self._raw_modification)

    def rollback(self):
        """Rollback deployment modification process"""
        self.workflow_ctx.internal.handler.rollback_deployment_modification(
            self._raw_modification)


class ModificationNodes(WorkflowNodesAndInstancesContainer):
    def __init__(self, modification, raw_nodes, raw_node_instances):
        super(ModificationNodes, self).__init__(
            modification.workflow_ctx,
            raw_nodes,
            raw_node_instances
        )


class WorkflowDeploymentContext(context.DeploymentContext):

    def __init__(self, cloudify_context, workflow_ctx):
        super(WorkflowDeploymentContext, self).__init__(cloudify_context)
        self.workflow_ctx = workflow_ctx

    def start_modification(self, nodes):
        """Start deployment modification process

        :param nodes: Modified nodes specification
        :return: Workflow modification wrapper
        :rtype: Modification
        """
        handler = self.workflow_ctx.internal.handler
        modification = handler.start_deployment_modification(nodes)
        self.workflow_ctx.refresh_node_instances()
        return modification

    def list_started_modifications(self):
        """List modifications already started (and not finished)

        :return: A list of workflow modification wrappers
        :rtype: list of Modification
        """
        handler = self.workflow_ctx.internal.handler
        return handler.list_deployment_modifications(
            DeploymentModificationState.STARTED)

    @property
    def scaling_groups(self):
        return self.workflow_ctx.internal.handler.scaling_groups


def task_config(fn=None, **arguments):
    if fn is not None:
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            return fn(*args, **kwargs)
        wrapper.workflow_task_config = arguments
        return wrapper
    else:
        def partial_wrapper(func):
            return task_config(func, **arguments)
        return partial_wrapper
