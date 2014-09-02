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


import copy
import uuid
import importlib
import logging
import sys
import threading
import Queue
import os
import tempfile
import copy

from cloudify.manager import (get_node_instance,
                              update_node_instance,
                              update_execution_status,
                              get_bootstrap_context,
                              get_rest_client)
from cloudify.workflows.tasks import (RemoteWorkflowTask,
                                      LocalWorkflowTask,
                                      NOPLocalWorkflowTask,
                                      DEFAULT_TOTAL_RETRIES,
                                      DEFAULT_RETRY_INTERVAL)
from cloudify.workflows import events
from cloudify.workflows.tasks_graph import TaskDependencyGraph
from cloudify.logs import (CloudifyWorkflowLoggingHandler,
                           CloudifyWorkflowNodeLoggingHandler,
                           init_cloudify_logger,
                           send_workflow_event,
                           send_workflow_node_event)
from cloudify_rest_client.node_instances import (NodeInstance as
                                                    RestNodeInstance)


class CloudifyWorkflowRelationshipInstance(object):
    """
    A node instance relationship instance

    :param ctx: a CloudifyWorkflowContext instance
    :param node_instance: a CloudifyWorkflowNodeInstance instance
    :param relationship_instance: A relationship dict from a NodeInstance
           instance (of the rest client model)
    """

    def __init__(self, ctx, node_instance, relationship_instance):
        self.ctx = ctx
        self.node_instance = node_instance
        self._relationship_instance = relationship_instance
        self._relationship = node_instance.node.get_relationship(
            relationship_instance['target_name'])

    @property
    def target_id(self):
        """The relationship target node id"""
        return self._relationship_instance.get('target_id')

    @property
    def target_node_instance(self):
        """The relationship target node WorkflowContextNodeInstance instance"""
        return self.ctx.get_node_instance(self.target_id)

    @property
    def relationship(self):
        """The relationship object for this relationship instance"""
        return self._relationship

    def execute_source_operation(self,
                                 operation,
                                 kwargs=None,
                                 allow_kwargs_override=False):
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
            allow_kwargs_override=allow_kwargs_override)

    def execute_target_operation(self,
                                 operation,
                                 kwargs=None,
                                 allow_kwargs_override=False):
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
            allow_kwargs_override=allow_kwargs_override)


class CloudifyWorkflowRelationship(object):
    """
    A node relationship

    :param ctx: a CloudifyWorkflowContext instance
    :param node: a CloudifyWorkflowNode instance
    :param relationship: a relationship dict from a Node instance (of the
           rest client mode)
    """

    def __init__(self, ctx, node, relationship):
        self.ctx = ctx
        self.node = node
        self._relationship = relationship

    @property
    def target_id(self):
        """The relationship target node id"""
        return self._relationship.get('target_id')

    @property
    def target_node(self):
        """The relationship target node WorkflowContextNode instance"""
        return self.ctx.get_node(self.target_id)

    @property
    def source_operations(self):
        """The relationship source operations"""
        return self._relationship.get('source_operations', {})

    @property
    def target_operations(self):
        """The relationship target operations"""
        return self._relationship.get('target_operations', {})


class CloudifyWorkflowNodeInstance(object):
    """
    A plan node instance

    :param ctx: a CloudifyWorkflowContext instance
    :param node: a CloudifyWorkflowContextNode instance
    :param node_instance: a NodeInstance (rest client response model)
    """

    def __init__(self, ctx, node, node_instance):
        self.ctx = ctx
        self._node = node
        self._node_instance = node_instance
        self._relationship_instances = {
            relationship_instance['target_id']:
                CloudifyWorkflowRelationshipInstance(self.ctx,
                                                     self,
                                                     relationship_instance)
            for relationship_instance in node_instance.relationships
        }
        # adding the node instance to the node instances map
        node._node_instances[self.id] = self

        self._logger = None

    def set_state(self, state, runtime_properties=None):
        """
        Set the node state

        :param state: The node state
        :return: the state set
        """
        set_state_task = self.ctx.internal.handler.get_set_state_task(
            self, state, runtime_properties)

        return self.ctx.local_task(
            local_task=set_state_task,
            node=self,
            info=state)

    def get_state(self):
        """
        Get the node state

        :return: The node state
        """
        get_state_task = self.ctx.internal.handler.get_get_state_task(self)
        return self.ctx.local_task(
            local_task=get_state_task,
            node=self)

    def send_event(self, event, additional_context=None):
        """
        Sends a workflow node event to RabbitMQ

        :param event: The event
        :param additional_context: additional context to be added to the
               context
        """
        send_event_task = self.ctx.internal.handler.get_send_node_event_task(
            self, event, additional_context)
        return self.ctx.local_task(
            local_task=send_event_task,
            node=self,
            info=event)

    def execute_operation(self,
                          operation,
                          kwargs=None,
                          allow_kwargs_override=False):
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
            allow_kwargs_override=allow_kwargs_override)

    @property
    def id(self):
        """The node instance id"""
        return self._node_instance.id

    @property
    def node_id(self):
        """The node id (this instance is an instance of that node)"""
        return self._node_instance.node_id

    @property
    def relationships(self):
        """The node relationships"""
        return self._relationship_instances.itervalues()

    @property
    def node(self):
        """The node object for this node instance"""
        return self._node

    @property
    def logger(self):
        """A logger for this workflow node"""
        if self._logger is None:
            self._logger = self._init_cloudify_logger()
        return self._logger

    def _init_cloudify_logger(self):
        logger_name = self.id if self.id is not None \
            else 'cloudify_workflow_node'
        logging_handler = \
            self.ctx.internal.handler.get_node_logging_handler(self)
        return init_cloudify_logger(logging_handler, logger_name)


class CloudifyWorkflowNode(object):
    """
    A plan node instance

    :param ctx: a CloudifyWorkflowContext instance
    :param node: a Node instance (rest client response model)
    """

    def __init__(self, ctx, node):
        self.ctx = ctx
        self._node = node
        self._relationships = {
            relationship['target_id']: CloudifyWorkflowRelationship(
                self.ctx, self, relationship)
            for relationship in node.relationships}
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
    def relationships(self):
        """The node relationships"""
        return self._relationships.itervalues()

    @property
    def operations(self):
        """The node operations"""
        return self._node.operations

    @property
    def instances(self):
        """The node instances"""
        return self._node_instances.itervalues()

    def get_relationship(self, target_id):
        """Get a node relationship by its target id"""
        return self._relationships.get(target_id)


class CloudifyWorkflowContext(object):
    """
    A context used in workflow operations

    :param ctx: a cloudify_context workflow dict
    """

    def __init__(self, ctx):
        # Before anything else so property access will work properly
        self._context = ctx

        if self.local:
            nodes = ctx.pop('nodes')
            node_instances = ctx.pop('node_instances')
            resources_root = ctx.pop('resources_root')
            handler = LocalCloudifyWorkflowContextHandler(self,
                                                          node_instances,
                                                          resources_root)
        else:
            rest = get_rest_client()
            nodes = rest.nodes.list(self.deployment_id)
            node_instances = rest.node_instances.list(self.deployment_id)
            handler = RemoteCloudifyWorkflowContextHandler(self)

        self._nodes = {node.id: CloudifyWorkflowNode(self, node) for
                       node in nodes}
        self._node_instances = {
            instance.id: CloudifyWorkflowNodeInstance(
                self, self._nodes[instance.node_id], instance)
            for instance in node_instances}

        self._logger = None

        self._internal = CloudifyWorkflowContextInternal(self, handler)

    def graph_mode(self):
        """
        Switch the workflow context into graph mode

        :return: A task dependency graph instance
        """
        if next(self.internal.task_graph.tasks_iter(), None) is not None:
            raise RuntimeError('Cannot switch to graph mode when tasks have'
                               'already been executed')

        self.internal.graph_mode = True
        return self.internal.task_graph

    @property
    def internal(self):
        return self._internal

    @property
    def nodes(self):
        """The plan node instances"""
        return self._nodes.itervalues()

    @property
    def deployment_id(self):
        """The deployment id"""
        return self._context.get('deployment_id')

    @property
    def blueprint_id(self):
        """The blueprint id"""
        return self._context.get('blueprint_id')

    @property
    def execution_id(self):
        """The execution id"""
        return self._context.get('execution_id')

    @property
    def workflow_id(self):
        """The workflow id"""
        return self._context.get('workflow_id')

    @property
    def local(self):
        """Is the workflow running in a local or remote context"""
        return self._context.get('local', False)

    @property
    def logger(self):
        """A logger for this workflow"""
        if self._logger is None:
            self._logger = self._init_cloudify_logger()
        return self._logger

    def _init_cloudify_logger(self):
        logger_name = self.workflow_id if self.workflow_id is not None \
            else 'cloudify_workflow'
        logging_handler = self.internal.handler.get_context_logging_handler()
        return init_cloudify_logger(logging_handler, logger_name)

    def send_event(self, event, event_type='workflow_stage',
                   args=None,
                   additional_context=None):
        """
        Sends a workflow event to RabbitMQ

        :param event: The event
        :param event_type: The event type
        :param args: additional arguments that may be added to the message
        :param additional_context: additional context to be added to the
               context
        """

        send_event_task = self.internal.handler.get_send_workflow_event_task(
            event, event_type, args, additional_context)

        return self.local_task(
            local_task=send_event_task,
            info=event)

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
        Get a node by its id

        :param node_instance_id: The node instance id
        :return: a CloudifyWorkflowNode instance for the node or None if
                 not found
        """
        return self._node_instances.get(node_instance_id)

    def _execute_operation(self,
                           operation,
                           node_instance,
                           operations,
                           related_node_instance=None,
                           kwargs=None,
                           allow_kwargs_override=False):
        kwargs = kwargs or {}
        node = node_instance.node
        op_struct = operations.get(operation)
        if op_struct is None:
            return NOPLocalWorkflowTask()
        plugin_name = op_struct['plugin']
        operation_mapping = op_struct['operation']
        operation_properties = op_struct.get('properties', {})
        task_queue = self.internal.handler.get_operation_task_queue(
            node_instance, plugin_name)
        task_name = operation_mapping

        node_context = {
            'node_id': node_instance.id,
            'node_name': node_instance.node_id,
            'node_properties': copy.copy(node.properties),
            'plugin': plugin_name,
            'operation': operation,
            'relationships': [rel.target_id
                              for rel in node_instance.relationships]
        }
        if related_node_instance is not None:
            node_context['related'] = {
                'node_id': related_node_instance.id,
                'node_properties': copy.copy(
                    related_node_instance.node.properties)
            }

        final_kwargs = self._merge_dicts(merged_from=kwargs,
                                         merged_into=operation_properties,
                                         allow_override=allow_kwargs_override)

        return self.execute_task(task_name,
                                 task_queue=task_queue,
                                 kwargs=final_kwargs,
                                 node_context=node_context)

    @staticmethod
    def _merge_dicts(merged_from, merged_into, allow_override=False):
        result = copy.copy(merged_into)
        for key, value in merged_from.iteritems():
            if not allow_override and key in merged_into:
                raise RuntimeError('Duplicate definition of {} in operation'
                                   ' properties and in kwargs. To allow '
                                   'redefinition, pass '
                                   '"allow_kwargs_override" to '
                                   '"execute_operation"'.format(key))
            result[key] = value
        return result

    def update_execution_status(self, new_status):
        """
        Updates the execution status to new_status.
        Note that the workflow status gets automatically updated before and
        after its run (whether the run succeeded or failed)
        """
        update_execution_status_task = \
            self.internal.handler.get_update_execution_status_task(new_status)

        return self.local_task(
            local_task=update_execution_status_task,
            info=new_status)

    def _build_cloudify_context(self,
                                task_id,
                                task_queue,
                                task_name,
                                node_context):
        node_context = node_context or {}
        context = {
            '__cloudify_context': '0.3',
            'task_id': task_id,
            'task_name': task_name,
            'task_target': task_queue,
            'blueprint_id': self.blueprint_id,
            'deployment_id': self.deployment_id,
            'execution_id': self.execution_id,
            'workflow_id': self.workflow_id,
        }
        context.update(node_context)
        context.update(self.internal.handler.operation_cloudify_context)
        return context

    def execute_task(self,
                     task_name,
                     task_queue=None,
                     kwargs=None,
                     node_context=None):
        """
        Execute a task

        :param task_name: the task named
        :param task_queue: the task queue, if None runs the task locally
        :param kwargs: optional kwargs to be passed to the task
        :param node_context: Used internally by node.execute_operation
        """
        kwargs = kwargs or {}
        task_id = str(uuid.uuid4())
        cloudify_context = self._build_cloudify_context(
            task_id,
            task_queue,
            task_name,
            node_context)
        kwargs['__cloudify_context'] = cloudify_context

        if task_queue is None:
            # Local task
            values = task_name.split('.')
            module_name = '.'.join(values[:-1])
            method_name = values[-1]
            module = importlib.import_module(module_name)
            task = getattr(module, method_name)
            return self.local_task(local_task=task,
                                   info=task_name,
                                   name=task_name,
                                   kwargs=kwargs,
                                   task_id=task_id)
        else:
            # Remote task
            # Import here because this only applies to remote tasks execution
            # environment
            import celery

            task = celery.subtask(task_name,
                                  kwargs=kwargs,
                                  queue=task_queue,
                                  immutable=True)
            return self.remote_task(task=task,
                                    cloudify_context=cloudify_context,
                                    task_id=task_id)

    def local_task(self,
                   local_task,
                   node=None,
                   info=None,
                   kwargs=None,
                   task_id=None,
                   name=None):
        """
        Create a local workflow task

        :param local_task: A callable implementation for the task
        :param node: A node if this task is called in a node context
        :param info: Additional info that will be accessed and included
                     in log messages
        :param kwargs: kwargs to pass to the local_task when invoked
        :param task_id: The task id
        """
        return self._process_task(
            LocalWorkflowTask(local_task=local_task,
                              workflow_context=self,
                              node=node,
                              info=info,
                              kwargs=kwargs,
                              task_id=task_id,
                              name=name,
                              **self.internal.get_task_configuration()))

    def remote_task(self,
                    task,
                    cloudify_context,
                    task_id):
        """
        Create a remote workflow task

        :param task: The underlying celery task
        :param cloudify_context: A dict for creating the CloudifyContext
                                 used by the called task
        :param task_id: The task id
        """
        return self._process_task(
            RemoteWorkflowTask(task=task,
                               cloudify_context=cloudify_context,
                               workflow_context=self,
                               task_id=task_id,
                               **self.internal.get_task_configuration()))

    def _process_task(self, task):
        if self.internal.graph_mode:
            return task
        else:
            self.internal.task_graph.add_task(task)
            return task.apply_async()


class CloudifyWorkflowContextInternal(object):

    def __init__(self, workflow_context, handler):
        self.workflow_context = workflow_context
        self.handler = handler
        self._bootstrap_context = None
        self._graph_mode = False
        # the graph is always created internally for events to work properly
        # when graph mode is turned on this instance is returned to the user.
        self._task_graph = TaskDependencyGraph(workflow_context)

        # events related
        self._event_monitor = None
        self._send_task_event_func = events.send_task_event_local_func(
            self.workflow_context.logger)

        # local task processing
        self.local_tasks_processor = LocalTasksProcessing(
            thread_pool_size=1)

    def get_task_configuration(self):
        bootstrap_context = self._get_bootstrap_context()
        workflows = bootstrap_context.get('workflows', {})
        total_retries = workflows.get('task_retries', DEFAULT_TOTAL_RETRIES)
        retry_interval = workflows.get('task_retry_interval',
                                       DEFAULT_RETRY_INTERVAL)
        return dict(total_retries=total_retries,
                    retry_interval=retry_interval)

    def _get_bootstrap_context(self):
        return self.handler.bootstrap_context

    @property
    def task_graph(self):
        return self._task_graph

    @property
    def graph_mode(self):
        return self._graph_mode

    @graph_mode.setter
    def graph_mode(self, graph_mode):
        self._graph_mode = graph_mode

    @property
    def event_monitor(self):
        return self._event_monitor

    @event_monitor.setter
    def event_monitor(self, value):
        self._event_monitor = value

    def start_event_monitor(self):
        """
        Start an event monitor in its own thread for handling task events
        defined in the task dependency graph

        """
        monitor = events.Monitor(self.task_graph)
        thread = threading.Thread(target=monitor.capture)
        thread.daemon = True
        thread.start()
        self.event_monitor = thread

    def send_task_event(self, state, task, event=None):
        send_task_event_func = self.handler.get_send_task_event_func(task)
        events.send_task_event(state, task, send_task_event_func, event)

    def start_local_tasks_processing(self):
        self.local_tasks_processor.start()

    def stop_local_tasks_processing(self):
        self.local_tasks_processor.stop()

    def add_local_task(self, task):
        self.local_tasks_processor.add_task(task)


class LocalTasksProcessing(object):

    def __init__(self, thread_pool_size=1):
        self._local_tasks_queue = Queue.Queue()
        self._local_task_processing_pool = [
            threading.Thread(target=self._process_local_task)
            for _ in range(thread_pool_size)]
        self.stopped = False

    def start(self):
        for thread in self._local_task_processing_pool:
            thread.daemon = True
            thread.start()

    def stop(self):
        self.stopped = True

    def add_task(self, task):
        self._local_tasks_queue.put(task)

    def _process_local_task(self):
        while not self.stopped:
            try:
                task = self._local_tasks_queue.get(timeout=1)
                task()
            except Queue.Empty:
                pass

# Local/Remote Handlers


class CloudifyWorkflowContextHandler(object):

    def __init__(self, workflow_ctx):
        self.workflow_ctx = workflow_ctx

    def get_context_logging_handler(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_node_logging_handler(self, workflow_node_instance):
        raise NotImplementedError('Implemented by subclasses')

    @property
    def bootstrap_context(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_send_task_event_func(self, task):
        raise NotImplementedError('Implemented by subclasses')

    def get_update_execution_status_task(self, new_status):
        raise NotImplementedError('Implemented by subclasses')

    def get_send_node_event_task(self, workflow_node_instance,
                                 event, additional_context=None):
        raise NotImplementedError('Implemented by subclasses')

    def get_send_workflow_event_task(self, event, event_type, args,
                                     additional_context=None):
        raise NotImplementedError('Implemented by subclasses')

    def get_operation_task_queue(self, workflow_node_instance, plugin_name):
        raise NotImplementedError('Implemented by subclasses')

    @property
    def operation_cloudify_context(self):
        raise NotImplementedError('Implemented by subclasses')

    def get_set_state_task(self,
                           workflow_node_instance,
                           state,
                           runtime_properties):
        raise NotImplementedError('Implemented by subclasses')

    def get_get_state_task(self, workflow_node_instance):
        raise NotImplementedError('Implemented by subclasses')


class RemoteCloudifyWorkflowContextHandler(CloudifyWorkflowContextHandler):

    def __init__(self, workflow_ctx):
        super(RemoteCloudifyWorkflowContextHandler, self).__init__(
            workflow_ctx)

    def get_context_logging_handler(self):
        return CloudifyWorkflowLoggingHandler(self.workflow_ctx)

    def get_node_logging_handler(self, workflow_node_instance):
        return CloudifyWorkflowNodeLoggingHandler(workflow_node_instance)

    @property
    def bootstrap_context(self):
        return get_bootstrap_context()

    def get_send_task_event_func(self, task):
        if task.is_remote():
            send_task_event_func = events.send_task_event_remote_task_func
        else:
            send_task_event_func = events.send_task_event_local_task_func
        return send_task_event_func

    def get_update_execution_status_task(self, new_status):
        def update_execution_status_task():
            update_execution_status(self.workflow_ctx.execution_id, new_status)
        return update_execution_status_task

    def get_send_node_event_task(self, workflow_node_instance,
                                 event, additional_context=None):
        def send_event_task():
            send_workflow_node_event(ctx=workflow_node_instance,
                                     event_type='workflow_node_event',
                                     message=event,
                                     additional_context=additional_context)
        return send_event_task

    def get_send_workflow_event_task(self, event, event_type, args,
                                     additional_context=None):
        def send_event_task():
            send_workflow_event(ctx=self.workflow_ctx,
                                event_type=event_type,
                                message=event,
                                args=args,
                                additional_context=additional_context)
        return send_event_task

    def get_operation_task_queue(self, workflow_node_instance, plugin_name):
        workflow_node = workflow_node_instance.node
        rest_node = workflow_node._node
        rest_node_instance = workflow_node_instance._node_instance
        task_queue = 'cloudify.management'
        if rest_node.plugins[plugin_name]['agent_plugin'] == 'true':
            task_queue = rest_node_instance.host_id
        elif rest_node.plugins[plugin_name]['manager_plugin'] == 'true':
            task_queue = self.workflow_ctx.deployment_id
        return task_queue

    @property
    def operation_cloudify_context(self):
        return {'local': False}

    def get_set_state_task(self,
                           workflow_node_instance,
                           state,
                           runtime_properties):
        def set_state_task():
            node_state = get_node_instance(workflow_node_instance.id)
            node_state.state = state
            if runtime_properties is not None:
                node_state.runtime_properties.update(runtime_properties)
            update_node_instance(node_state)
            return node_state
        return set_state_task

    def get_get_state_task(self, workflow_node_instance):
        def get_state_task():
            return get_node_instance(workflow_node_instance.id).state
        return get_state_task


class LocalCloudifyWorkflowContextHandler(CloudifyWorkflowContextHandler):

    def __init__(self, workflow_ctx, node_instances, resources_root):
        super(LocalCloudifyWorkflowContextHandler, self).__init__(
            workflow_ctx)
        self.storage = LocalCloudifyWorkflowContextStorage(node_instances,
                                                           resources_root)

    def get_context_logging_handler(self):
        return logging.StreamHandler(sys.stdout)

    def get_node_logging_handler(self, workflow_node_instance):
        return logging.StreamHandler(sys.stdout)

    @property
    def bootstrap_context(self):
        return {}

    def get_send_task_event_func(self, task):
        return self.workflow_ctx.internal._send_task_event_func

    def get_update_execution_status_task(self, new_status):
        raise RuntimeError('Update execution status is not supported for '
                           'local workflow execution')

    def get_send_node_event_task(self, workflow_node_instance,
                                 event, additional_context=None):
        def send_event_task():
            workflow_node_instance.logger.info(
                '[{}] {} [additional_context={}]'
                .format(workflow_node_instance.id,
                        event,
                        additional_context or {}))
        return send_event_task

    def get_send_workflow_event_task(self, event, event_type, args,
                                     additional_context=None):
        def send_event_task():
            self.workflow_ctx.logger.info(
                '[{}] {} [additional_context={}]'
                .format(self.workflow_ctx.workflow_id,
                        event,
                        additional_context or {}))
        return send_event_task

    def get_operation_task_queue(self, workflow_node_instance, plugin_name):
        return None

    @property
    def operation_cloudify_context(self):
        return {'local': True,
                'storage': self.storage}

    def get_set_state_task(self,
                           workflow_node_instance,
                           state,
                           runtime_properties):
        def set_state_task():
            self.storage.update_node_instance(
                workflow_node_instance.id,
                runtime_properties,
                state)
        return set_state_task

    def get_get_state_task(self, workflow_node_instance):
        def get_state_task():
            instance = self.storage.get_node_instance(
                workflow_node_instance.id)
            return instance.state
        return get_state_task


class LocalCloudifyWorkflowContextStorage(object):

    def __init__(self, node_instances, resources_root):
        self.node_instances = node_instances
        self.resources_root = resources_root

    def get_resource(self, resource_path):
        with open(os.path.join(self.resources_root, resource_path)) as f:
            return f.read()

    def download_resource(self, resource_path, target_path=None):
        if not target_path:
            target_path = tempfile.mktemp(suffix=os.path.basename(
                resource_path))
        resource = self.get_resource(resource_path)
        with open(target_path, 'w') as f:
            f.write(resource)
        return target_path

    def get_node_instance(self, node_instance_id):
        instance = copy.deepcopy(self._get_node_instance(node_instance_id))
        return RestNodeInstance(instance)

    def update_node_instance(self,
                             node_instance_id,
                             runtime_properties=None,
                             state=None,
                             version=None):
        instance = self._get_node_instance(node_instance_id)
        if runtime_properties is not None:
            instance['runtime_properties'] = runtime_properties
        if state is not None:
            instance['state'] = state
        if version is not None:
            instance['version'] = version

    def _get_node_instance(self, node_instance_id):
        instance = self.node_instances.get(node_instance_id)
        if instance is None:
            raise RuntimeError('Instance {} does not exist'
                               .format(node_instance_id))
        return instance
