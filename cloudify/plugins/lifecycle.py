########
# Copyright (c) 2015 GigaSpaces Technologies Ltd. All rights reserved
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

import itertools

from cloudify import utils
from cloudify import constants
from cloudify.state import workflow_ctx
from cloudify.workflows.tasks_graph import forkjoin, make_or_get_graph
from cloudify.workflows import tasks as workflow_tasks


def install_node_instances(graph, node_instances, related_nodes=None):
    processor = LifecycleProcessor(graph=graph,
                                   node_instances=node_instances,
                                   related_nodes=related_nodes)
    processor.install()


def uninstall_node_instances(graph,
                             node_instances,
                             ignore_failure,
                             related_nodes=None):
    processor = LifecycleProcessor(graph=graph,
                                   node_instances=node_instances,
                                   ignore_failure=ignore_failure,
                                   related_nodes=related_nodes)
    processor.uninstall()


def reinstall_node_instances(graph,
                             node_instances,
                             ignore_failure,
                             related_nodes=None):
    processor = LifecycleProcessor(graph=graph,
                                   node_instances=node_instances,
                                   ignore_failure=ignore_failure,
                                   related_nodes=related_nodes,
                                   name_prefix='reinstall')
    processor.uninstall()
    processor.install()


def execute_establish_relationships(graph,
                                    node_instances,
                                    related_nodes=None,
                                    modified_relationship_ids=None):
    processor = LifecycleProcessor(
        graph=graph,
        related_nodes=node_instances,
        modified_relationship_ids=modified_relationship_ids,
        name_prefix='establish')
    processor.install()


def execute_unlink_relationships(graph,
                                 node_instances,
                                 related_nodes=None,
                                 modified_relationship_ids=None):
    processor = LifecycleProcessor(
        graph=graph,
        related_nodes=node_instances,
        modified_relationship_ids=modified_relationship_ids,
        name_prefix='unlink')
    processor.uninstall()


class LifecycleProcessor(object):

    def __init__(self,
                 graph,
                 node_instances=None,
                 related_nodes=None,
                 modified_relationship_ids=None,
                 ignore_failure=False,
                 name_prefix=''):
        self.graph = graph
        self.node_instances = node_instances or set()
        self.intact_nodes = related_nodes or set()
        self.modified_relationship_ids = modified_relationship_ids or {}
        self.ignore_failure = ignore_failure
        self._name_prefix = name_prefix

    def install(self):
        graph = self._process_node_instances(
            workflow_ctx,
            name=self._name_prefix + 'install',
            node_instance_subgraph_func=install_node_instance_subgraph,
            graph_finisher_func=self._finish_install)
        graph.execute()

    def uninstall(self):
        graph = self._process_node_instances(
            workflow_ctx,
            name=self._name_prefix + 'uninstall',
            node_instance_subgraph_func=uninstall_node_instance_subgraph,
            graph_finisher_func=self._finish_uninstall)
        graph.execute()

    @make_or_get_graph
    def _process_node_instances(self,
                                ctx,
                                node_instance_subgraph_func,
                                graph_finisher_func):
        subgraphs = {}
        for instance in self.node_instances:
            subgraphs[instance.id] = \
                node_instance_subgraph_func(
                    instance, self.graph, ignore_failure=self.ignore_failure)

        for instance in self.intact_nodes:
            subgraphs[instance.id] = self.graph.subgraph(
                'stub_{0}'.format(instance.id))

        graph_finisher_func(self.graph, subgraphs)
        return self.graph

    def _finish_install(self, graph, subgraphs):
        self._finish_subgraphs(
            graph=graph,
            subgraphs=subgraphs,
            intact_op='cloudify.interfaces.relationship_lifecycle.establish',
            install=True)

    def _finish_uninstall(self, graph, subgraphs):
        self._finish_subgraphs(
            graph=graph,
            subgraphs=subgraphs,
            intact_op='cloudify.interfaces.relationship_lifecycle.unlink',
            install=False)

    def _finish_subgraphs(self, graph, subgraphs, intact_op, install):
        # Create task dependencies based on node relationships
        self._add_dependencies(graph=graph,
                               subgraphs=subgraphs,
                               instances=self.node_instances,
                               install=install)

        def intact_on_dependency_added(instance, rel, source_task_sequence):
            if (rel.target_node_instance in self.node_instances or
                    rel.target_node_instance.node_id in
                    self.modified_relationship_ids.get(instance.node_id, {})):
                intact_tasks = _relationship_operations(rel, intact_op)
                for intact_task in intact_tasks:
                    if not install:
                        set_send_node_event_on_error_handler(
                            intact_task, instance)
                    source_task_sequence.add(intact_task)
        # Add operations for intact nodes depending on a node instance
        # belonging to node_instances
        self._add_dependencies(graph=graph,
                               subgraphs=subgraphs,
                               instances=self.intact_nodes,
                               install=install,
                               on_dependency_added=intact_on_dependency_added)

    def _add_dependencies(self, graph, subgraphs, instances, install,
                          on_dependency_added=None):
        subgraph_sequences = dict(
            (instance_id, subgraph.sequence())
            for instance_id, subgraph in subgraphs.items())
        for instance in instances:
            relationships = list(instance.relationships)
            if not install:
                relationships = reversed(relationships)
            for rel in relationships:
                if (rel.target_node_instance in self.node_instances or
                        rel.target_node_instance in self.intact_nodes):
                    source_subgraph = subgraphs[instance.id]
                    target_subgraph = subgraphs[rel.target_id]
                    if install:
                        graph.add_dependency(source_subgraph, target_subgraph)
                    else:
                        graph.add_dependency(target_subgraph, source_subgraph)
                    if on_dependency_added:
                        task_sequence = subgraph_sequences[instance.id]
                        on_dependency_added(instance, rel, task_sequence)


def set_send_node_event_on_error_handler(task, instance):
    def send_node_event_error_handler(tsk):
        instance.send_event('Ignoring task {0} failure'.format(tsk.name))
        return workflow_tasks.HandlerResult.ignore()
    task.on_failure = send_node_event_error_handler


def _skip_nop_operations(task, pre=None, post=None):
    """If `task` is a NOP, then skip pre and post

    Useful for skipping the 'creating node instance' message in case
    no creating is actually going to happen.
    """
    if not task or task.is_nop():
        return []
    if pre is None:
        pre = []
    if post is None:
        post = []
    if not isinstance(pre, list):
        pre = [pre]
    if not isinstance(post, list):
        post = [post]
    return pre + [task] + post


def install_node_instance_subgraph(instance, graph, **kwargs):
    """This function is used to create a tasks sequence installing one node
    instance.
    Considering the order of tasks executions, it enforces the proper
    dependencies only in context of this particular node instance.

    :param instance: node instance to generate the installation tasks for
    """
    subgraph = graph.subgraph('install_{0}'.format(instance.id))
    sequence = subgraph.sequence()
    tasks = []
    create = _skip_nop_operations(
        pre=forkjoin(instance.send_event('Creating node instance'),
                     instance.set_state('creating')),
        task=instance.execute_operation(
            'cloudify.interfaces.lifecycle.create'),
        post=forkjoin(instance.send_event('Node instance created'),
                      instance.set_state('created')))
    preconf = _skip_nop_operations(
        pre=instance.send_event('Pre-configuring relationships'),
        task=_relationships_operations(
            subgraph,
            instance,
            'cloudify.interfaces.relationship_lifecycle.preconfigure'
        ),
        post=instance.send_event('Relationships pre-configured')
    )
    configure = _skip_nop_operations(
        pre=forkjoin(instance.set_state('configuring'),
                     instance.send_event('Configuring node instance')),
        task=instance.execute_operation(
            'cloudify.interfaces.lifecycle.configure'),
        post=forkjoin(instance.set_state('configured'),
                      instance.send_event('Node instance configured'))
    )
    postconf = _skip_nop_operations(
        pre=instance.send_event('Post-configuring relationships'),
        task=_relationships_operations(
            subgraph,
            instance,
            'cloudify.interfaces.relationship_lifecycle.postconfigure'
        ),
        post=instance.send_event('Relationships post-configured'),
    )
    start = _skip_nop_operations(
        pre=forkjoin(instance.set_state('starting'),
                     instance.send_event('Starting node instance')),
        task=instance.execute_operation('cloudify.interfaces.lifecycle.start'),
    )
    # If this is a host node, we need to add specific host start
    # tasks such as waiting for it to start and installing the agent
    # worker (if necessary)
    if is_host_node(instance):
        post_start = _host_post_start(instance)
    else:
        post_start = []
    monitoring_start = _skip_nop_operations(
        instance.execute_operation('cloudify.interfaces.monitoring.start')
    )
    establish = _skip_nop_operations(
        pre=instance.send_event('Establishing relationships'),
        task=_relationships_operations(
            subgraph,
            instance,
            'cloudify.interfaces.relationship_lifecycle.establish'
        ),
        post=instance.send_event('Relationships established'),
    )
    if any([create, preconf, configure, postconf, start, post_start,
            monitoring_start, establish]):
        tasks = (
            [instance.set_state('initializing')] +
            (create or
             [instance.send_event('Creating node instance: nothing to do')]) +
            preconf +
            (configure or
             [instance.send_event(
                 'Configuring node instance: nothing to do')]) +
            postconf +
            (start or
             [instance.send_event('Starting node instance: nothing to do')]) +
            post_start +
            monitoring_start +
            establish +
            [forkjoin(
                instance.set_state('started'),
                instance.send_event('Node instance started')
            )]
        )
    else:
        tasks = [forkjoin(
            instance.set_state('started'),
            instance.send_event('Node instance started (nothing to do)')
        )]

    sequence.add(*tasks)
    subgraph.on_failure = get_subgraph_on_failure_handler(instance)
    return subgraph


def uninstall_node_instance_subgraph(instance, graph, ignore_failure=False):
    subgraph = graph.subgraph(instance.id)
    sequence = subgraph.sequence()
    monitoring_stop = [
        forkjoin(
            instance.set_state('stopping'),
            instance.send_event('Stopping node instance')
        ),
        instance.execute_operation('cloudify.interfaces.monitoring.stop')
    ]
    pre_stop = _host_pre_stop(instance)

    stop = _skip_nop_operations(
        task=instance.execute_operation('cloudify.interfaces.lifecycle.stop'),
        post=forkjoin(
            instance.set_state('stopped'),
            instance.send_event('Stopped node instance'))
    )
    unlink = _skip_nop_operations(
        pre=instance.send_event('Unlinking relationships'),
        task=_relationships_operations(
            subgraph,
            instance,
            'cloudify.interfaces.relationship_lifecycle.unlink',
            reverse=True),
        post=instance.send_event('Relationships unlinked')
    )
    delete = _skip_nop_operations(
        pre=forkjoin(
            instance.set_state('deleting'),
            instance.send_event('Deleting node instance')),
        task=instance.execute_operation(
            'cloudify.interfaces.lifecycle.delete'),
        post=forkjoin(
            instance.set_state('deleted'),
            instance.send_event('Deleted node instance'))
    )

    def set_ignore_handlers(_subgraph):
        for task in _subgraph.tasks.itervalues():
            if task.is_subgraph:
                set_ignore_handlers(task)
            else:
                set_send_node_event_on_error_handler(task, instance)

    tasks = (
        monitoring_stop +
        pre_stop +
        stop +
        unlink +
        delete)
    sequence.add(*tasks)

    if ignore_failure:
        set_ignore_handlers(subgraph)
    else:
        subgraph.on_failure = get_subgraph_on_failure_handler(
            instance, uninstall_node_instance_subgraph)

    return subgraph


def reinstall_node_instance_subgraph(instance, graph):
    reinstall_subgraph = graph.subgraph('reinstall_{0}'.format(instance.id))
    uninstall_subgraph = uninstall_node_instance_subgraph(
        instance, reinstall_subgraph, ignore_failure=False)
    install_subgraph = install_node_instance_subgraph(
        instance, reinstall_subgraph)
    reinstall_sequence = reinstall_subgraph.sequence()
    reinstall_sequence.add(
        instance.send_event('Node lifecycle failed. '
                            'Attempting to re-run node lifecycle'),
        uninstall_subgraph,
        install_subgraph)
    reinstall_subgraph.on_failure = get_subgraph_on_failure_handler(
        instance)
    return reinstall_subgraph


def get_subgraph_on_failure_handler(
        instance, retried_task=reinstall_node_instance_subgraph):
    def subgraph_on_failure_handler(subgraph):
        graph = subgraph.graph
        for task in subgraph.tasks.itervalues():
            subgraph.remove_task(task)
        if not subgraph.containing_subgraph:
            result = workflow_tasks.HandlerResult.retry()
            result.retried_task = retried_task(instance, graph)
            result.retried_task.current_retries = subgraph.current_retries + 1
        else:
            result = workflow_tasks.HandlerResult.ignore()
            subgraph.containing_subgraph.failed_task = subgraph.failed_task
            subgraph.containing_subgraph.set_state(workflow_tasks.TASK_FAILED)
        return result
    return subgraph_on_failure_handler


def _relationships_operations(graph,
                              node_instance,
                              operation,
                              reverse=False,
                              modified_relationship_ids=None):
    def on_failure(subgraph):
        for task in subgraph.tasks.itervalues():
            subgraph.remove_task(task)
        handler_result = workflow_tasks.HandlerResult.ignore()
        subgraph.containing_subgraph.failed_task = subgraph.failed_task
        subgraph.containing_subgraph.set_state(workflow_tasks.TASK_FAILED)
        return handler_result
    relationships_groups = itertools.groupby(
        node_instance.relationships,
        key=lambda r: r.relationship.target_id)
    tasks = []
    for _, relationship_group in relationships_groups:
        group_tasks = []
        for relationship in relationship_group:
            # either the relationship ids aren't specified, or all the
            # relationship should be added
            source_id = relationship.node_instance.node.id
            target_id = relationship.target_node_instance.node.id
            if (not modified_relationship_ids or
                    (source_id in modified_relationship_ids and
                     target_id in modified_relationship_ids[source_id])):
                group_tasks += [
                    op
                    for op in _relationship_operations(relationship, operation)
                    if not op.is_nop()
                ]
        if group_tasks:
            tasks.append(forkjoin(*group_tasks))
    if reverse:
        tasks = reversed(tasks)
    if not tasks:
        return
    result = graph.subgraph('{0}_subgraph'.format(operation))
    result.on_failure = on_failure
    sequence = result.sequence()
    sequence.add(*tasks)
    return result


def _relationship_operations(relationship, operation):
    return [relationship.execute_source_operation(operation),
            relationship.execute_target_operation(operation)]


def is_host_node(node_instance):
    return constants.COMPUTE_NODE_TYPE in node_instance.node.type_hierarchy


def _wait_for_host_to_start(host_node_instance):
    task = host_node_instance.execute_operation(
        'cloudify.interfaces.host.get_state')

    # handler returns True if if get_state returns False,
    # this means, that get_state will be re-executed until
    # get_state returns True
    def node_get_state_handler(tsk):
        host_started = tsk.async_result.get()
        if host_started:
            return workflow_tasks.HandlerResult.cont()
        else:
            return workflow_tasks.HandlerResult.retry(
                ignore_total_retries=True)
    if not task.is_nop():
        task.on_success = node_get_state_handler
    return task


def prepare_running_agent(host_node_instance):
    tasks = []
    plugins_to_install = filter(lambda plugin: plugin['install'],
                                host_node_instance.node.plugins_to_install)

    _plugins_install_task = plugins_install_task(host_node_instance,
                                                 plugins_to_install)
    if _plugins_install_task:
        tasks += [host_node_instance.send_event('Installing plugins')]
        tasks += [_plugins_install_task]
    tasks += [host_node_instance.send_event('Plugins installed')]
    tasks += [
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.install'),
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.start'),
    ]
    return tasks


def plugins_uninstall_task(host_node_instance, plugins_to_uninstall):
    install_method = utils.internal.get_install_method(
        host_node_instance.node.properties)
    if (plugins_to_uninstall and
            install_method != constants.AGENT_INSTALL_METHOD_NONE):
        return host_node_instance.execute_operation(
            'cloudify.interfaces.cloudify_agent.uninstall_plugins',
            kwargs={'plugins': plugins_to_uninstall})
    return None


def plugins_install_task(host_node_instance, plugins_to_install):
    install_method = utils.internal.get_install_method(
        host_node_instance.node.properties)
    if (plugins_to_install and
            install_method != constants.AGENT_INSTALL_METHOD_NONE):
        node_operations = host_node_instance.node.operations

        if 'cloudify.interfaces.plugin_installer.install' in \
                node_operations:
            # 3.2 Compute Node
            return host_node_instance.execute_operation(
                'cloudify.interfaces.plugin_installer.install',
                kwargs={'plugins': plugins_to_install})
        else:
            return host_node_instance.execute_operation(
                'cloudify.interfaces.cloudify_agent.install_plugins',
                kwargs={'plugins': plugins_to_install})
    return None


def _host_post_start(host_node_instance):
    install_method = utils.internal.get_install_method(
        host_node_instance.node.properties)
    tasks = [_wait_for_host_to_start(host_node_instance)]
    if install_method != constants.AGENT_INSTALL_METHOD_NONE:
        node_operations = host_node_instance.node.operations
        if 'cloudify.interfaces.worker_installer.install' in node_operations:
            # 3.2 Compute Node
            tasks += [
                host_node_instance.send_event('Installing agent'),
                host_node_instance.execute_operation(
                    'cloudify.interfaces.worker_installer.install'),
                host_node_instance.send_event('Agent installed'),
                host_node_instance.send_event('Starting agent'),
                host_node_instance.execute_operation(
                    'cloudify.interfaces.worker_installer.start'),
                host_node_instance.send_event('Agent started')
            ]
        else:
            tasks += [
                host_node_instance.send_event('Creating agent'),
                host_node_instance.execute_operation(
                    'cloudify.interfaces.cloudify_agent.create'),
                host_node_instance.send_event('Agent created')
            ]
            # In remote mode the `create` operation configures/starts the agent
            if install_method in [constants.AGENT_INSTALL_METHOD_PLUGIN,
                                  constants.AGENT_INSTALL_METHOD_INIT_SCRIPT]:
                tasks += [
                    host_node_instance.send_event('Waiting for '
                                                  'agent to start'),
                    host_node_instance.execute_operation(
                        'cloudify.interfaces.cloudify_agent.start'),
                    host_node_instance.send_event('Agent started'),
                ]

    tasks.extend(prepare_running_agent(host_node_instance))
    return tasks


def _host_pre_stop(host_node_instance):
    install_method = utils.internal.get_install_method(
        host_node_instance.node.properties)
    tasks = []
    tasks += [
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.stop'),
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.uninstall'),
    ]
    if install_method != constants.AGENT_INSTALL_METHOD_NONE:
        tasks.append(host_node_instance.send_event('Stopping agent'))
        if install_method in constants.AGENT_INSTALL_METHODS_SCRIPTS:
            # this option is only available since 3.3 so no need to
            # handle 3.2 version here.
            tasks += [
                host_node_instance.execute_operation(
                    'cloudify.interfaces.cloudify_agent.stop_amqp'),
                host_node_instance.send_event('Deleting agent'),
                host_node_instance.execute_operation(
                    'cloudify.interfaces.cloudify_agent.delete')
            ]
        else:
            node_operations = host_node_instance.node.operations
            if 'cloudify.interfaces.worker_installer.stop' in node_operations:
                tasks += [
                    host_node_instance.execute_operation(
                        'cloudify.interfaces.worker_installer.stop'),
                    host_node_instance.send_event('Deleting agent'),
                    host_node_instance.execute_operation(
                        'cloudify.interfaces.worker_installer.uninstall')
                ]
            else:
                tasks += [
                    host_node_instance.execute_operation(
                        'cloudify.interfaces.cloudify_agent.stop'),
                    host_node_instance.send_event('Deleting agent'),
                    host_node_instance.execute_operation(
                        'cloudify.interfaces.cloudify_agent.delete')
                ]
        tasks += [host_node_instance.send_event('Agent deleted')]
    return tasks
