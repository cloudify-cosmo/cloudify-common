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

from cloudify import exceptions, utils
from cloudify import constants
from cloudify.state import workflow_ctx
from cloudify.workflows.tasks_graph import forkjoin, make_or_get_graph
from cloudify.workflows import tasks as workflow_tasks


def install_node_instances(graph,
                           node_instances,
                           related_nodes=None,
                           name_prefix=''):
    processor = LifecycleProcessor(graph=graph,
                                   node_instances=node_instances,
                                   related_nodes=related_nodes,
                                   name_prefix=name_prefix)
    processor.install()


def uninstall_node_instances(graph,
                             node_instances,
                             ignore_failure,
                             related_nodes=None,
                             name_prefix=''):
    processor = LifecycleProcessor(graph=graph,
                                   node_instances=node_instances,
                                   ignore_failure=ignore_failure,
                                   related_nodes=related_nodes,
                                   name_prefix=name_prefix)
    processor.uninstall()


def heal_node_instances(
    graph,
    node_instances,
    related_nodes=None,
    name_prefix='',
    ignore_failure=False,
):
    processor = LifecycleProcessor(graph=graph,
                                   node_instances=node_instances,
                                   related_nodes=related_nodes,
                                   name_prefix=name_prefix,
                                   ignore_failure=ignore_failure,)
    processor.heal()


def update_node_instances(
    graph,
    node_instances,
    related_nodes=None,
    name_prefix=''
):
    processor = LifecycleProcessor(graph=graph,
                                   node_instances=node_instances,
                                   related_nodes=related_nodes,
                                   name_prefix=name_prefix)
    processor.update()


def reinstall_node_instances(graph,
                             node_instances,
                             ignore_failure,
                             related_nodes=None):
    uninstall_node_instances(graph, node_instances, ignore_failure,
                             related_nodes, name_prefix='reinstall-')
    # refresh the local node instance references, because they most likely
    # changed their state & runtime props during the uninstall
    workflow_ctx.refresh_node_instances()
    node_instances = set(workflow_ctx.get_node_instance(inst.id)
                         for inst in node_instances)
    related_nodes = set(workflow_ctx.get_node_instance(inst.id)
                        for inst in related_nodes)
    install_node_instances(graph, node_instances, related_nodes,
                           name_prefix='reinstall-')


def execute_establish_relationships(graph,
                                    node_instances,
                                    related_nodes=None,
                                    modified_relationship_ids=None):
    processor = LifecycleProcessor(
        graph=graph,
        related_nodes=node_instances,
        modified_relationship_ids=modified_relationship_ids,
        name_prefix='establish-')
    processor.install()


def execute_unlink_relationships(graph,
                                 node_instances,
                                 related_nodes=None,
                                 modified_relationship_ids=None):
    processor = LifecycleProcessor(
        graph=graph,
        related_nodes=node_instances,
        modified_relationship_ids=modified_relationship_ids,
        name_prefix='unlink-')
    processor.uninstall()


def rollback_node_instances(
    graph,
    node_instances,
    related_nodes=None,
    name_prefix='',
    ignore_failure=True,
):
    processor = LifecycleProcessor(graph=graph,
                                   node_instances=node_instances,
                                   related_nodes=related_nodes,
                                   name_prefix=name_prefix,
                                   ignore_failure=ignore_failure)

    processor.rollback()


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
        if workflow_ctx.resume:
            self._update_resumed_install(graph)
        graph.execute()

    def uninstall(self):
        graph = self._process_node_instances(
            workflow_ctx,
            name=self._name_prefix + 'uninstall',
            node_instance_subgraph_func=uninstall_node_instance_subgraph,
            graph_finisher_func=self._finish_uninstall)
        graph.execute()

    def rollback(self):
        graph = self._process_node_instances(
            workflow_ctx,
            name=self._name_prefix + 'rollback',
            node_instance_subgraph_func=rollback_node_instance_subgraph,
            graph_finisher_func=self._finish_uninstall)
        graph.execute()

    def heal(self):
        graph = self._process_node_instances(
            workflow_ctx,
            name=self._name_prefix + 'heal',
            node_instance_subgraph_func=heal_node_instance_subgraph,
            graph_finisher_func=self._finish_heal)
        graph.execute()

    def update(self):
        graph = self._process_node_instances(
            workflow_ctx,
            name=self._name_prefix + 'update',
            node_instance_subgraph_func=update_node_instance_subgraph,
            graph_finisher_func=self._finish_update)
        graph.execute()

    def _update_resumed_install(self, graph):
        """Update a resumed install graph to cleanup first.

        When an install is resumed:
         - if we are going to re-send the create operation, send delete first
         - if we are going to re-send the start operation, send stop first
        """
        install_subgraphs = _find_install_subgraphs(graph)
        for instance in self.node_instances:
            install_subgraph = install_subgraphs.get(instance.id)
            if not install_subgraph:
                continue

            tasks = []
            if instance.state in ['starting'] and _would_resend(
                    install_subgraph, 'cloudify.interfaces.lifecycle.start'):
                tasks += _pre_resume_stop(instance)
            if instance.state in ['creating'] and _would_resend(
                    install_subgraph, 'cloudify.interfaces.lifecycle.create'):
                tasks += _pre_resume_uninstall(instance)
            if not tasks:
                continue
            tasks = [
                instance.send_event('Rolling back prior to resuming install')
            ] + tasks + [
                instance.send_event(
                    'Finished rolling back, now resuming install')
            ]

            uninstall_subgraph = graph.subgraph(
                'resume_cleanup_{0}'.format(instance.id))
            sequence = uninstall_subgraph.sequence()
            sequence.add(*tasks)
            ignore_subgraph_on_task_failure(uninstall_subgraph)
            _run_subgraph_before(uninstall_subgraph, install_subgraph)

    @make_or_get_graph
    def _process_node_instances(self,
                                ctx,
                                node_instance_subgraph_func,
                                graph_finisher_func):
        subgraphs = {}
        for instance in self.node_instances:
            subgraph = node_instance_subgraph_func(
                instance,
                self.graph,
                ignore_failure=self.ignore_failure,
            )
            if subgraph is None:
                subgraph = self.graph.subgraph('stub_{0}'.format(instance.id))
            subgraphs[instance.id] = subgraph

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

    def _finish_heal(self, graph, subgraphs):
        self._add_dependencies(
            graph=graph,
            subgraphs=subgraphs,
            instances=self.node_instances,
            install=True,
        )

    def _finish_update(self, graph, subgraphs):
        self._add_dependencies(
            graph=graph,
            subgraphs=subgraphs,
            instances=self.node_instances,
            install=True,
        )

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

    @staticmethod
    def _handle_dependency_creation(source_subgraph, target_subgraph,
                                    operation, target_id, graph):
        if operation:
            for task_subgraph in target_subgraph.graph.tasks:
                # If the task is not an operation task
                if not task_subgraph.cloudify_context:
                    continue

                operation_path, operation_name = \
                    (task_subgraph.cloudify_context["operation"]["name"]
                     .rsplit(".", 1))
                node_id = task_subgraph.cloudify_context["node_id"]
                if (operation_path == 'cloudify.interfaces.lifecycle' and
                        operation_name == operation and
                        target_id == node_id):

                    # Adding dependency to all post tasks that are dependent
                    # of the chosen operation, with the assumption that they
                    # are all only dependent on the operation not each other.
                    for task_id in target_subgraph.graph._dependents.get(
                            task_subgraph.id, []):
                        graph.add_dependency(source_subgraph,
                                             target_subgraph.graph.get_task(
                                                 task_id))
                    break
        else:
            graph.add_dependency(source_subgraph, target_subgraph)

    def _add_dependencies(self, graph, subgraphs, instances, install,
                          on_dependency_added=None):
        subgraph_sequences = dict(
            (instance_id, subgraph.sequence())
            for instance_id, subgraph in subgraphs.items()
            if subgraph is not None
        )
        for instance in instances:
            relationships = list(instance.relationships)
            if not install:
                relationships = reversed(relationships)
            for rel in relationships:
                if (rel.target_node_instance in self.node_instances or
                        rel.target_node_instance in self.intact_nodes):
                    source_subgraph = subgraphs.get(instance.id)
                    target_subgraph = subgraphs.get(rel.target_id)
                    if source_subgraph is None or target_subgraph is None:
                        continue
                    operation = rel.relationship.properties.get("operation",
                                                                None)

                    if install:
                        self._handle_dependency_creation(source_subgraph,
                                                         target_subgraph,
                                                         operation,
                                                         rel.target_id,
                                                         graph)
                    else:
                        self._handle_dependency_creation(target_subgraph,
                                                         source_subgraph,
                                                         operation,
                                                         instance.id,
                                                         graph)

                    if on_dependency_added:
                        task_sequence = subgraph_sequences[instance.id]
                        on_dependency_added(instance, rel, task_sequence)


def _find_install_subgraphs(graph):
    """In the install graph, find subgraphs that install a node instance.

    Make a dict of {instance id: subgraph}, based on the subgraph name.
    """
    install_subgraphs = {}
    for task in graph.tasks:
        if task.is_subgraph and task.name.startswith('install_'):
            instance_name = task.name[len('install_'):]
            install_subgraphs[instance_name] = task
    return install_subgraphs


def _would_resend(subgraph, operation):
    """Would the subgraph send the operation again?

    Find the task named by operation, and check its state.
    """
    found_task = None
    for task in subgraph.tasks.values():
        if task.task_type != 'RemoteWorkflowTask':
            continue
        try:
            if task.cloudify_context['operation']['name'] == operation:
                found_task = task
                break
        except KeyError:
            pass
    else:
        return False
    return found_task.get_state() == workflow_tasks.TASK_PENDING


def _pre_resume_uninstall(instance):
    """Run these uninstall tasks before resuming/resending a create."""
    delete = _skip_nop_operations(
        pre=instance.send_event('Deleting node instance'),
        task=instance.execute_operation(
            'cloudify.interfaces.lifecycle.delete'),
        post=instance.send_event('Deleted node instance')
    )
    if instance.node.has_operation('cloudify.interfaces.lifecycle.postdelete'):
        postdelete = _skip_nop_operations(
            pre=instance.send_event('Postdeleting node instance'),
            task=instance.execute_operation(
                'cloudify.interfaces.lifecycle.postdelete'),
            post=instance.send_event('Node instance postdeleted'))
    else:
        postdelete = []

    return delete + postdelete


def _pre_resume_stop(instance):
    """Run these stop tasks before resuming/resending a start."""
    if instance.node.has_operation('cloudify.interfaces.lifecycle.prestop'):
        prestop = _skip_nop_operations(
            pre=instance.send_event('Prestopping node instance'),
            task=instance.execute_operation(
                'cloudify.interfaces.lifecycle.prestop'),
            post=instance.send_event('Node instance prestopped'))
    else:
        prestop = []

    if is_host_node(instance):
        host_pre_stop = _host_pre_stop(instance)
    else:
        host_pre_stop = []

    stop = _skip_nop_operations(
        task=instance.execute_operation(
            'cloudify.interfaces.lifecycle.stop'),
        post=instance.send_event('Stopped node instance'))
    return prestop + host_pre_stop + stop


def _run_subgraph_before(subgraph_before, subgraph_after):
    """Hook up dependencies so that subgraph_before runs before subgraph_after.

    "before" will depend on everything that "after" depends, and "after" will
    also depend on the "before".
    """
    if subgraph_before.graph is not subgraph_after.graph:
        raise RuntimeError('{0} and {1} belong to different graphs'
                           .format(subgraph_before, subgraph_after))
    graph = subgraph_before.graph
    for dependency_id in graph._dependencies.get(subgraph_after.id, []):
        graph.add_dependency(subgraph_before, graph.get_task(dependency_id))
    graph.add_dependency(subgraph_after, subgraph_before)


def ignore_subgraph_on_task_failure(subgraph):
    """If the subgraph fails, just ignore it.

    This is to be used in the pre-resume cleanup graphs, so that
    the cleanup failing doesn't block the install from being resumed.
    """
    for task in subgraph.tasks.values():
        if task.is_subgraph:
            ignore_subgraph_on_task_failure(task)
            task.on_failure = _ignore_subgraph_failure
        else:
            task.on_failure = _ignore_task_failure


def set_send_node_event_on_error_handler(task, instance):
    task.on_failure = _SendNodeEventHandler(instance)


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
    node = instance.node
    instance_state = instance.state
    if instance_state in [
        'started', 'starting', 'created', 'creating',
        'configured', 'configuring'
    ]:
        creation_validation = []
        precreate = []
    else:
        # Only exists in >= 5.0.
        if node.has_operation('cloudify.interfaces.validation.create'):
            creation_validation = _skip_nop_operations(
                pre=instance.send_event(
                    'Validating node instance before creation'),
                task=instance.execute_operation(
                    'cloudify.interfaces.validation.create'
                ),
                post=instance.send_event(
                    'Node instance validated before creation')
            )
        else:
            creation_validation = []
        # Only exists in >= 5.0.
        if node.has_operation('cloudify.interfaces.lifecycle.precreate'):
            precreate = _skip_nop_operations(
                pre=instance.send_event('Precreating node instance'),
                task=instance.execute_operation(
                    'cloudify.interfaces.lifecycle.precreate'),
                post=instance.send_event('Node instance precreated'))
        else:
            precreate = []
    if instance_state in ['started', 'starting', 'created',
                          'configuring', 'configured']:
        create = []
    else:
        create = _skip_nop_operations(
            pre=forkjoin(instance.send_event('Creating node instance'),
                         instance.set_state('creating')),
            task=instance.execute_operation(
                'cloudify.interfaces.lifecycle.create'),
            post=forkjoin(instance.send_event('Node instance created'),
                          instance.set_state('created')))
    if instance_state in ['started', 'starting', 'configured']:
        preconf = []
        configure = []
    else:
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
    if instance_state == 'started':
        postconf = []
        host_post_start = []
        poststart = []
        monitoring_start = []
        establish = []
        start = []
    else:
        postconf = _skip_nop_operations(
            pre=instance.send_event('Post-configuring relationships'),
            task=_relationships_operations(
                subgraph,
                instance,
                'cloudify.interfaces.relationship_lifecycle.postconfigure'
            ),
            post=instance.send_event('Relationships post-configured')
        )

        start = _skip_nop_operations(
            pre=forkjoin(instance.set_state('starting'),
                         instance.send_event('Starting node instance')),
            task=instance.execute_operation(
                'cloudify.interfaces.lifecycle.start'
            )
        )
        # If this is a host node, we need to add specific host start
        # tasks such as waiting for it to start and installing the agent
        # worker (if necessary)
        if is_host_node(instance):
            host_post_start = _host_post_start(instance)
        else:
            host_post_start = []

        # Only exists in >= 5.0.
        if node.has_operation('cloudify.interfaces.lifecycle.poststart'):
            poststart = _skip_nop_operations(
                pre=instance.send_event('Poststarting node instance'),
                task=instance.execute_operation(
                    'cloudify.interfaces.lifecycle.poststart'),
                post=instance.send_event('Node instance poststarted'))
        else:
            poststart = []

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
            post=instance.send_event('Relationships established')
        )
    if any([creation_validation, precreate, create, preconf, configure,
            postconf, start, host_post_start, poststart, monitoring_start,
            establish]):
        tasks = (
            [instance.set_state('initializing')] +
            (creation_validation or
             [instance.send_event('Validating node instance before creation: '
                                  'nothing to do')]) +
            (precreate or
             [instance.send_event(
                 'Precreating node instance: nothing to do')]) +
            (create or
             [instance.send_event('Creating node instance: nothing to do')]) +
            preconf +
            (configure or
             [instance.send_event(
                 'Configuring node instance: nothing to do')]) +
            postconf +
            (start or
             [instance.send_event('Starting node instance: nothing to do')]) +
            host_post_start +
            (poststart or
             [instance.send_event(
                 'Poststarting node instance: nothing to do')]) +
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
    subgraph.on_failure = _SubgraphOnFailure(instance)
    return subgraph


def uninstall_node_instance_subgraph(instance, graph, ignore_failure=False):
    subgraph = graph.subgraph(instance.id)
    sequence = subgraph.sequence()

    def set_ignore_handlers(_subgraph):
        for task in _subgraph.tasks.values():
            if task.is_subgraph:
                set_ignore_handlers(task)
            else:
                set_send_node_event_on_error_handler(task, instance)

    # Remove unneeded operations
    node = instance.node
    instance_state = instance.state
    if instance_state in ['stopped', 'deleting', 'deleted', 'uninitialized',
                          'configured']:
        stop_message = []
        monitoring_stop = []
        host_pre_stop = []
        prestop = []
        deletion_validation = []
        stop = [instance.send_event(
            'Stop: instance already {0}'.format(instance_state))]
    else:
        stop_message = [
            forkjoin(
                instance.set_state('stopping'),
                instance.send_event('Stopping node instance')
            )
        ]
        monitoring_stop = _skip_nop_operations(
            instance.execute_operation('cloudify.interfaces.monitoring.stop')
        )

        # Only exists in >= 5.0.
        if node.has_operation('cloudify.interfaces.validation.delete'):
            deletion_validation = _skip_nop_operations(
                pre=instance.send_event(
                    'Validating node instance before deletion'),
                task=instance.execute_operation(
                    'cloudify.interfaces.validation.delete'
                ),
                post=instance.send_event(
                    'Node instance validated before deletion')
            )
        else:
            deletion_validation = []

        # Only exists in >= 5.0.
        if node.has_operation('cloudify.interfaces.lifecycle.prestop'):
            prestop = _skip_nop_operations(
                pre=instance.send_event('Prestopping node instance'),
                task=instance.execute_operation(
                    'cloudify.interfaces.lifecycle.prestop'),
                post=instance.send_event('Node instance prestopped'))
        else:
            prestop = []

        if is_host_node(instance):
            host_pre_stop = _host_pre_stop(instance)
        else:
            host_pre_stop = []

        stop = _skip_nop_operations(
            task=instance.execute_operation(
                'cloudify.interfaces.lifecycle.stop'),
            post=instance.send_event('Stopped node instance'))
    if instance_state in ['stopped', 'deleting', 'deleted']:
        stopped_set_state = []
    else:
        stopped_set_state = [instance.set_state('stopped')]
    if instance_state in ['deleted', 'uninitialized']:
        unlink = []
        postdelete = []
        delete = [instance.send_event(
            'Delete: instance already {0}'.format(instance_state))]
    else:
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
                'cloudify.interfaces.lifecycle.delete')
        )
        # Only exists in >= 5.0.
        if node.has_operation('cloudify.interfaces.lifecycle.postdelete'):
            postdelete = _skip_nop_operations(
                pre=instance.send_event('Postdeleting node instance'),
                task=instance.execute_operation(
                    'cloudify.interfaces.lifecycle.postdelete'),
                post=instance.send_event('Node instance postdeleted'))
        else:
            postdelete = []

    if instance_state in ['deleted']:
        finish_message = []
    else:
        finish_message = [forkjoin(
            instance.set_state('deleted'),
            instance.send_event('Deleted node instance')
        )]

    tasks = (
        stop_message +
        (deletion_validation or
         [instance.send_event('Validating node instance after deletion: '
                              'nothing to do')]) +
        monitoring_stop +
        prestop +
        host_pre_stop +
        (stop or
         [instance.send_event('Stopped node instance: nothing to do')]) +
        stopped_set_state +
        unlink +
        (delete or
         [instance.send_event('Deleting node instance: nothing to do')]) +
        postdelete +
        finish_message
    )
    sequence.add(*tasks)

    if ignore_failure:
        set_ignore_handlers(subgraph)
    else:
        subgraph.on_failure = _SubgraphOnFailure(instance, 'uninstall')

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
    reinstall_subgraph.on_failure = _SubgraphOnFailure(instance)
    return reinstall_subgraph


def _relationships_operations(graph,
                              node_instance,
                              operation,
                              reverse=False,
                              modified_relationship_ids=None):
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
    if not tasks:
        return
    if reverse:
        tasks = reversed(tasks)
    result = graph.subgraph('{0}_subgraph'.format(operation))
    result.on_failure = _relationship_subgraph_on_failure
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
    if task.is_nop() or workflow_ctx.dry_run:
        return task
    task.on_success = _node_get_state_handler
    return task


def prepare_running_agent(host_node_instance):
    return [
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.install'),
        host_node_instance.execute_operation(
            'cloudify.interfaces.monitoring_agent.start'),
    ]


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
    node = host_node_instance.node
    install_method = utils.internal.get_install_method(
        host_node_instance.node.properties)
    if (plugins_to_install and
            install_method != constants.AGENT_INSTALL_METHOD_NONE):

        if node.has_operation('cloudify.interfaces.plugin_installer.install'):
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
    node = host_node_instance.node
    tasks = [_wait_for_host_to_start(host_node_instance)]
    if install_method != constants.AGENT_INSTALL_METHOD_NONE:
        if node.has_operation('cloudify.interfaces.worker_installer.install'):
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
    node = host_node_instance.node
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
            if node.has_operation('cloudify.interfaces.worker_installer.stop'):
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


def rollback_node_instance_subgraph(instance, graph, ignore_failure):
    subgraph = graph.subgraph(instance.id)
    sequence = subgraph.sequence()
    node = instance.node

    def set_ignore_handlers(_subgraph):
        for task in _subgraph.tasks.values():
            if task.is_subgraph:
                set_ignore_handlers(task)
            else:
                set_send_node_event_on_error_handler(task, instance)

    # Remove unneeded operations
    instance_state = instance.state
    # decide if do prestop stop and validation delete

    if instance_state not in ['starting']:
        stop_message = []
        monitoring_stop = []
        host_pre_stop = []
        prestop = []
        deletion_validation = []
        stop = [instance.send_event(
            'Rollback Stop: nothing to do, instance state is {0}'.format(
                instance_state))]
        configured_set_state = []
    else:
        stop_message = [
            forkjoin(
                instance.set_state('stopping'),
                instance.send_event('Stopping node instance')
            )
        ]
        monitoring_stop = _skip_nop_operations(
            instance.execute_operation('cloudify.interfaces.monitoring.stop')
        )

        # Only exists in >= 5.0.
        if node.has_operation('cloudify.interfaces.validation.delete'):
            deletion_validation = _skip_nop_operations(
                pre=instance.send_event(
                    'Validating node instance before deletion'),
                task=instance.execute_operation(
                    'cloudify.interfaces.validation.delete'
                ),
                post=instance.send_event(
                    'Node instance validated before deletion')
            )
        else:
            deletion_validation = []

        # Only exists in >= 5.0.
        if node.has_operation('cloudify.interfaces.lifecycle.prestop'):
            prestop = _skip_nop_operations(
                pre=instance.send_event('Prestopping node instance'),
                task=instance.execute_operation(
                    'cloudify.interfaces.lifecycle.prestop'),
                post=instance.send_event('Node instance prestopped'))
        else:
            prestop = []

        if is_host_node(instance):
            host_pre_stop = _host_pre_stop(instance)
        else:
            host_pre_stop = []

        stop = _skip_nop_operations(
            task=instance.execute_operation(
                'cloudify.interfaces.lifecycle.stop'),
            post=instance.send_event('Stopped node instance'))

        configured_set_state = [instance.set_state('configured')]

    # Decide when we want to unlink +delete + post delete
    if instance_state not in ['creating', 'configuring']:
        unlink = []
        postdelete = []
        delete = [instance.send_event(
            'Rollback Delete: nothing to do, instance state is {0}'.format(
                instance_state))]
        uninitialized_set_state = []
    else:
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
                'cloudify.interfaces.lifecycle.delete')
        )
        # Only exists in >= 5.0.
        if node.has_operation('cloudify.interfaces.lifecycle.postdelete'):
            postdelete = _skip_nop_operations(
                pre=instance.send_event('Postdeleting node instance'),
                task=instance.execute_operation(
                    'cloudify.interfaces.lifecycle.postdelete'),
                post=instance.send_event('Node instance postdeleted'))
        else:
            postdelete = []

        uninitialized_set_state = [instance.set_state('uninitialized')]

    if instance_state not in ['creating', 'configuring', 'starting']:
        finish_message = []
    else:
        finish_message = [
            instance.send_event('Rollbacked node instance')
        ]

    tasks = (
            stop_message +
            (deletion_validation or
             [instance.send_event('Validating node instance after deletion: '
                                  'nothing to do')]) +
            monitoring_stop +
            prestop +
            host_pre_stop +
            (stop or
             [instance.send_event('Stopped node instance: nothing to do')]) +
            configured_set_state +
            unlink +
            (delete or
             [instance.send_event('Deleting node instance: nothing to do')]) +
            postdelete +
            uninitialized_set_state +
            finish_message
    )
    sequence.add(*tasks)

    if ignore_failure:
        set_ignore_handlers(subgraph)

    return subgraph


class _SubgraphOnFailure(object):
    def __init__(self, instance=None, on_retry='reinstall', instance_id=None):
        if instance is None:
            instance = workflow_ctx.get_node_instance(instance_id)
        self.instance = instance
        if on_retry not in ('reinstall', 'uninstall'):
            raise exceptions.NonRecoverableError(
                'on_retry must be reinstall or uninstall')
        self.on_retry = on_retry

    def dump(self):
        return {
            'instance_id': self.instance.id,
            'on_retry': self.on_retry
        }

    def __call__(self, subgraph):
        graph = subgraph.graph
        for task in subgraph.tasks.values():
            subgraph.remove_task(task)
        if not subgraph.containing_subgraph:
            result = workflow_tasks.HandlerResult.retry()
            if self.on_retry == 'reinstall':
                result.retried_task = reinstall_node_instance_subgraph(
                    self.instance, graph)
            elif self.on_retry == 'uninstall':
                result.retried_task = uninstall_node_instance_subgraph(
                    self.instance, graph)
            else:
                raise exceptions.NonRecoverableError(
                    'subgraph {0} on_failure: unknown retry method {1}'
                    .format(subgraph, self.on_retry))
            result.retried_task.current_retries = subgraph.current_retries + 1
        else:
            result = workflow_tasks.HandlerResult.ignore()
            subgraph.containing_subgraph.failed_task = subgraph.failed_task
            subgraph.containing_subgraph.set_state(workflow_tasks.TASK_FAILED)
        return result


def _ignore_subgraph_failure(tsk):
    workflow_ctx.logger.info('Ignoring subgraph failure in cleanup')
    for t in tsk.tasks.values():
        if t.get_state() == workflow_tasks.TASK_PENDING:
            tsk.remove_task(t)
    return workflow_tasks.HandlerResult.ignore()


def _ignore_task_failure(tsk):
    workflow_ctx.logger.info('Ignoring task failure in cleanup')
    return workflow_tasks.HandlerResult.ignore()


class _SendNodeEventHandler(object):
    def __init__(self, instance=None, instance_id=None):
        if instance is None:
            instance = workflow_ctx.get_node_instance(instance_id)
        self.instance = instance

    def dump(self):
        return {
            'instance_id': self.instance.id,
        }

    def __call__(self, tsk):
        event = self.instance.send_event(
            'Ignoring task {0} failure'.format(tsk.name))
        event.apply_async()
        return workflow_tasks.HandlerResult.ignore()


def _relationship_subgraph_on_failure(subgraph):
    for task in subgraph.tasks.values():
        subgraph.remove_task(task)
    handler_result = workflow_tasks.HandlerResult.ignore()
    subgraph.containing_subgraph.failed_task = subgraph.failed_task
    subgraph.containing_subgraph.set_state(workflow_tasks.TASK_FAILED)
    return handler_result


def _node_get_state_handler(tsk):
    host_started = tsk.async_result.get()
    if host_started:
        return workflow_tasks.HandlerResult.cont()
    else:
        return workflow_tasks.HandlerResult.retry(ignore_total_retries=True)


def _on_heal_success(task):
    """Heal success callback - set the status system property to OK.

    After a heal has succeeded, we'll consider the node passing the status
    check immediately.
    """
    instance_id = task.info['instance_id']
    workflow_context = task.workflow_context
    ni = workflow_context.get_node_instance(instance_id)
    system_properties = ni.system_properties or {}
    system_properties.setdefault('status', {}).update(
        ok=True,
        task=None,
        healed=True,
    )
    workflow_context.update_node_instance(
        instance_id,
        force=True,
        system_properties=system_properties
    )
    return workflow_tasks.HandlerResult.cont()


def _on_heal_failure(task):
    """Heal failure callback - mark the node as having failed a heal

    We mark the node that a heal was attempted and failed, so that we know
    it needs to be fully reinstalled.
    """
    instance_id = task.info['instance_id']
    workflow_context = task.workflow_context
    ni = workflow_context.get_node_instance(instance_id)
    system_properties = ni.system_properties or {}
    system_properties['heal_failed'] = workflow_context.execution_id
    workflow_context.update_node_instance(
        instance_id,
        force=True,
        system_properties=system_properties
    )
    return workflow_tasks.HandlerResult.ignore()


def heal_node_instance_subgraph(instance, graph, **kwargs):
    """Make a subgraph of healing a single node instance.

    Just run all the heal-related operations. Let's also have success
    and failures callbacks on them, so that the workflow knows if this
    instance was healed successfully or not.
    """
    subgraph = graph.subgraph('heal_{0}'.format(instance.id))
    sequence = subgraph.sequence()
    sequence.add(
        instance.send_event('Healing node instance'),
        instance.execute_operation('cloudify.interfaces.lifecycle.preheal'),
        instance.execute_operation('cloudify.interfaces.lifecycle.heal'),
        instance.execute_operation('cloudify.interfaces.lifecycle.postheal'),
        instance.send_event('Node instance healed'),
    )
    subgraph.info['instance_id'] = instance.id
    subgraph.on_success = _on_heal_success
    subgraph.on_failure = _on_heal_failure
    return subgraph


def _on_update_success(task):
    instance_id = task.info['instance_id']
    workflow_context = task.workflow_context
    ni = workflow_context.get_node_instance(instance_id)
    system_properties = ni.system_properties or {}
    system_properties['configuration_drift'] = None
    workflow_context.update_node_instance(
        instance_id,
        force=True,
        system_properties=system_properties
    )
    return workflow_tasks.HandlerResult.cont()


def _on_update_failure(task):
    instance_id = task.info['instance_id']
    workflow_context = task.workflow_context
    ni = workflow_context.get_node_instance(instance_id)
    system_properties = ni.system_properties or {}
    system_properties['update_failed'] = workflow_context.execution_id
    workflow_context.update_node_instance(
        instance_id,
        force=True,
        system_properties=system_properties,
    )
    return workflow_tasks.HandlerResult.ignore()


def update_node_instance_subgraph(instance, graph, **kwargs):
    """Make a subgraph of updating a single node instance.

    Runs all the update-related operations.
    The success callback clears configuration_drift: it is assumed that
    an update operation does removes all drift.
    The failure callback just notes that the update did fail, so that the
    workflow can reinstall the node-instance.
    """
    operations = []
    system_props = instance.system_properties

    drift = system_props.get('configuration_drift') or {}
    has_own_drift = bool(drift.get('result'))

    # only run the update operations if we have configuration_drift on the
    # instance itself. Even if we don't, there might still be relationship
    # drift, which will later run relationship update operations
    if has_own_drift:
        operations += [
            instance.execute_operation(interface)
            for interface in [
                'cloudify.interfaces.lifecycle.preupdate',
                'cloudify.interfaces.lifecycle.update',
                'cloudify.interfaces.lifecycle.postupdate',
                'cloudify.interfaces.lifecycle.update_config',
                'cloudify.interfaces.lifecycle.update_apply',
                'cloudify.interfaces.lifecycle.update_postapply',
            ]
        ]

    source_drifts = system_props.get(
        'source_relationships_configuration_drift', {})
    for relationship in instance.relationships:
        target_instance = workflow_ctx.get_node_instance(
            relationship.target_id)
        target_drifts = target_instance.system_properties.get(
            'target_relationships_configuration_drift', {})
        if relationship.target_id in source_drifts:
            operations.append(relationship.execute_source_operation(
                'cloudify.interfaces.relationship_lifecycle.update',
            ))
        if relationship.source_id in target_drifts:
            operations.append(relationship.execute_target_operation(
                'cloudify.interfaces.relationship_lifecycle.update',
            ))

    operations = [op for op in operations if not op.is_nop()]
    if operations:
        subgraph = graph.subgraph('update_{0}'.format(instance.id))
        sequence = subgraph.sequence()
        sequence.add(
            instance.send_event('Updating node instance'),
        )
        sequence.add(*operations)
        sequence.add(
            instance.send_event('Node instance updated'),
        )
        subgraph.info['instance_id'] = instance.id
        subgraph.on_success = _on_update_success
        subgraph.on_failure = _on_update_failure
        return subgraph
    return None
