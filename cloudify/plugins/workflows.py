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

import numbers
from itertools import chain

from cloudify import constants, utils
from cloudify.decorators import workflow
from cloudify.plugins import lifecycle
from cloudify.manager import get_rest_client
from cloudify.workflows.tasks_graph import make_or_get_graph
from cloudify.utils import add_plugins_to_install, add_plugins_to_uninstall


@workflow(resumable=True)
def install(ctx, **kwargs):
    """Default install workflow"""
    lifecycle.install_node_instances(
        graph=ctx.graph_mode(),
        node_instances=set(ctx.node_instances))


@workflow(resumable=True)
def uninstall(ctx, ignore_failure=False, **kwargs):
    """Default uninstall workflow"""

    lifecycle.uninstall_node_instances(
        graph=ctx.graph_mode(),
        node_instances=set(ctx.node_instances),
        ignore_failure=ignore_failure)


@workflow
def auto_heal_reinstall_node_subgraph(
        ctx,
        node_instance_id,
        diagnose_value='Not provided',
        ignore_failure=True,
        **kwargs):
    """Reinstalls the whole subgraph of the system topology

    The subgraph consists of all the nodes that are hosted in the
    failing node's compute and the compute itself.
    Additionally it unlinks and establishes appropriate relationships

    :param ctx: cloudify context
    :param node_instance_id: node_instances to reinstall
    :param diagnose_value: diagnosed reason of failure
    :param ignore_failure: ignore operations failures in uninstall workflow
    """

    ctx.logger.info("Starting 'heal' workflow on {0}, Diagnosis: {1}".format(
        node_instance_id, diagnose_value))
    failing_node = ctx.get_node_instance(node_instance_id)
    if failing_node is None:
        raise ValueError('No node instance with id `{0}` was found'.format(
            node_instance_id))
    failing_node_host = ctx.get_node_instance(
        failing_node._node_instance.host_id)
    if failing_node_host is None:
        subgraph_node_instances = failing_node.get_contained_subgraph()
    else:
        subgraph_node_instances = failing_node_host.get_contained_subgraph()
    intact_nodes = set(ctx.node_instances) - subgraph_node_instances
    graph = ctx.graph_mode()
    lifecycle.reinstall_node_instances(graph=graph,
                                       node_instances=subgraph_node_instances,
                                       related_nodes=intact_nodes,
                                       ignore_failure=ignore_failure)


def get_groups_with_members(ctx):
    """
        Get group instance membership.

        :param ctx: cloudify context
        :return: A dict keyed on scaling group instances with lists of node
                 instances belonging to that group instance.

                 e.g.
                 {
                     "vmgroup_x8u01s": ["fakevm_lfog3x", "fakevm2_gmlwzu"],
                     "vmgroup_m2d2cf": ["fakevm_j067z2", "fakevm2_7zcbg2"],
                 }
    """
    groups_members = {}
    for instance in ctx.node_instances:
        scaling_group_ids = [sg.get('id') for sg in instance.scaling_groups]
        for sg_id in scaling_group_ids:
            if sg_id not in groups_members:
                groups_members[sg_id] = []
            groups_members[sg_id].append(instance.id)
    return groups_members


def _check_for_too_many_exclusions(exclude_instances, available_instances,
                                   delta, groups_members):
    """
        Check whether the amount of exluded instances will make it possible to
        scale down by the given delta.

        :param exclude_instances: A list of node instance IDs to exclude.
        :param available_instances: A list of all available instance IDs. For
                                    groups this should include both the group
                                    instance IDs and the member node instance
                                    IDs.
        :param delta: How much to scale by. This is expected to be negative.
        :param groups_members: A dict of group instances with their members,
                               e.g. produced by get_groups_with_members
        :return: A string detailing problems if there are any, or an empty
                 string if there are no problems.
    """
    if groups_members:
        excluded_groups = set()
        # For groups, we must add all group instances to the exclusions that
        # contain an excluded node instance as one of their members, AND we
        # must add all group instances that are excluded, as the
        # exclude_instances list could legitimately contain both
        for inst in exclude_instances:
            for group, members in groups_members.items():
                if inst in members or inst == group:
                    excluded_groups.add(group)
        planned_group_instances = len(groups_members) + delta
        if planned_group_instances < len(excluded_groups):
            return (
                'Instances from too many different groups were excluded. '
                'Target number of group instances was {group_count}, but '
                '{excluded_count} excluded groups.'.format(
                    group_count=planned_group_instances,
                    excluded_count=len(excluded_groups),
                )
            )
    else:
        planned_num_instances = len(available_instances) + delta
        if planned_num_instances < len(exclude_instances):
            return (
                'Target number of instances is less than excluded '
                'instance count. Target number of instances was '
                '{target}. Excluded instances were: '
                '{instances}. '.format(
                    target=planned_num_instances,
                    instances=', '.join(exclude_instances),
                )
            )

    # No problems found, return no error
    return ''


def _get_available_instances_list(available_instances, groups_members):
    """
        Get a string of available instances, given a list of instance IDs
        and a group membership list.
        This string will be formatted for providing helpful feedback to a
        user, e.g. regarding available instances when they have selected a
        non-existent instance.

        :param available_instances: A list of all available instance IDs. For
                                    groups this should include both the group
                                    instance IDs and the member node instance
                                    IDs.
                                    This will be ignored if groups_members
                                    contains anything.
        :param groups_members: A dict of group instances with their members,
                               e.g. produced by get_groups_with_members
        :return: A user-friendly string listing what instances are valid for
                 selection based on the provided inputs.
    """
    if groups_members:
        return 'Groups and member instances were: {groups}'.format(
            groups='; '.join(
                '{group}: {members}'.format(
                    group=group,
                    members=', '.join(members)
                )
                for group, members in groups_members.items()
            ),
        )
    else:
        return 'Available instances were: {instances}'.format(
            instances=', '.join(available_instances),
        )


def validate_inclusions_and_exclusions(include_instances, exclude_instances,
                                       available_instances,
                                       delta, scale_compute,
                                       groups_members=None):
    """
        Validate provided lists of included or excluded node instances for
        scale down operations.

        :param include_instances: A list of node instance IDs to include.
        :param exclude_instances: A list of node instance IDs to exclude.
        :param available_instances: A list of all available instance IDs. For
                                    groups this should include both the group
                                    instance IDs and the member node instance
                                    IDs.
        :param delta: How much to scale by. This is expected to be negative.
        :param scale_compute: A boolean determining whether scaling compute
                              instances containing the target nodes has been
                              requested.
        :param groups_members: A dict of group instances with their members,
                               e.g. produced by get_groups_with_members
        :raises: RuntimeError if there are validation issues.
    """
    if not include_instances and not exclude_instances:
        # We have no inclusions or exclusions, so they can't be wrong!
        return

    # Validate inclusions/exclusions
    error_message = ''
    missing_include = set(include_instances).difference(
        available_instances,
    )
    if missing_include:
        error_message += (
            'The following included instances did not exist: '
            '{instances}. '.format(instances=', '.join(
                missing_include,
            ))
        )
    missing_exclude = set(exclude_instances).difference(
        available_instances,
    )
    if missing_exclude:
        error_message += (
            'The following excluded instances did not exist: '
            '{instances}. '.format(instances=', '.join(
                missing_exclude,
            ))
        )
    instances_in_both = set(exclude_instances).intersection(
        include_instances,
    )

    if instances_in_both:
        error_message += (
            'The following instances were both excluded and '
            'included: {instances}. '.format(instances=', '.join(
                instances_in_both,
            ))
        )

    error_message += _check_for_too_many_exclusions(
        exclude_instances,
        available_instances,
        delta,
        groups_members,
    )

    if scale_compute:
        error_message += (
            'Cannot include or exclude instances while '
            'scale_compute is True. Please specify the '
            'desired compute instances and set scale_compute '
            'to False. '
        )

    # Abort if there are validation issues
    if error_message:
        error_message += _get_available_instances_list(
            available_instances,
            groups_members,
        )
        raise RuntimeError(error_message)


@workflow
def scale_entity(ctx,
                 scalable_entity_name,
                 delta,
                 scale_compute,
                 ignore_failure=False,
                 include_instances=None,
                 exclude_instances=None,
                 rollback_if_failed=True,
                 abort_started=False,
                 **kwargs):
    """Scales in/out the subgraph of node_or_group_name.

    If a node name is passed, and `scale_compute` is set to false, the
    subgraph will consist of all the nodes that are contained in the node and
    the node itself.
    If a node name is passed, and `scale_compute` is set to true, the subgraph
    will consist of all nodes that are contained in the compute node that
    contains the node and the compute node itself.
    If a group name or a node that is not contained in a compute
    node, is passed, this property is ignored.

    `delta` is used to specify the scale factor.
    For `delta > 0`: If current number of instances is `N`, scale out to
    `N + delta`.
    For `delta < 0`: If current number of instances is `N`, scale in to
    `N - |delta|`.

    :param ctx: cloudify context
    :param scalable_entity_name: the node or group name to scale
    :param delta: scale in/out factor
    :param scale_compute: should scale apply on compute node containing
                          the specified node
    :param ignore_failure: ignore operations failures in uninstall workflow
    :param include_instances: Instances to include when scaling down
    :param exclude_instances: Instances to exclude when scaling down
    :param rollback_if_failed: when False, no rollback will be triggered.
    :param abort_started: Remove any started deployment modifications
                          created prior to this scaling workflow
    """

    include_instances = include_instances or []
    exclude_instances = exclude_instances or []
    if not isinstance(include_instances, list):
        include_instances = [include_instances]
    if not isinstance(exclude_instances, list):
        exclude_instances = [exclude_instances]

    if not isinstance(delta, numbers.Integral):
        try:
            delta = int(delta)
        except ValueError:
            raise ValueError('The delta parameter must be a number. Got: {0}'
                             .format(delta))

    if delta == 0:
        ctx.logger.info('delta parameter is 0, so no scaling will take place.')
        return

    if delta > 0 and (include_instances or exclude_instances):
        raise ValueError(
            'Instances cannot be included or excluded when scaling up.'
        )

    scaling_group = ctx.deployment.scaling_groups.get(scalable_entity_name)
    if scaling_group:
        groups_members = get_groups_with_members(ctx)
        # Available instances for checking inclusions/exclusions needs to
        # include all groups and their members
        available_instances = set(
            chain.from_iterable(groups_members.values())
        ).union(groups_members)
        validate_inclusions_and_exclusions(
            include_instances,
            exclude_instances,
            available_instances=available_instances,
            delta=delta,
            scale_compute=scale_compute,
            groups_members=groups_members,
        )
        curr_num_instances = scaling_group['properties']['current_instances']
        planned_num_instances = curr_num_instances + delta
        scale_id = scalable_entity_name
    else:
        node = ctx.get_node(scalable_entity_name)
        if not node:
            raise ValueError("No scalable entity named {0} was found".format(
                scalable_entity_name))
        validate_inclusions_and_exclusions(
            include_instances,
            exclude_instances,
            available_instances=[instance.id for instance in node.instances],
            delta=delta,
            scale_compute=scale_compute,
        )
        host_node = node.host_node
        scaled_node = host_node if (scale_compute and host_node) else node
        curr_num_instances = scaled_node.number_of_instances
        planned_num_instances = curr_num_instances + delta
        scale_id = scaled_node.id

    if planned_num_instances < 0:
        raise ValueError('Provided delta: {0} is illegal. current number of '
                         'instances of entity {1} is {2}'
                         .format(delta,
                                 scalable_entity_name,
                                 curr_num_instances))

    if abort_started:
        _abort_started_deployment_modifications(ctx, ignore_failure)

    modification = ctx.deployment.start_modification({
        scale_id: {
            'instances': planned_num_instances,
            'removed_ids_exclude_hint': exclude_instances,
            'removed_ids_include_hint': include_instances,

            # While these parameters are now exposed, this comment is being
            # kept as it provides useful insight into the hints
            # These following parameters are not exposed at the moment,
            # but should be used to control which node instances get scaled in
            # (when scaling in).
            # They are mentioned here, because currently, the modification API
            # is not very documented.
            # Special care should be taken because if `scale_compute == True`
            # (which is the default), then these ids should be the compute node
            # instance ids which are not necessarily instances of the node
            # specified by `scalable_entity_name`.

            # Node instances denoted by these instance ids should be *kept* if
            # possible.
            # 'removed_ids_exclude_hint': [],

            # Node instances denoted by these instance ids should be *removed*
            # if possible.
            # 'removed_ids_include_hint': []
        }
    })
    graph = ctx.graph_mode()
    try:
        ctx.logger.info('Deployment modification started. '
                        '[modification_id={0}]'.format(modification.id))
        if delta > 0:
            added_and_related = set(modification.added.node_instances)
            added = set(i for i in added_and_related
                        if i.modification == 'added')
            related = added_and_related - added
            try:
                lifecycle.install_node_instances(
                    graph=graph,
                    node_instances=added,
                    related_nodes=related)
            except Exception:
                if not rollback_if_failed:
                    ctx.logger.error('Scale out failed.')
                    raise

                ctx.logger.error('Scale out failed, scaling back in.')
                for task in graph.tasks:
                    graph.remove_task(task)
                lifecycle.uninstall_node_instances(
                    graph=graph,
                    node_instances=added,
                    ignore_failure=ignore_failure,
                    related_nodes=related)
                raise
        else:
            removed_and_related = set(modification.removed.node_instances)
            removed = set(i for i in removed_and_related
                          if i.modification == 'removed')
            related = removed_and_related - removed
            lifecycle.uninstall_node_instances(
                graph=graph,
                node_instances=removed,
                ignore_failure=ignore_failure,
                related_nodes=related)
    except Exception:
        if not rollback_if_failed:
            raise

        ctx.logger.warn('Rolling back deployment modification. '
                        '[modification_id={0}]'.format(modification.id))
        try:
            modification.rollback()
        except Exception:
            ctx.logger.warn('Deployment modification rollback failed. The '
                            'deployment model is most likely in some corrupted'
                            ' state.'
                            '[modification_id={0}]'.format(modification.id))
            raise
        raise
    else:
        try:
            modification.finish()
        except Exception:
            ctx.logger.warn('Deployment modification finish failed. The '
                            'deployment model is most likely in some corrupted'
                            ' state.'
                            '[modification_id={0}]'.format(modification.id))
            raise


def _abort_started_deployment_modifications(ctx, ignore_failure):
    """Aborts any started deployment modifications running in this context.

    :param ctx: cloudify context
    :param ignore_failure: ignore operations failures in uninstall workflow
    """
    started_modifications = ctx.deployment.list_started_modifications()
    graph = ctx.graph_mode()
    for modification in started_modifications:
        ctx.logger.info('Rolling back deployment modification. '
                        '[modification_id=%s]', modification.id)
        added_and_related = set(modification.added.node_instances)
        added = set(i for i in added_and_related
                    if i.modification == 'added')
        related = added_and_related - added
        if added:
            lifecycle.uninstall_node_instances(
                graph=graph,
                node_instances=added,
                ignore_failure=ignore_failure,
                related_nodes=related,
            )
        modification.rollback()


# Kept for backward compatibility with older versions of types.yaml
@workflow
def scale(ctx, node_id, delta, scale_compute, **kwargs):
    return scale_entity(ctx=ctx,
                        scalable_entity_name=node_id,
                        delta=delta,
                        scale_compute=scale_compute,
                        **kwargs)


def _filter_node_instances(ctx, node_ids, node_instance_ids, type_names):
    filtered_node_instances = []
    for node in ctx.nodes:
        if node_ids and node.id not in node_ids:
            continue
        if type_names and not next((type_name for type_name in type_names if
                                    type_name in node.type_hierarchy), None):
            continue

        for instance in node.instances:
            if node_instance_ids and instance.id not in node_instance_ids:
                continue
            filtered_node_instances.append(instance)
    return filtered_node_instances


def _get_all_host_instances(ctx):
    node_instances = set()
    for node_instance in ctx.node_instances:
        if lifecycle.is_host_node(node_instance):
            node_instances.add(node_instance)
    return node_instances


@make_or_get_graph
def _make_install_agents_graph(
        ctx, install_agent_timeout, node_ids,
        node_instance_ids, install_methods=None, validate=True,
        install=True, manager_ip=None, manager_certificate=None,
        stop_old_agent=False, **_):
    hosts = _create_hosts_list(ctx, node_ids, node_instance_ids,
                               install_methods)
    _assert_hosts_started(hosts)
    graph = ctx.graph_mode()
    if validate:
        validate_subgraph = _add_validate_to_task_graph(
            graph,
            hosts,
            current_amqp=False,
            manager_ip=manager_ip,
            manager_certificate=manager_certificate
        )
    if install:
        install_subgraph = graph.subgraph('install')
        for host in hosts:
            seq = install_subgraph.sequence()
            seq.add(
                host.send_event('Installing new agent'),
                host.execute_operation(
                    'cloudify.interfaces.cloudify_agent.create_amqp',
                    kwargs={
                        'install_agent_timeout': install_agent_timeout,
                        'manager_ip': manager_ip,
                        'manager_certificate': manager_certificate,
                        'stop_old_agent': stop_old_agent
                    },
                    allow_kwargs_override=True),
                host.send_event('New agent installed.'),
                host.execute_operation(
                    'cloudify.interfaces.cloudify_agent.validate_amqp',
                    kwargs={'current_amqp': True}),
                *lifecycle.prepare_running_agent(host)
            )
            for subnode in host.get_contained_subgraph():
                seq.add(subnode.execute_operation(
                    'cloudify.interfaces.monitoring.start'))
    if validate and install:
        graph.add_dependency(install_subgraph, validate_subgraph)
    return graph


@workflow(resumable=True)
def install_new_agents(ctx, **kwargs):
    graph = _make_install_agents_graph(ctx, name='install_agents', **kwargs)
    graph.execute()


@workflow(resumable=True)
def start(ctx, operation_parms, run_by_dependency_order, type_names, node_ids,
          node_instance_ids, **kwargs):
    execute_operation(ctx, 'cloudify.interfaces.lifecycle.start',
                      operation_parms, True, run_by_dependency_order,
                      type_names, node_ids, node_instance_ids, **kwargs)


@workflow(resumable=True)
def stop(ctx, operation_parms, run_by_dependency_order, type_names, node_ids,
         node_instance_ids, **kwargs):
    execute_operation(ctx, 'cloudify.interfaces.lifecycle.stop',
                      operation_parms, True, run_by_dependency_order,
                      type_names, node_ids, node_instance_ids, **kwargs)


@workflow(resumable=True)
def restart(ctx, stop_parms, start_parms, run_by_dependency_order, type_names,
            node_ids, node_instance_ids, **kwargs):
    stop(ctx, stop_parms, run_by_dependency_order, type_names,
         node_ids, node_instance_ids, **kwargs)
    start(ctx, start_parms, run_by_dependency_order, type_names,
          node_ids, node_instance_ids, **kwargs)


@make_or_get_graph
def _make_execute_operation_graph(ctx, operation, operation_kwargs,
                                  allow_kwargs_override,
                                  run_by_dependency_order, type_names,
                                  node_ids, node_instance_ids, **kwargs):
    graph = ctx.graph_mode()
    subgraphs = {}

    # filtering node instances
    filtered_node_instances = _filter_node_instances(
        ctx=ctx,
        node_ids=node_ids,
        node_instance_ids=node_instance_ids,
        type_names=type_names)

    if run_by_dependency_order:
        # if run by dependency order is set, then create stub subgraphs for the
        # rest of the instances. This is done to support indirect
        # dependencies, i.e. when instance A is dependent on instance B
        # which is dependent on instance C, where A and C are to be executed
        # with the operation on (i.e. they're in filtered_node_instances)
        # yet B isn't.
        # We add stub subgraphs rather than creating dependencies between A
        # and C themselves since even though it may sometimes increase the
        # number of dependency relationships in the execution graph, it also
        # ensures their number is linear to the number of relationships in
        # the deployment (e.g. consider if A and C are one out of N instances
        # of their respective nodes yet there's a single instance of B -
        # using subgraphs we'll have 2N relationships instead of N^2).
        filtered_node_instances_ids = set(inst.id for inst in
                                          filtered_node_instances)
        for instance in ctx.node_instances:
            if instance.id not in filtered_node_instances_ids:
                subgraphs[instance.id] = graph.subgraph(instance.id)

    # preparing the parameters to the execute_operation call
    exec_op_params = {
        'kwargs': operation_kwargs,
        'operation': operation
    }
    if allow_kwargs_override is not None:
        exec_op_params['allow_kwargs_override'] = allow_kwargs_override

    # registering actual tasks to sequences
    for instance in filtered_node_instances:
        start_event_message = 'Starting operation {0}'.format(operation)
        if operation_kwargs:
            start_event_message += ' (Operation parameters: {0})'.format(
                operation_kwargs)
        subgraph = graph.subgraph(instance.id)
        sequence = subgraph.sequence()
        sequence.add(
            instance.send_event(start_event_message),
            instance.execute_operation(**exec_op_params),
            instance.send_event('Finished operation {0}'.format(operation)))
        subgraphs[instance.id] = subgraph

    # adding tasks dependencies if required
    if run_by_dependency_order:
        for instance in ctx.node_instances:
            for rel in instance.relationships:
                graph.add_dependency(subgraphs[instance.id],
                                     subgraphs[rel.target_id])
    return graph


@workflow(resumable=True)
def execute_operation(ctx, operation, *args, **kwargs):
    """ A generic workflow for executing arbitrary operations on nodes """
    name = 'execute_operation_{0}'.format(operation)
    graph = _make_execute_operation_graph(
        ctx, operation, name=name, *args, **kwargs)
    graph.execute()


@workflow
def update(ctx,
           update_id,
           added_instance_ids,
           added_target_instances_ids,
           removed_instance_ids,
           remove_target_instance_ids,
           modified_entity_ids,
           extended_instance_ids,
           extend_target_instance_ids,
           reduced_instance_ids,
           reduce_target_instance_ids,
           skip_install,
           skip_uninstall,
           ignore_failure=False,
           install_first=False,
           node_instances_to_reinstall=None,
           central_plugins_to_install=None,
           central_plugins_to_uninstall=None,
           update_plugins=True):
    node_instances_to_reinstall = node_instances_to_reinstall or []
    instances_by_change = {
        'added_instances': (added_instance_ids, []),
        'added_target_instances_ids': (added_target_instances_ids, []),
        'removed_instances': (removed_instance_ids, []),
        'remove_target_instance_ids': (remove_target_instance_ids, []),
        'extended_and_target_instances':
            (extended_instance_ids + extend_target_instance_ids, []),
        'reduced_and_target_instances':
            (reduced_instance_ids + reduce_target_instance_ids, []),
    }
    for instance in ctx.node_instances:
        instance_holders = [instance_holder
                            for _, (changed_ids, instance_holder)
                            in instances_by_change.items()
                            if instance.id in changed_ids]
        for instance_holder in instance_holders:
            instance_holder.append(instance)

    graph = ctx.graph_mode()
    to_install = set(instances_by_change['added_instances'][1])
    to_uninstall = set(instances_by_change['removed_instances'][1])

    def _install():
        def _install_nodes():
            if skip_install:
                return
            # Adding nodes or node instances should be based on modified
            # instances
            lifecycle.install_node_instances(
                graph=graph,
                node_instances=to_install,
                related_nodes=set(
                    instances_by_change['added_target_instances_ids'][1])
            )

            # This one as well.
            lifecycle.execute_establish_relationships(
                graph=graph,
                node_instances=set(
                    instances_by_change['extended_and_target_instances'][1]),
                modified_relationship_ids=modified_entity_ids['relationship']
            )

        _install_nodes()
        _install_plugins_on_agent()

    def _uninstall():
        def _uninstall_nodes():
            if skip_uninstall:
                return
            lifecycle.execute_unlink_relationships(
                graph=graph,
                node_instances=set(
                    instances_by_change['reduced_and_target_instances'][1]),
                modified_relationship_ids=modified_entity_ids['relationship']
            )

            lifecycle.uninstall_node_instances(
                graph=graph,
                node_instances=to_uninstall,
                ignore_failure=ignore_failure,
                related_nodes=set(
                    instances_by_change['remove_target_instance_ids'][1])
            )

        _uninstall_nodes()
        _uninstall_plugins_on_agent()

    def _reinstall():
        subgraph = set([])
        for node_instance_id in node_instances_to_reinstall:
            subgraph |= ctx.get_node_instance(
                node_instance_id).get_contained_subgraph()
        subgraph -= to_uninstall
        intact_nodes = set(ctx.node_instances) - subgraph - to_uninstall
        for n in subgraph:
            for r in n._relationship_instances:
                if r in removed_instance_ids:
                    n._relationship_instances.pop(r)
        lifecycle.reinstall_node_instances(graph=graph,
                                           node_instances=subgraph,
                                           related_nodes=intact_nodes,
                                           ignore_failure=ignore_failure)

    def _uninstall_plugins_on_agent():
        if not update_plugins:
            return
        _handle_plugin_after_update(
            ctx, modified_entity_ids['plugin'], 'remove')

    def _install_plugins_on_agent():
        if not update_plugins:
            return
        _handle_plugin_after_update(
            ctx, modified_entity_ids['plugin'], 'add')

    def _update_central_plugins():
        if not update_plugins:
            return
        sequence = graph.sequence()
        add_plugins_to_uninstall(ctx, central_plugins_to_uninstall, sequence)
        add_plugins_to_install(ctx, central_plugins_to_install, sequence)
        graph.execute()

    if install_first:
        _install()
        _uninstall()
    else:
        _uninstall()
        _install()
    _reinstall()

    _update_central_plugins()

    # Finalize the commit (i.e. remove relationships or nodes)
    client = get_rest_client()
    client.deployment_updates.finalize_commit(update_id)


@workflow(resumable=True)
def validate_agents(ctx, node_ids, node_instance_ids,
                    install_methods=None, **_):

    hosts = _create_hosts_list(ctx, node_ids, node_instance_ids,
                               install_methods)
    # Make sure all hosts' state is started
    _assert_hosts_started(hosts)

    # Add validate_amqp to task graph
    graph = ctx.graph_mode()
    _add_validate_to_task_graph(graph, hosts, current_amqp=True)
    graph.execute()


def _create_hosts_list(ctx, node_ids, node_instance_ids, install_methods=None):
    if install_methods is None:
        install_methods = constants.AGENT_INSTALL_METHODS_INSTALLED
    if node_ids or node_instance_ids:
        filtered_node_instances = _filter_node_instances(
            ctx=ctx,
            node_ids=node_ids,
            node_instance_ids=node_instance_ids,
            type_names=[])
        errors = list()
        for node_instance in filtered_node_instances:
            if not lifecycle.is_host_node(node_instance):
                errors.append('Node instance {0} is not host.'.format(
                    node_instance.id))
            elif utils.internal.get_install_method(
                    node_instance.node.properties) \
                    == constants.AGENT_INSTALL_METHOD_NONE:
                errors.append(
                    'Agent should not be installed on node instance '
                    '{0}').format(node_instance.id)
        if errors:
            raise ValueError('Specified filters are not correct:\n{0}'.format(
                '\n'.join(errors)))
        hosts = filtered_node_instances
    else:
        hosts = [host for host in _get_all_host_instances(ctx)
                 if utils.internal.get_install_method(host.node.properties) in
                 install_methods]
    return hosts


def _assert_hosts_started(hosts):
    for host in hosts:
        state = host.get_state().get()
        if state != 'started':
            raise RuntimeError('Node {0} is not started (state: {1})'.format(
                host.id,
                state))


def _add_validate_to_task_graph(graph, hosts, current_amqp, manager_ip=None,
                                manager_certificate=None):
    validate_subgraph = graph.subgraph('validate')
    for host in hosts:
        seq = validate_subgraph.sequence()
        seq.add(
            host.send_event('Validating agent connection.'),
            host.execute_operation(
                'cloudify.interfaces.cloudify_agent.validate_amqp',
                kwargs={'current_amqp': current_amqp,
                        'manager_ip': manager_ip,
                        'manager_certificate': manager_certificate}),
            host.send_event('Validation done'))
    return validate_subgraph


def _handle_plugin_after_update(ctx, plugins_list, action):
    """ Either install or uninstall plugins on the relevant hosts """

    prefix = 'I' if action == 'add' else 'Uni'
    message = '{0}nstalling plugins'.format(prefix)

    graph = ctx.graph_mode()
    plugin_subgraph = graph.subgraph('handle_plugins')

    # The plugin_list is a list of (possibly empty) dicts that may contain
    # `add`/`remove` keys and (node_id, plugin_dict) values. E.g.
    # [{}, {}, {'add': (NODE_ID, PLUGIN_DICT)},
    # {'add': (NODE_ID, PLUGIN_DICT), 'remove': (NODE_ID, PLUGIN_DICT)}]
    # So we filter out only those dicts that have the relevant action
    plugins_to_handle = [p[action] for p in plugins_list if p.get(action)]

    # The list might contain duplicates, and it's organized in the following
    # way: [(node_id, plugin_dict), (node_id, plugin_to_handle), ...] so
    # we reorganize it into: {node_id: [list_of_plugins], ...}
    node_to_plugins_map = {}
    for node, plugin in plugins_to_handle:
        plugin_list = node_to_plugins_map.setdefault(node, [])
        if plugin not in plugin_list:
            plugin_list.append(plugin)

    for node_id, plugins in node_to_plugins_map.items():
        if not plugins:
            continue

        instances = ctx.get_node(node_id).instances
        for instance in instances:
            if action == 'add':
                task = lifecycle.plugins_install_task(instance, plugins)
            else:
                task = lifecycle.plugins_uninstall_task(instance, plugins)

            if task:
                seq = plugin_subgraph.sequence()
                seq.add(
                    instance.send_event(message),
                    task
                )
    graph.execute()


@workflow(resumable=True)
def rollback(ctx,
             type_names,
             node_ids,
             node_instance_ids,
             full_rollback=False,
             **kwargs):
    """Rollback workflow.

    Rollback workflow will look at each node state, decide if the node state
    is unresolved, and for those that are, execute the corresponding node
    operation that will get us back to a resolved node state, and then
    execute the unfinished workflow.
    Unresolved states are: creating, configuring, starting.
    Nodes that are in `creating` and `configuring` states will rollback to
    `uninitialized` state.
    Nodes that are in `starting` state will rollback to `configured` state.
    :param ctx : Cloudify context
    :param type_names: A list of type names. The operation will be executed
          only on node instances which are of these types or of types which
          (recursively) derive from them. An empty list means no filtering
          will take place and all type names are valid.
    :param node_ids: A list of node ids. The operation will be executed only
          on node instances which are instances of these nodes. An empty list
          means no filtering will take place and all nodes are valid.
    :param node_instance_ids: A list of node instance ids. The operation will
          be executed only on the node instances specified. An empty list
          means no filtering will take place and all node instances are valid.
    :param full_rollback Whether to perform uninstall after rollback to
    resolved state.
    """
    # Find all node instances in unresolved state
    unresolved_node_instances = _find_all_unresolved_node_instances(
        ctx,
        node_ids,
        node_instance_ids,
        type_names)

    ctx.logger.debug("unresolved node instances: %s",
                     [instance.id for instance in unresolved_node_instances])
    intact_nodes = set(ctx.node_instances) - set(unresolved_node_instances)
    ctx.logger.debug("intact node instances: %s",
                     [instance.id for instance in intact_nodes])

    lifecycle.rollback_node_instances(
        graph=ctx.graph_mode(),
        node_instances=set(unresolved_node_instances),
        related_nodes=intact_nodes
    )
    ctx.refresh_node_instances()
    if full_rollback:
        ctx.logger.debug("Start uninstall after rollback.")
        lifecycle.uninstall_node_instances(
            graph=ctx.graph_mode(),
            node_instances=set(ctx.node_instances),
            ignore_failure=False,
            name_prefix='uninstall-a')


def _find_all_unresolved_node_instances(ctx,
                                        node_ids,
                                        node_instance_ids,
                                        type_names):
    unresolved_states = ['creating', 'configuring', 'starting']
    unresolved_node_instances = []
    filtered_node_instances = _filter_node_instances(
        ctx=ctx,
        node_ids=node_ids,
        node_instance_ids=node_instance_ids,
        type_names=type_names)

    for instance in filtered_node_instances:
        if instance.state in unresolved_states:
            unresolved_node_instances.append(instance)
    return unresolved_node_instances


@workflow(resumable=True)
def pull(ctx,
         operation_parms,
         run_by_dependency_order,
         type_names,
         node_ids,
         node_instance_ids,
         **kwargs):
    """
    Pull workflow will execute the "pull" operation on each node instance.
    :param ctx : Cloudify context
    :param operation_parms: A dictionary of keyword arguments that will be
        passed to the operation invocation (Default: {}).
    :param run_by_dependency_order: A boolean describing whether the operation
        should execute on the relevant nodes according to the order of their
        relationships dependencies or rather execute on all relevant nodes in
        parallel (Default: true).
    :param type_names: A list of type names. The operation will be executed
          only on node instances which are of these types or of types which
          (recursively) derive from them. An empty list means no filtering
          will take place and all type names are valid.
    :param node_ids: A list of node ids. The operation will be executed only
          on node instances which are instances of these nodes. An empty list
          means no filtering will take place and all nodes are valid.
    :param node_instance_ids: A list of node instance ids. The operation will
          be executed only on the node instances specified. An empty list
          means no filtering will take place and all node instances are valid.
    """
    execute_operation(ctx, 'cloudify.interfaces.lifecycle.pull',
                      operation_parms, True, run_by_dependency_order,
                      type_names, node_ids, node_instance_ids, **kwargs)
