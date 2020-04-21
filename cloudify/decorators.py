########
# Copyright (c) 2013 GigaSpaces Technologies Ltd. All rights reserved
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

import functools

from . import ctx, manager
from .utils import get_instances_of_node
from .exceptions import CloudifySerializationRetry


def operation(func=None, resumable=False, **kwargs):
    """This decorator is not required for operations to work.

    Use this for readability, and to add additional markers for the function
    (eg. is it resumable).
    """
    if func:
        func.resumable = resumable
        return func
    else:
        return lambda fn: operation(fn, resumable=resumable, **kwargs)


def workflow(func=None, system_wide=False, resumable=False, **kwargs):
    """This decorator should only be used to decorate system wide
       workflows. It is not required for regular workflows.
    """
    if func:
        func.workflow_system_wide = system_wide
        func.resumable = resumable
        return func
    else:
        return lambda fn: workflow(fn, system_wide=system_wide,
                                   resumable=resumable, **kwargs)


def serial_operation(threshold=0,
                     workflows=None,
                     states=None,
                     **op_kwargs):
    """Control order of multiple node instances of a single node template.
        Normally, multiple node instance operations will be
        executed in parallel.
        However, some cases, like clusters require that cluster members are
        installed serially.

    :param threshold: Switch back to parallel execution
        after n node instances. Integer.
        0: Never use parallel. Always straight serial.
        1: After 1 node is installed, return to Parallel. Default.
        n + 1: After n node instances.
    :param workflows: List of workflows to apply serialization
        to. The default is ['install']. This means we scale will
        execute in parallel.
    :param states: List of states that the preceding
        node instance may be in before executing the current node instance.
        The default is ['started']. We will only continue to next instance
        after the preceding instance is in "started" state.
    :raises: CloudifySerializationRetry.
    :return: decorator wrapper
    """
    workflows = workflows or ['install']
    states = states or ['started']

    def outer_wrapper(func):
        @functools.wraps(func)
        def inner_wrapper(*args, **kwargs):
            _ctx = kwargs.get('ctx', ctx)
            rest_client = manager.get_rest_client()
            if _ctx.workflow_id in workflows:
                node_instances = get_instances_of_node(
                    rest_client,
                    _ctx.deployment.id,
                    _ctx.node.id)
                not_blocking_instances = sum(
                    1 for ni in node_instances if ni.state in states)
                if threshold == 0 or threshold <= not_blocking_instances:
                    priority = [ni for ni in node_instances if
                                ni.index < _ctx.instance.index]
                    if not all(ni.state in states for ni in priority):
                        raise CloudifySerializationRetry(
                            'Serial Cloudify Operation. '
                            'Waiting for the following '
                            'node instances to be ready: '
                            '{priority_instances}.'.format(
                                priority_instances=[(p.id, p.state) for p
                                                    in priority]
                            ))
            return func(*args, **kwargs)
        return operation(func=inner_wrapper, **op_kwargs)
    return outer_wrapper


task = operation
