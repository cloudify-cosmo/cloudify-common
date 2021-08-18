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

import mock
import time
import testtools
from contextlib import contextmanager

from cloudify_rest_client.operations import Operation

from cloudify.exceptions import WorkflowFailed
from cloudify.workflows import api
from cloudify.workflows import tasks
from cloudify.workflows.tasks_graph import TaskDependencyGraph

from cloudify.state import current_workflow_ctx
from cloudify.plugins.lifecycle import LifecycleProcessor
from cloudify.workflows.workflow_context import (
    _WorkflowContextBase,
    WorkflowNodesAndInstancesContainer
)
from cloudify_rest_client.node_instances import NodeInstance
from cloudify_rest_client.nodes import Node


@contextmanager
def limited_sleep_mock(limit=100):
    # using lists to allow a 'nonlocal' set in py2
    current_time = [time.time()]
    calls = [0]

    def _fake_sleep(delta):
        current_time[0] += delta
        calls[0] += 1
        if calls[0] > limit:
            raise RuntimeError('Timeout')

    def _fake_time():
        return current_time[0]

    mock_sleep = mock.patch('time.sleep', _fake_sleep)
    mock_time = mock.patch('time.time', _fake_time)
    with mock_sleep:
        with mock_time:
            yield mock_sleep, mock_time


class MockWorkflowContext(_WorkflowContextBase):
    wait_after_fail = 600
    dry_run = False
    resume = False

    def __init__(self):
        super(MockWorkflowContext, self).__init__({}, lambda *a: mock.Mock())
        self.internal.handler.operation_cloudify_context = {}


class TestTasksGraphExecute(testtools.TestCase):
    def test_executes_single_task(self):
        """A single NOP task is executed within a single iteration of the
        tasks graph loop"""
        g = TaskDependencyGraph(MockWorkflowContext())
        task = tasks.NOPLocalWorkflowTask(mock.Mock())
        g.add_task(task)
        with limited_sleep_mock(limit=1):
            g.execute()
        self.assertTrue(task.is_terminated)

    def test_executes_multiple_concurrent(self):
        """Independent tasks will be executed concurrently within the same
        iteration of the graph loop.
        """
        g = TaskDependencyGraph(MockWorkflowContext())
        task1 = tasks.NOPLocalWorkflowTask(mock.Mock())
        task2 = tasks.NOPLocalWorkflowTask(mock.Mock())
        g.add_task(task1)
        g.add_task(task2)
        with limited_sleep_mock(limit=1):
            g.execute()
        self.assertTrue(task1.is_terminated)
        self.assertTrue(task2.is_terminated)

    def test_task_failed(self):
        """Execution is stopped when a task failed. The next task is not
        executed"""
        class FailedTask(tasks.WorkflowTask):
            name = 'failtask'

            def apply_async(self):
                self.async_result.result = None
                self.set_state(tasks.TASK_FAILED)
                return self.async_result

        task1 = FailedTask(mock.Mock(), total_retries=0)
        task2 = mock.Mock(execute_after=0)

        g = TaskDependencyGraph(MockWorkflowContext())
        seq = g.sequence()
        seq.add(task1, task2)

        with limited_sleep_mock():
            self.assertRaisesRegex(WorkflowFailed, 'failtask', g.execute)
        self.assertTrue(task1.is_terminated)
        self.assertFalse(task2.apply_async.called)

    def test_wait_after_fail(self):
        """When a task fails, the already-running tasks are waited for"""
        class FailedTask(tasks.WorkflowTask):
            """Task that fails 1 second after starting"""
            name = 'failtask'

            def apply_async(self):
                self.set_state(tasks.TASK_FAILED)
                self.async_result.result = tasks.HandlerResult.fail()
                return self.async_result

            def handle_task_terminated(self):
                rv = super(FailedTask, self).handle_task_terminated()
                task2.set_state(tasks.TASK_SUCCEEDED)
                task2.async_result.result = None
                return rv

        class DelayedTask(tasks.WorkflowTask):
            """Task that succeeds 3 seconds after starting"""
            name = 'delayedtask'
            handle_task_terminated = mock.Mock()

        task1 = FailedTask(mock.Mock(), total_retries=0)
        task2 = DelayedTask(mock.Mock())

        g = TaskDependencyGraph(MockWorkflowContext())
        g.add_task(task1)
        g.add_task(task2)
        self.assertRaisesRegex(WorkflowFailed, 'failtask', g.execute)

        # even though the workflow failed 1 second in, the other task was
        # still waited for and completed
        task2.handle_task_terminated.assert_called()

    def test_task_sequence(self):
        """Tasks in a sequence are called in order"""

        class Task(tasks.WorkflowTask):
            name = 'task'

            def apply_async(self):
                record.append(self.i)
                self.set_state(tasks.TASK_SUCCEEDED)
                self.async_result.result = None
                return self.async_result

        task_count = 10

        # prepare the task seuqence
        seq_tasks = []
        for i in range(task_count):
            t = Task(mock.Mock())
            seq_tasks.append(t)
            t.i = i
        g = TaskDependencyGraph(MockWorkflowContext())
        seq = g.sequence()
        seq.add(*seq_tasks)

        record = []

        with limited_sleep_mock():
            g.execute()

        expected = list(range(task_count))
        self.assertEqual(expected, record)

    def test_cancel(self):
        """When execution is cancelled, an error is thrown and tasks are
        not executed.
        """
        g = TaskDependencyGraph(MockWorkflowContext())
        task = mock.Mock()
        g.add_task(task)
        with mock.patch('cloudify.workflows.api.cancel_request', True):
            self.assertRaises(api.ExecutionCancelled, g.execute)

        self.assertFalse(task.apply_async.called)
        self.assertFalse(task.cancel.called)


class _CustomRestorableTask(tasks.WorkflowTask):
    """A custom user-provided task, that can be restored"""
    name = '_CustomRestorableTask'
    task_type = 'cloudify.tests.test_tasks_graph._CustomRestorableTask'


class TestTaskGraphRestore(testtools.TestCase):
    def _remote_task(self):
        """Make a RemoteWorkflowTask mock for use in tests"""
        return {
            'type': 'RemoteWorkflowTask',
            'dependencies': [],
            'parameters': {
                'info': {},
                'current_retries': 0,
                'send_task_events': False,
                'containing_subgraph': None,
                'task_kwargs': {
                    'kwargs': {
                        '__cloudify_context': {}
                    }
                }
            }
        }

    def _subgraph(self):
        """Make a SubgraphTask mock for use in tests"""
        return {
            'type': 'SubgraphTask',
            'id': 0,
            'dependencies': [],
            'parameters': {
                'info': {},
                'current_retries': 0,
                'send_task_events': False,
                'containing_subgraph': None,
                'task_kwargs': {}
            }
        }

    def _restore_graph(self, operations):
        mock_wf_ctx = mock.Mock()
        mock_wf_ctx.get_operations.return_value = [
            Operation(op) for op in operations]
        mock_retrieved_graph = mock.Mock(id=0)
        return TaskDependencyGraph.restore(mock_wf_ctx, mock_retrieved_graph)

    def test_restore_empty(self):
        """Restoring an empty list of operations results in an empty graph"""
        assert self._restore_graph([]).tasks == []

    def test_restore_single(self):
        """A single operation is restored into the graph"""
        graph = self._restore_graph([self._remote_task()])
        assert len(graph.tasks) == 1
        assert isinstance(graph.tasks[0], tasks.RemoteWorkflowTask)

    def test_restore_finished(self):
        """Finished tasks are not restored into the graph"""
        task = self._remote_task()
        task['state'] = tasks.TASK_SUCCEEDED
        graph = self._restore_graph([task])
        assert graph.tasks == []

    def test_restore_with_subgraph(self):
        """Restoring operations keeps subgraph structure"""
        subgraph = self._subgraph()
        task = self._remote_task()
        subgraph['id'] = 15
        task['parameters']['containing_subgraph'] = 15

        graph = self._restore_graph([subgraph, task])
        assert len(graph.tasks) == 2
        subgraphs = [op for op in graph.tasks if op.is_subgraph]
        remote_tasks = [op for op in graph.tasks if not op.is_subgraph]

        assert len(subgraphs) == 1
        assert len(remote_tasks) == 1

        assert len(subgraphs[0].tasks) == 1
        assert remote_tasks[0].containing_subgraph is subgraphs[0]

    def test_restore_with_dependencies(self):
        """Restoring operations keeps the dependency structure"""
        task1 = self._remote_task()
        task1['id'] = 1
        task2 = self._remote_task()
        task2['id'] = 2
        task2['dependencies'] = [1]

        graph = self._restore_graph([task1, task2])
        assert len(graph.tasks) == 2
        assert graph._dependencies[2] == set([1])

    def test_restore_with_finished_subgraph(self):
        """Restoring operations keeps subgraph structure"""
        subgraph = self._subgraph()
        task = self._remote_task()
        subgraph['id'] = 15
        task['parameters']['containing_subgraph'] = 15

        subgraph['state'] = tasks.TASK_SUCCEEDED

        graph = self._restore_graph([subgraph, task])
        assert len(graph.tasks) == 2
        subgraphs = [op for op in graph.tasks if op.is_subgraph]
        remote_tasks = [op for op in graph.tasks if not op.is_subgraph]

        assert len(subgraphs) == 1
        assert len(remote_tasks) == 1

        assert len(subgraphs[0].tasks) == 1
        assert remote_tasks[0].containing_subgraph is subgraphs[0]

    def test_restore_multiple_in_subgraph(self):
        """Multiple tasks in the same subgraph, reference the same subgraph.

        As opposed to restoring the same subgraph multiple times, it's all
        references to one object. If it was restored multiple times, then
        the next task in the subgraph would still run even if a previous
        task failed.
        """
        subgraph = self._subgraph()
        subgraph['id'] = 15
        task1 = self._remote_task()
        task1['id'] = 1
        task2 = self._remote_task()
        task2['id'] = 2
        task1['parameters']['containing_subgraph'] = 15
        task2['parameters']['containing_subgraph'] = 15

        graph = self._restore_graph([subgraph, task1, task2])
        assert len(graph.tasks) == 3
        subgraphs = [op for op in graph.tasks if op.is_subgraph]
        remote_tasks = [op for op in graph.tasks if not op.is_subgraph]

        # those are all references to the same subgraph, the subgraph was
        # NOT restored multiple times
        assert remote_tasks[0].containing_subgraph \
            is remote_tasks[1].containing_subgraph \
            is subgraphs[0]

        assert len(subgraphs[0].tasks) == 2

    def test_restore_custom(self):
        task = _CustomRestorableTask(None)
        serialized = task.dump()
        # dependencies are added by the graph normally, not by task.dump
        serialized['dependencies'] = []

        graph = self._restore_graph([serialized])
        assert len(graph.tasks) == 1
        assert isinstance(graph.tasks[0], _CustomRestorableTask)
        # ..but we didn't just get the same object back, it was restored indeed
        assert graph.tasks[0] is not task


class NonExecutingGraph(TaskDependencyGraph):
    """A TaskDependencyGraph that never actually executes anything"""
    def execute(self):
        return

    def store(self, name=None):
        pass


class TestLifecycleGraphs(testtools.TestCase):
    def _make_ctx_and_graph(self):
        ctx = MockWorkflowContext()
        graph = NonExecutingGraph(ctx)
        ctx.get_tasks_graph = mock.Mock(return_value=None)
        ctx.get_operations = mock.Mock(return_value=[])
        ctx.internal.graph_mode = True
        return ctx, graph

    def _make_node(self, **kwargs):
        node = {
            'id': 'node1',
            'relationships': [],
            'operations': {},
            'id': 'node1',
            'type_hierarchy': []
        }
        node.update(kwargs)
        return Node(node)

    def _make_instance(self, **kwargs):
        instance = {
            'id': 'node1_1',
            'node_id': 'node1',
            'relationships': [],
        }
        instance.update(kwargs)
        return NodeInstance(instance)

    def _make_operation(self, **kwargs):
        operation = {
            'operation': 'plugin1.op1',
            'plugin': 'plugin1',
            'has_intrinsic_functions': False,
            'executor': 'host_agent',
            'max_retries': 10,
            'retry_interval': 10,
        }
        operation.update(kwargs)
        return operation

    def _make_lifecycle_processor(self, ctx, graph, nodes, instances):
        container = WorkflowNodesAndInstancesContainer(
            ctx, nodes, instances)
        return LifecycleProcessor(
            graph,
            node_instances=list(container.node_instances)
        )

    def _make_plugin(self, name='plugin1'):
        return {'name': name, 'package_name': name}

    def test_install_empty(self):
        """No instances - no operations"""
        ctx, graph = self._make_ctx_and_graph()
        pr = LifecycleProcessor(graph)
        with current_workflow_ctx.push(ctx):
            pr.install()
        assert graph.tasks == []

    def test_install_empty_instance(self):
        """Instance without interfaces - still set to started"""
        ctx, graph = self._make_ctx_and_graph()
        pr = self._make_lifecycle_processor(
            ctx, graph,
            nodes=[self._make_node()],
            instances=[self._make_instance()]
        )
        with current_workflow_ctx.push(ctx):
            pr.install()
        assert any(
            task.name == 'SetNodeInstanceStateTask'
            and task.info == 'started'
            for task in graph.tasks
        )

    def test_install_create_operation(self):
        """Instance with a create interface - the operation is called"""
        ctx, graph = self._make_ctx_and_graph()

        pr = self._make_lifecycle_processor(
            ctx, graph,
            nodes=[self._make_node(
                operations={
                    'cloudify.interfaces.lifecycle.create':
                    self._make_operation()
                },
                plugins=[self._make_plugin()]
            )],
            instances=[self._make_instance()]
        )
        with current_workflow_ctx.push(ctx):
            pr.install()
        assert any(
            task.name == 'plugin1.op1'
            for task in graph.tasks
        )
        assert any(
            task.name == 'SetNodeInstanceStateTask'
            and task.info == 'started'
            for task in graph.tasks
        )

    def test_update_resumed_install(self):
        """When resuming an interrupted install, the instance is deleted first
        """
        ctx, graph = self._make_ctx_and_graph()

        node = self._make_node(
            operations={
                'cloudify.interfaces.lifecycle.create':
                self._make_operation(operation='plugin1.op1'),
                'cloudify.interfaces.lifecycle.delete':
                self._make_operation(operation='plugin1.op2')
            },
            plugins=[{'name': 'plugin1', 'package_name': 'plugin1'}]
        )
        instance = self._make_instance()
        pr = self._make_lifecycle_processor(
            ctx, graph,
            nodes=[node],
            instances=[instance]
        )
        with current_workflow_ctx.push(ctx):
            pr.install()

        # after creating the install graph, resume it - it should first
        # delete the instance, before re-installing it
        ctx.resume = True
        instance['state'] = 'creating'
        pr._update_resumed_install(graph)

        delete_task_index = None
        install_task_index = None
        for ix, task in enumerate(graph.linearize()):
            if task.name == 'plugin1.op1':
                install_task_index = ix
            elif task.name == 'plugin1.op2':
                delete_task_index = ix

        assert install_task_index is not None
        assert delete_task_index is not None
        assert delete_task_index < install_task_index

    def test_update_resumed_install_dependency(self):
        """Similar to test_update_resumed_install, but with a relationship too
        """
        ctx, graph = self._make_ctx_and_graph()

        node1 = self._make_node(
            operations={
                'cloudify.interfaces.lifecycle.create':
                self._make_operation(operation='plugin1.n1_create'),
                'cloudify.interfaces.lifecycle.delete':
                self._make_operation(operation='plugin1.n1_delete')
            },
            plugins=[self._make_plugin()]
        )
        node2 = self._make_node(
            relationships=[{
                'target_id': 'node1',
                'type_hierarchy': ['cloudify.relationships.depends_on']
            }],
            id='node2',
            operations={
                'cloudify.interfaces.lifecycle.create':
                self._make_operation(operation='plugin1.n2_create'),
                'cloudify.interfaces.lifecycle.delete':
                self._make_operation(operation='plugin1.n2_delete')
            },
            plugins=[self._make_plugin()]
        )
        ni1 = self._make_instance()
        ni2 = self._make_instance(
            relationships=[{
                'target_id': 'node1_1',
                'target_name': 'node1'
            }],
            node_id='node2',
            id='node2_1'
        )
        pr = self._make_lifecycle_processor(
            ctx, graph,
            nodes=[node1, node2],
            instances=[ni1, ni2]
        )
        with current_workflow_ctx.push(ctx):
            pr.install()

        # resume an install, with one node that was interrupted in creating
        ni1['state'] = 'creating'
        ctx.resume = True
        pr._update_resumed_install(graph)

        # to check that the operations happened in the desired order, examine
        # their position in the linearized graph
        task_indexes = {
            'n1_create': None,
            'n1_delete': None,
            'n2_create': None,
            'n2_delete': None,
        }
        for ix, task in enumerate(graph.linearize()):
            name = task.name.replace('plugin1.', '')
            if name in task_indexes:
                task_indexes[name] = ix

        # delete happened before create - reinstall
        assert task_indexes['n1_delete'] < task_indexes['n1_create']
        # dependency installed before the dependent
        assert task_indexes['n1_create'] < task_indexes['n2_create']

        # n2 didnt need to be deleted, because it wasn't in the creating state
        assert task_indexes['n2_delete'] is None
