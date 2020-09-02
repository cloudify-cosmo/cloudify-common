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

from cloudify.workflows import api
from cloudify.workflows import tasks
from cloudify.workflows.tasks_graph import TaskDependencyGraph


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


class MockWorkflowContext(object):
    wait_after_fail = 600


class TestTasksGraphExecute(testtools.TestCase):
    def test_executes_single_task(self):
        """A single NOP task is executed within a single iteration of the
        tasks graph loop"""
        g = TaskDependencyGraph(MockWorkflowContext())
        task = tasks.NOPLocalWorkflowTask(None)
        g.add_task(task)
        with limited_sleep_mock(limit=1):
            g.execute()
        self.assertTrue(task.is_terminated)

    def test_executes_multiple_concurrent(self):
        """Independent tasks will be executed concurrently within the same
        iteration of the graph loop.
        """
        g = TaskDependencyGraph(MockWorkflowContext())
        task1 = tasks.NOPLocalWorkflowTask(None)
        task2 = tasks.NOPLocalWorkflowTask(None)
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
                self.set_state(tasks.TASK_FAILED)

        task1 = FailedTask(mock.Mock())
        task2 = mock.Mock(execute_after=0)

        g = TaskDependencyGraph(MockWorkflowContext())
        seq = g.sequence()
        seq.add(task1, task2)

        with limited_sleep_mock():
            self.assertRaisesRegex(RuntimeError, 'Workflow failed', g.execute)
        self.assertTrue(task1.is_terminated)
        self.assertFalse(task2.apply_async.called)

    def test_wait_after_fail(self):
        """When a task fails, the already-running tasks are waited for"""
        class FailedTask(tasks.WorkflowTask):
            """Task that fails 1 second after starting"""
            name = 'failtask'

            def get_state(self):
                if time.time() > start_time + 1:
                    return tasks.TASK_FAILED
                else:
                    return tasks.TASK_SENT

        class DelayedTask(tasks.WorkflowTask):
            """Task that succeeds 3 seconds after starting"""
            name = 'delayedtask'

            def get_state(self):
                if time.time() > start_time + 3:
                    return tasks.TASK_SUCCEEDED
                else:
                    return tasks.TASK_SENT
            handle_task_terminated = mock.Mock()

        task1 = FailedTask(mock.Mock())
        task2 = DelayedTask(mock.Mock())

        g = TaskDependencyGraph(MockWorkflowContext())
        seq = g.sequence()
        seq.add(task1, task2)
        with limited_sleep_mock():
            start_time = time.time()
            self.assertRaisesRegex(RuntimeError, 'Workflow failed', g.execute)

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
        task_count = 10

        # prepare the task seuqence
        seq_tasks = []
        for i in range(task_count):
            t = Task(None)
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
        graph = self._restore_graph([])
        operations = list(graph.tasks_iter())
        assert operations == []

    def test_restore_single(self):
        """A single operation is restored into the graph"""
        graph = self._restore_graph([self._remote_task()])
        operations = list(graph.tasks_iter())
        assert len(operations) == 1
        assert isinstance(operations[0], tasks.RemoteWorkflowTask)

    def test_restore_finished(self):
        """Finished tasks are not restored into the graph"""
        task = self._remote_task()
        task['state'] = tasks.TASK_SUCCEEDED
        graph = self._restore_graph([task])
        operations = list(graph.tasks_iter())
        assert operations == []

    def test_restore_with_subgraph(self):
        """Restoring operations keeps subgraph structure"""
        subgraph = self._subgraph()
        task = self._remote_task()
        subgraph['id'] = 15
        task['parameters']['containing_subgraph'] = 15

        graph = self._restore_graph([subgraph, task])
        operations = list(graph.tasks_iter())
        assert len(operations) == 2
        subgraphs = [op for op in operations if op.is_subgraph]
        remote_tasks = [op for op in operations if not op.is_subgraph]

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
        operations = list(graph.tasks_iter())
        assert len(operations) == 2
        assert graph.graph.predecessors(1) == [2]

    def test_restore_with_finished_subgraph(self):
        """Restoring operations keeps subgraph structure"""
        subgraph = self._subgraph()
        task = self._remote_task()
        subgraph['id'] = 15
        task['parameters']['containing_subgraph'] = 15

        subgraph['state'] = tasks.TASK_SUCCEEDED

        graph = self._restore_graph([subgraph, task])
        operations = list(graph.tasks_iter())
        assert len(operations) == 2
        subgraphs = [op for op in operations if op.is_subgraph]
        remote_tasks = [op for op in operations if not op.is_subgraph]

        assert len(subgraphs) == 1
        assert len(remote_tasks) == 1

        assert len(subgraphs[0].tasks) == 1
        assert remote_tasks[0].containing_subgraph is subgraphs[0]
