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
import unittest

from cloudify.workflows import api
from cloudify.workflows import tasks
from cloudify.workflows.tasks_graph import TaskDependencyGraph


def limited_sleep_mock(limit=100):
    s = [None] * limit + [RuntimeError()]
    return mock.patch('time.sleep', side_effect=s)


class TestTasksGraphExecute(unittest.TestCase):
    def test_executes_single_task(self):
        """A single NOP task is executed within a single iteration of the
        tasks graph loop"""
        g = TaskDependencyGraph(None)
        task = tasks.NOPLocalWorkflowTask(None)
        g.add_task(task)
        with limited_sleep_mock(limit=1):
            g.execute()
        self.assertTrue(task.is_terminated)

    def test_executes_multiple_concurrent(self):
        """Independent tasks will be executed concurrently within the same
        iteration of the graph loop.
        """
        g = TaskDependencyGraph(None)
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
        task2 = mock.Mock()

        g = TaskDependencyGraph(None)
        seq = g.sequence()
        seq.add(task1, task2)

        with limited_sleep_mock():
            try:
                g.execute()
            except RuntimeError as e:
                self.assertIn('Workflow failed', e.message)
            else:
                self.fail('Expected task to fail')

        self.assertTrue(task1.is_terminated)
        self.assertFalse(task2.apply_async.called)

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
        g = TaskDependencyGraph(None)
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
        g = TaskDependencyGraph(None)
        task = mock.Mock()
        g.add_task(task)
        with mock.patch('cloudify.workflows.api.cancel_request', True):
            try:
                g.execute()
            except api.ExecutionCancelled:
                pass
            else:
                self.fail('Execution should have been cancelled')
        self.assertFalse(task.apply_async.called)
        self.assertFalse(task.cancel.called)
