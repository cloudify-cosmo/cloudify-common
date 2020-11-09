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

from testtools import TestCase

from cloudify import exceptions
from cloudify.workflows import tasks, tasks_graph
from cloudify_rest_client.operations import Operation, TasksGraph


class _MockCtx(object):
    def __init__(self, storage):
        self._storage = storage
        self.execution_token = 'mock_token'

    def _get_current_object(self):
        return self

    def store_tasks_graph(self, name, operations):
        self._storage['name'] = name
        self._storage['operations'] = [Operation(op) for op in operations]
        return {'id': 'abc'}

    def get_operations(self, graph_id):
        return self._storage['operations']


def _make_remote_task(kwargs=None):
    kwargs = kwargs or {'a': 1}
    kwargs['__cloudify_context'] = {'task_name': 'x'}
    return tasks.RemoteWorkflowTask(
        kwargs=kwargs,
        cloudify_context=kwargs['__cloudify_context'],
        workflow_context=None,
        info={'info': 'info'}
    )


def _on_success_func(tsk):
    pass


class _OnFailureHandler(object):
    def __init__(self, value):
        self.value = value

    def dump(self):
        return {
            'value': self.value
        }

    def __call__(self):
        return self.value


class TestSerialize(TestCase):

    def test_task_serialize(self):
        task = _make_remote_task()
        task._state = tasks.TASK_SENT
        serialized = Operation(task.dump())
        deserialized = tasks.RemoteWorkflowTask.restore(
            ctx=_MockCtx({}),
            graph=None,
            task_descr=serialized)

        for attr_name in ['id', 'info', 'total_retries', 'retry_interval',
                          '_state', 'current_retries']:
            self.assertEqual(getattr(task, attr_name),
                             getattr(deserialized, attr_name))
        task_ctx = task._cloudify_context
        deserialized_task_ctx = deserialized._cloudify_context
        # when deserializing, execution_token is added from the workflow ctx
        self.assertEqual(deserialized_task_ctx, {
            'task_name': task_ctx['task_name'],
            'execution_token': 'mock_token'
        })

    def test_marks_as_stored(self):
        task = _make_remote_task()
        self.assertFalse(task.stored)
        task.dump()
        self.assertTrue(task.stored)

    def test_handler_serialize_func(self):
        task = _make_remote_task()
        task.on_success = _on_success_func
        serialized = Operation(task.dump())
        deserialized = tasks.RemoteWorkflowTask.restore(
            ctx=_MockCtx({}),
            graph=None,
            task_descr=serialized)
        self.assertIs(task.on_success, deserialized.on_success)

    def test_handler_serialize_class(self):
        task = _make_remote_task()
        task.on_success = _OnFailureHandler(42)
        serialized = Operation(task.dump())
        deserialized = tasks.RemoteWorkflowTask.restore(
            ctx=_MockCtx({}),
            graph=None,
            task_descr=serialized)
        self.assertEqual(deserialized.on_success(), 42)

    def test_handler_serialize_error(self):
        task = _make_remote_task()
        task.on_success = lambda tsk: None
        self.assertRaises(
            exceptions.NonRecoverableError,
            task.dump
        )


class TestGraphSerialize(TestCase):
    def test_graph_serialize(self):
        _stored = {}
        task = _make_remote_task({'task': 1})
        graph = tasks_graph.TaskDependencyGraph(_MockCtx(_stored))
        graph.add_task(task)

        self.assertIs(graph.id, None)
        self.assertFalse(graph._stored)

        graph.store(name='graph1')

        self.assertIsNot(graph.id, None)
        self.assertTrue(graph._stored)

        self.assertEqual(_stored['name'], 'graph1')
        self.assertEqual(len(_stored['operations']), 1)

    def test_graph_dependencies(self):
        _stored = {}
        ctx = _MockCtx(_stored)
        task1 = _make_remote_task({'task': 1})
        task2 = _make_remote_task({'task': 2})
        graph = tasks_graph.TaskDependencyGraph(ctx)
        subgraph = graph.subgraph('sub1')
        subgraph.add_task(task1)
        subgraph.add_task(task2)
        graph.add_dependency(task1, task2)
        graph.store(name='graph1')

        deserialized = tasks_graph.TaskDependencyGraph.restore(
            ctx, TasksGraph({'id': graph.id}))

        self.assertEqual(graph.id, deserialized.id)

        deserialized_task1 = deserialized.get_task(task1.id)
        deserialized_task2 = deserialized.get_task(task2.id)

        deserialized_subgraph = deserialized.get_task(subgraph.id)
        self.assertEqual(deserialized_task1.containing_subgraph.id,
                         deserialized_subgraph.id)
        self.assertEqual(deserialized_task2.containing_subgraph.id,
                         deserialized_subgraph.id)

        # this checks dependencies
        self.assertEqual(deserialized._dependencies, graph._dependencies)
