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


import time
import threading
from collections import defaultdict
from functools import wraps

from cloudify.utils import get_func
from cloudify.exceptions import WorkflowFailed
from cloudify.workflows import api
from cloudify.workflows import tasks
from cloudify.state import workflow_ctx


def make_or_get_graph(f):
    """Decorate a graph-creating function with this, to automatically
    make it try to retrieve the graph from storage first.
    """
    @wraps(f)
    def _inner(*args, **kwargs):
        if workflow_ctx.dry_run:
            kwargs.pop('name', None)
            return f(*args, **kwargs)
        name = kwargs.pop('name')
        graph = workflow_ctx.get_tasks_graph(name)
        if not graph:
            graph = f(*args, **kwargs)
            graph.store(name=name)
        else:
            graph = TaskDependencyGraph.restore(workflow_ctx, graph)
        return graph
    return _inner


class TaskDependencyGraphError(object):
    def __init__(self, task_name, traceback, error_causes, error_time=None):
        self.task_name = task_name
        self.traceback = traceback
        self.error_causes = error_causes
        self.error_time = error_time or time.time()


def _task_error_causes_short(task):
    """Examine the task, and return a summary of its errors.

    If the task didn't error out, return the empty string.
    """
    if not hasattr(task, 'error') or not isinstance(task.error, dict):
        return ''
    error_parts = []
    causes = task.error.get('known_exception_type_kwargs', {}).get('causes')
    for c in causes:
        error_parts.append('{0} `{1}`'.format(c['type'], c['message']))
    if not causes:
        # no known causes - just show the exception directly
        error_parts.append(
            '{0} `{1}`'.format(
                task.error['exception_type'],
                task.error['message']
            )
        )
    return '\n'.join(error_parts)


def _task_error_causes_traceback(task):
    """Examine the task, and return tracebacks of its errors.

    The tracebacks are concatenated into a single string.
    If the task didn't error out, return the empty string.
    """
    if not hasattr(task, 'error') or not isinstance(task.error, dict):
        return ''
    error_parts = []
    for c in task.error.get('known_exception_type_kwargs', {}).get('causes'):
        traceback = c.get('traceback')
        if traceback:
            # this is a bit dirty - remove the first line of the traceback,
            # which says "Traceback (most recent call last):". This is so
            # that we can add our own line instead later, which also
            # says the name of the erroring out task
            header, _, traceback = traceback.strip().partition('\n')
            error_parts.append(traceback)
    return '\n'.join(error_parts)


class TaskDependencyGraphErrors(object):
    def __init__(self):
        self._errors = []

    def __len__(self):
        return len(self._errors)

    def __bool__(self):
        return len(self._errors) > 0

    def add_error(self, result, task):
        summary = "Task failed: {0}".format(task.short_description)
        short_causes_text = _task_error_causes_short(task)
        if short_causes_text:
            summary = '{0}: {1}'.format(summary, short_causes_text)
        self._errors.append(TaskDependencyGraphError(
            task_name=task.short_description,
            error_causes=short_causes_text,
            traceback=_task_error_causes_traceback(task),
        ))

    def first_error_time(self):
        if not self._errors:
            return None
        return self._errors[0].error_time

    def format_exception(self):
        """Turn errors stored here into a single human-readable WorkflowFailed

        This formats the actual message the user will see. Show information
        about all errors that happened, and a traceback.
        """
        if not self._errors:
            return None
        if len(self._errors) > 1:
            message = '{0} operation errors:\n{1}'.format(
                len(self._errors),
                '\n'.join(
                    '{0}: {1}'.format(err.task_name, err.error_causes)
                    for err in self._errors
                ),
            )
        else:
            message = 'Task failed: {0}: {1}'.format(
                self._errors[0].task_name,
                self._errors[0].error_causes,
            )
        if self._errors[0].traceback:
            message = (
                '{0}\nTraceback of {1} (most recent call last):\n{2}'
                .format(
                    message,
                    self._errors[0].task_name,
                    self._errors[0].traceback,
                )
            )
        return WorkflowFailed(
            message,
            # a task failed, not the workflow function itself: no need to
            # show the traceback of the workflow function
            hide_traceback=True,
        )


class TaskDependencyGraph(object):
    """A task graph.

    :param workflow_context: A WorkflowContext instance (used for logging)
    """

    @classmethod
    def restore(cls, workflow_context, retrieved_graph):
        graph = cls(workflow_context, graph_id=retrieved_graph.id)
        ops = workflow_context.get_operations(retrieved_graph.id)
        graph._restore_operations(ops)
        graph._restore_dependencies(ops)
        graph._stored = True
        return graph

    def __init__(self, workflow_context, graph_id=None,
                 default_subgraph_task_config=None):
        self.ctx = workflow_context
        default_subgraph_task_config = default_subgraph_task_config or {}
        self._default_subgraph_task_config = default_subgraph_task_config
        self._wake_after_fail = None
        self._stored = False
        self.id = graph_id
        self._tasks = {}
        self._dependencies = defaultdict(set)
        self._dependents = defaultdict(set)
        self._ready = set()
        self._waiting_for = set()
        self._tasks_wait = threading.Event()
        self._finished_tasks = {}
        self._op_types_cache = {}
        self._errors = None

    def optimize(self):
        """Optimize this tasks graph, removing tasks that do nothing.

        Empty subgraphs, and NOP tasks, are dropped. A subgraph is considered
        empty if it only contains NOP tasks, and empty subgraphs.
        """
        removable = [
            task for task in self._tasks.values()
            if task.is_nop()
        ]
        for task in removable:
            dependents = self._dependents[task]
            dependencies = self._dependencies[task]
            if task.containing_subgraph:
                task.containing_subgraph.tasks.pop(task.id)
            self.remove_task(task)
            for dependent in dependents:
                for dependency in dependencies:
                    self.add_dependency(dependent, dependency)

    def linearize(self):
        """Traverse the graph, and return tasks in dependency order.

        This makes sure that if task A depends on task B, then A is
        going to be after B in the resulting list. Ordering of tasks
        that are not related by dependencies is undefined.

        This is useful for logging, debugging, and testing.
        """
        dependencies_copy = {k: set(v) for k, v in self._dependencies.items()}
        ordered = []
        ready = [
            task for task in self._tasks.values()
            if not dependencies_copy.get(task)
        ]
        while ready:
            new_ready = []
            for task in ready:
                ordered.append(task)
                for dependent_task in self._dependents[task]:
                    dependencies_copy[dependent_task].discard(task)
                    if not dependencies_copy.get(dependent_task):
                        new_ready.append(dependent_task)
            ready = new_ready
        return ordered

    def _restore_dependencies(self, ops):
        """Set dependencies between this graph's tasks according to ops.

        :param ops: a list of rest-client Operation objects
        """
        # a mapping of operations which retry previously-failed operations
        retries_dict = dict(
            (x.parameters['retried_task'], x.id) for x in ops if
            x.parameters.get('retried_task'))

        for op_descr in ops:
            op = self.get_task(op_descr.id)
            if op is None:
                continue
            for target_id in op_descr.dependencies:
                target = self.get_task(target_id)
                if target is not None:
                    self.add_dependency(op, target)
                else:
                    new_target = self._retrieve_active_target(target_id,
                                                              retries_dict)
                    if new_target is not None:
                        self.add_dependency(op, new_target)

    def _retrieve_active_target(self, target_id, retries_dict):
        # traverse the retried task chain to find the active task which
        # corresponds to the defunct target
        next_target = target_id
        while next_target:
            last_target = next_target
            next_target = retries_dict.get(last_target)
        return self.get_task(last_target)

    def _restore_operations(self, ops):
        """Restore operations from ops into this graph.

        :param ops: a list of rest-client Operation objects
        """
        ops_by_id = dict((op.id, op) for op in ops)
        restored_ops = {}
        for op_descr in ops:
            if op_descr.id in restored_ops:  # already restored - a subgraph
                continue
            if op_descr.state in tasks.TERMINATED_STATES:
                continue

            op = self._restore_operation(op_descr)
            restored_ops[op_descr.id] = op

            # restore the subgraph - even if the subgraph was already finished,
            # we are going to be running an operation from it, so mark it as
            # pending again.
            # Follow the subgraph hierarchy up.
            while op_descr.containing_subgraph:
                subgraph_id = op_descr.containing_subgraph
                subgraph_descr = ops_by_id[subgraph_id]
                subgraph_descr['state'] = tasks.TASK_STARTED
                subgraph = self._restore_operation(subgraph_descr)
                self.add_task(subgraph)
                restored_ops[subgraph_id] = subgraph

                op.containing_subgraph = subgraph
                subgraph.add_task(op)

                op, op_descr = subgraph, subgraph_descr

            self.add_task(op)

    def _restore_operation(self, op_descr):
        """Create a Task object from a rest-client Operation object.

        If the task was already restored before, return a reference to the
        same object.
        """
        restored = self.get_task(op_descr.id)
        if restored is not None:
            return restored
        op_cls = self._get_operation_class(op_descr.type)
        return op_cls.restore(
            self.ctx._get_current_object(), self, op_descr)

    def _get_operation_class(self, task_type):
        if task_type in self._op_types_cache:
            return self._op_types_cache[task_type]
        if task_type == 'SubgraphTask':
            op_cls = SubgraphTask
        elif '.' in task_type:
            op_cls = get_func(task_type)
        else:
            op_cls = getattr(tasks, task_type)

        if not issubclass(op_cls, tasks.WorkflowTask):
            raise RuntimeError('{0} is not a subclass of WorkflowTask'
                               .format(task_type))
        self._op_types_cache[task_type] = op_cls
        return op_cls

    def store(self, name, optimize=True):
        if optimize:
            self.optimize()
        serialized_tasks = []
        for task in self._tasks.values():
            serialized = task.dump()
            serialized['dependencies'] = [
                dep.id for dep in self._dependencies.get(task, [])]
            serialized_tasks.append(serialized)
        stored_graph = self.ctx.store_tasks_graph(
            name, operations=serialized_tasks)
        if stored_graph:
            self.id = stored_graph['id']
            self._stored = True
            for task in self._tasks.values():
                task.stored = True

    @property
    def tasks(self):
        return list(self._tasks.values())

    def add_task(self, task):
        """Add a WorkflowTask to this graph

        :param task: The task
        """
        self._tasks[task.id] = task
        self._ready.add(task)

    def get_task(self, task_id):
        """Get a task instance that was inserted to this graph by its id

        :param task_id: the task id
        :return: a WorkflowTask instance for the requested task if found.
                 None, otherwise.
        """
        return self._tasks.get(task_id)

    def remove_task(self, task):
        """Remove the provided task from the graph

        :param task: The task
        """
        if task.is_subgraph:
            for subgraph_task in task.tasks.values():
                self.remove_task(subgraph_task)
        if task.id in self._tasks:
            del self._tasks[task.id]
            self._ready.discard(task)
            for dependent in self._dependents.pop(task, []):
                self._dependencies[dependent].discard(task)
                if not self._dependencies.get(dependent):
                    self._ready.add(dependent)
            for dependency in self._dependencies.pop(task, []):
                self._dependents[dependency].discard(task)

    def add_dependency(self, src_task, dst_task):
        """Add a dependency between tasks: src depends on dst.

        The source task will only be executed after the target task terminates.
        A task may depend on several tasks, in which case it will only be
        executed after all its 'destination' tasks terminate

        :param src_task: The source task
        :param dst_task: The target task
        """
        if src_task == dst_task:
            return
        if src_task.id not in self._tasks:
            raise RuntimeError('src not in graph: {0!r}'.format(src_task))
        if dst_task.id not in self._tasks:
            raise RuntimeError('dst not in graph: {0!r}'.format(dst_task))
        self._dependencies[src_task].add(dst_task)
        self._dependents[dst_task].add(src_task)
        self._ready.discard(src_task)

    def remove_dependency(self, src_task, dst_task):
        if src_task.id not in self._tasks:
            raise RuntimeError('src not in graph: {0!r}'.format(src_task))
        if dst_task.id not in self._tasks:
            raise RuntimeError('dst not in graph: {0!r}'.format(dst_task))
        self._dependencies[src_task].discard(dst_task)
        self._dependents[dst_task].discard(src_task)
        if not self._dependencies[src_task]:
            self._ready.add(src_task)
            self._tasks_wait.set()

    def sequence(self):
        """
        :return: a new TaskSequence for this graph
        """
        return TaskSequence(self)

    def subgraph(self, name):
        task = SubgraphTask(self, info={'name': name},
                            **self._default_subgraph_task_config)
        self.add_task(task)
        return task

    def execute(self):
        """Execute tasks in this graph.

        Run ready tasks, register callbacks on their result, process
        results from tasks that did finish.
        Tasks whose dependencies finished, are marked as ready for
        the next iteration.

        This main loop is directed by the _tasks_wait event, which
        is set only when there is something to be done: a task response
        has been received, some tasks dependencies finished which makes
        new tasks ready to be run, or the execution was cancelled.

        If a task failed, wait for ctx.wait_after_fail for additional
        responses to come in anyway.
        """
        self._errors = TaskDependencyGraphErrors()
        api.cancel_callbacks.add(self._tasks_wait.set)

        while not self._is_finished():
            self._tasks_wait.clear()

            while self._ready:
                task = self._ready.pop()
                if task.dependency_error:
                    # this task was "ready" because all of its dependencies
                    # finished (at least one successfully), but one of the
                    # dependencies finished with an error. It must not run.
                    continue
                self._run_task(task)

            self._tasks_wait.wait(1)

            while self._finished_tasks:
                task, result = self._finished_tasks.popitem()
                self._handle_terminated_task(result, task)

        api.cancel_callbacks.discard(self._tasks_wait.set)
        if self._wake_after_fail:
            self._wake_after_fail.cancel()
        if self._errors:
            raise self._errors.format_exception()
        if api.has_cancel_request():
            raise api.ExecutionCancelled()

    def _is_finished(self):
        if api.has_cancel_request():
            return True

        if not self._tasks:
            return True

        if self._errors:
            if not self._waiting_for and not self._ready:
                return True
            deadline = \
                self._errors.first_error_time() + self.ctx.wait_after_fail
            if time.time() > deadline:
                return True
            else:
                self._wake_after_fail = threading.Timer(
                    deadline - time.time(),
                    self._tasks_wait.set)
                self._wake_after_fail.daemon = True
                self._wake_after_fail.start()
        return False

    def _run_task(self, task):
        result = task.apply_async()
        self._waiting_for.add(task)
        result.on_result(self._task_finished, task)

    def _task_finished(self, result, task):
        self._finished_tasks[task] = result
        self._tasks_wait.set()

    def _handle_terminated_task(self, result, task):
        self._waiting_for.discard(task)
        handler_result = task.handle_task_terminated()
        dependents = self._dependents[task]
        dependencies = self._dependencies[task]
        self.remove_task(task)
        if handler_result.action == tasks.HandlerResult.HANDLER_FAIL:
            if isinstance(task, SubgraphTask) and task.failed_task:
                task = task.failed_task
            result = self._task_error(result, task)
            self._ready -= dependents
            # mark all the dependent tasks as having a dependency error, so
            # that they will not run, even if all their other dependencies
            # finish successfully
            for d in dependents:
                d.dependency_error = True
        elif handler_result.action == tasks.HandlerResult.HANDLER_RETRY:
            new_task = handler_result.retried_task
            if self.id is not None:
                stored = self.ctx.store_operation(
                    new_task,
                    [dep.id for dep in self._dependencies[task]],
                    self.id)
                if stored:
                    new_task.stored = True
            self.add_task(new_task)
            for dependency in dependencies:
                self.add_dependency(new_task, self.get_task(dependency))
            for dependent in dependents:
                self.add_dependency(self.get_task(dependent), new_task)

        self._tasks_wait.set()

    def _task_error(self, result, task):
        self._errors.add_error(result, task)
        self._waiting_for = {
            t for t in self._waiting_for if not t.is_subgraph
        }
        return result


class forkjoin(object):
    """
    A simple wrapper for tasks. Used in conjunction with TaskSequence.
    Defined to make the code easier to read (instead of passing a list)
    see ``TaskSequence.add`` for more details
    """

    def __init__(self, *tasks):
        self.tasks = tasks


class TaskSequence(object):
    """
    Helper class to add tasks in a sequential manner to a task dependency
    graph

    :param graph: The TaskDependencyGraph instance
    """

    def __init__(self, graph):
        self.graph = graph
        self.last_fork_join_tasks = None

    def add(self, *tasks):
        """Add tasks to the sequence.

        :param tasks: Each task might be:
            * A WorkflowTask instance, in which case, it will be
              added to the graph with a dependency between it and
              the task previously inserted into the sequence
            * A forkjoin of tasks, in which case it will be treated
              as a "fork-join" task in the sequence, i.e. all the
              fork-join tasks will depend on the last task in the
              sequence (could be fork join) and the next added task
              will depend on all tasks in this fork-join task
        """
        for fork_join_tasks in tasks:
            if isinstance(fork_join_tasks, forkjoin):
                fork_join_tasks = fork_join_tasks.tasks
            else:
                fork_join_tasks = [fork_join_tasks]
            for task in fork_join_tasks:
                self.graph.add_task(task)
                if self.last_fork_join_tasks is not None:
                    for last_fork_join_task in self.last_fork_join_tasks:
                        self.graph.add_dependency(task, last_fork_join_task)
            if fork_join_tasks:
                self.last_fork_join_tasks = fork_join_tasks


class SubgraphTask(tasks.WorkflowTask):

    def __init__(self,
                 graph,
                 workflow_context=None,
                 task_id=None,
                 total_retries=tasks.DEFAULT_SUBGRAPH_TOTAL_RETRIES,
                 **kwargs):
        kwargs.setdefault('info', {})
        super(SubgraphTask, self).__init__(
            graph.ctx,
            task_id,
            total_retries=total_retries,
            **kwargs)
        self.graph = graph
        self.tasks = {}
        self.failed_task = None
        if not self.on_failure:
            self.on_failure = _on_failure_handler_fail

    @classmethod
    def restore(cls, ctx, graph, task_descr):
        task_descr.parameters['task_kwargs']['graph'] = graph
        return super(SubgraphTask, cls).restore(ctx, graph, task_descr)

    def _duplicate(self):
        raise NotImplementedError('self.retried_task should be set explicitly'
                                  ' in self.on_failure handler')

    @property
    def cloudify_context(self):
        return {}

    def is_local(self):
        return True

    @property
    def name(self):
        return self.info.get('name') or self.id

    @property
    def is_subgraph(self):
        return True

    def is_nop(self):
        return not self.tasks or all(
            t.is_nop() for t in self.tasks.values()
        )

    def sequence(self):
        return TaskSequence(self)

    def subgraph(self, name):
        task = SubgraphTask(self.graph, info={'name': name},
                            **self.graph._default_subgraph_task_config)
        self.add_task(task)
        return task

    def add_task(self, task):
        self.graph.add_task(task)
        self.add_dependency(task, self)
        self.tasks[task.id] = task
        if task.containing_subgraph and task.containing_subgraph is not self:
            raise RuntimeError('task {0}[{1}] cannot be contained in more '
                               'than one subgraph. It is currently contained '
                               'in {2} and it is now being added to {3}'
                               .format(task,
                                       task.id,
                                       task.containing_subgraph.name,
                                       self.name))
        task.containing_subgraph = self

    def remove_task(self, task):
        self.graph.remove_task(task)

    def add_dependency(self, src_task, dst_task):
        self.graph.add_dependency(src_task, dst_task)

    def apply_async(self):
        super(SubgraphTask, self).apply_async()
        if not self.tasks:
            self.set_state(tasks.TASK_SUCCEEDED)
        else:
            # subgraph started - allow its tasks to run - remove their
            # dependency on the subgraph, so they don't wait on the
            # subgraph anymore
            for task_id, task in self.tasks.items():
                self.graph.remove_dependency(task, self)
            self.set_state(tasks.TASK_STARTED)
        return self.async_result

    def task_terminated(self, task, new_task=None):
        del self.tasks[task.id]
        if new_task:
            self.tasks[new_task.id] = new_task
            new_task.containing_subgraph = self
        if self.get_state() not in tasks.TERMINATED_STATES:
            if self.failed_task:
                self.set_state(tasks.TASK_FAILED)
            elif not self.tasks:
                self.set_state(tasks.TASK_SUCCEEDED)

    def set_state(self, state):
        super(SubgraphTask, self).set_state(state)
        if state in tasks.TERMINATED_STATES:
            self.async_result.result = None

    def __repr__(self):
        return '<{0} {1}: {2}>'.format(self.task_type, self.id, self.name)


def _on_failure_handler_fail(task):
    return tasks.HandlerResult.fail()
