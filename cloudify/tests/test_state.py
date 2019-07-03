
import threading
import unittest


from cloudify._compat import queue
from cloudify.state import ctx, current_ctx

from cloudify.mocks import MockCloudifyContext


class TestCurrentContextAndCtxLocalProxy(unittest.TestCase):

    def test_basic(self):
        self.assertRaises(RuntimeError, current_ctx.get_ctx)
        self.assertRaises(RuntimeError, lambda: ctx.instance.id)
        value = MockCloudifyContext(node_id='1')
        current_ctx.set(value)
        self.assertEqual(value, current_ctx.get_ctx())
        self.assertEqual(value.instance.id, ctx.instance.id)
        current_ctx.clear()
        self.assertRaises(RuntimeError, current_ctx.get_ctx)
        self.assertRaises(RuntimeError, lambda: ctx.instance.id)

    def test_threads(self):
        num_iterations = 1000
        num_threads = 10
        for _ in range(num_iterations):
            queues = [queue.Queue() for _ in range(num_threads)]

            def run(response_queue, value):
                try:
                    self.assertRaises(RuntimeError, current_ctx.get_ctx)
                    self.assertRaises(RuntimeError, lambda: ctx.instance.id)
                    current_ctx.set(value)
                    self.assertEqual(value, current_ctx.get_ctx())
                    self.assertEqual(value.instance.id, ctx.instance.id)
                    current_ctx.clear()
                    self.assertRaises(RuntimeError, current_ctx.get_ctx)
                    self.assertRaises(RuntimeError, lambda: ctx.instance.id)
                except Exception as e:
                    response_queue.put(e)
                else:
                    response_queue.put('ok')

            threads = []
            for index, thread_queue in enumerate(queues):
                value = MockCloudifyContext(node_id=str(index))
                threads.append(threading.Thread(target=run,
                                                args=(thread_queue, value)))

            for thread in threads:
                thread.start()

            for thread_queue in queues:
                self.assertEqual('ok', thread_queue.get())
