from six.moves.queue import Queue
from twisted.trial.unittest import TestCase
import chalk
from tiempo import TIEMPO_REGISTRY
from tiempo.conn import REDIS
from tiempo.execution import thread_init, ThreadManager
from tiempo.task import Task, resolve_group_namespace
from tiempo.tests.sample_tasks import some_callable
from twisted.internet import reactor
import random, string


random_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))

q = Queue()

def unblocker():
    q.put(random_string)


class TestTaskInQueue(TestCase):

    def test_task_is_in_queue(self):
        REDIS.flushall()

        task = Task()
        decorated = task(unblocker)  # Pushes onto registry dict

        tm = ThreadManager(1, [('1')])

        task_data_string = REDIS.lpop(resolve_group_namespace(1))
        self.assertIsNone(task_data_string)  # We haven't loaded the task into REDIS yet.

        decorated.soon()  # Now it's in there.
        d = tm.control() # Run the next task in line, which we hope to be the above.

        def assert_that_unblocker_ran(n):
            unblocked_code = q.get() # The q will have pushed the random string.
            self.assertEqual(unblocked_code, random_string)

        d.addCallback(assert_that_unblocker_ran)

        return d

