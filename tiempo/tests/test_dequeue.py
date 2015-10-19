import random
import string

from twisted.trial.unittest import TestCase

from six.moves.queue import Queue
from tiempo.conn import REDIS
from tiempo.runner import Runner
from tiempo.utils import namespace
from tiempo.work import Trabajo
from tiempo import tiempo_loop


random_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))

q = Queue()

def unblocker():
    q.put(random_string)


class TestTaskInQueue(TestCase):

    def test_running_without_current_job_raises_valueerror(self):
        tm = Runner(1, [('1')])
        self.assertRaises(ValueError, tm.run)  # Run the next task in line, which we hope to be the above.

    def test_task_is_in_queue(self):
        REDIS.flushall()

        decorated = Trabajo()(unblocker)  # Pushes onto registry dict

        tm = Runner(1, [('1')])

        task_data_string = REDIS.lpop(namespace(1))
        self.assertIsNone(task_data_string)  # We haven't loaded the task into REDIS yet.

        decorated.spawn_job()  # Now it's in there.
        d = tm.cycle()  # Run the next task in line, which we hope to be the above.

        def assert_that_unblocker_ran(n):
            unblocked_code = q.get() # The q will have pushed the random string.
            self.assertEqual(unblocked_code, random_string)

        d.addCallback(assert_that_unblocker_ran)

        return d

