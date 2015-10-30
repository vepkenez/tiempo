from tiempo import tiempo_loop
import string
import random

from twisted.trial.unittest import TestCase

from six.moves.queue import Queue
from tiempo.work import Trabajo


q = Queue()
random_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))


def unblocker():
    q.put(random_string)


class TaskRunning(TestCase):

    def test_force_interval_gets_job_spawned_on_first_cycle(self):
        decorated = Trabajo(force_interval=3)(unblocker)
        job = tiempo_loop.queue_up_new_tasks()
        self.assertIs(decorated, job.task)