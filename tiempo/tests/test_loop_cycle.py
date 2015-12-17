from tiempo import tiempo_loop, TIEMPO_REGISTRY
import string
import random
import time
import datetime

from twisted.trial.unittest import TestCase

from six.moves.queue import Queue
from tiempo.conn import subscribe_to_backend_notifications, REDIS
from tiempo.tests.sample_tasks import some_callable
from tiempo.tiempo_loop import schedule_tasks_for_queueing, glean_events_from_backend
from tiempo.work import Trabajo
from tiempo.utils import utc_now

q = Queue()
random_string = ''.join(random.choice(string.ascii_uppercase + string.digits) for _ in range(10))


def unblocker():
    q.put(random_string)


# class WholeCycleTests(TestCase):
#
#     def test_whole_cycle(self):
#         tiempo_loop.cycle()
#         self.fail()

class ScheduleExpiryTests(TestCase):
    '''
    When scheduled items expire from redis, it's time to run them.
    '''

    def setUp(self):
        TIEMPO_REGISTRY.clear()
        REDIS.flushall()

    def test_that_expiry_triggers_scheduling(self):
        subscribe_to_backend_notifications()
        decorated = Trabajo(force_interval=1)(some_callable) # Puts the task in TIEMPO_REGISTRY
        schedule_tasks_for_queueing()
        # TODO: Is there a better way to wait for REDIS expiry notifications?
        subscribe_events = glean_events_from_backend()
        time.sleep(2)
        other_events = glean_events_from_backend()
        tiempo_loop.queue_scheduled_tasks(other_events)

    def test_consecutive_scheduling_force_interval(self):

        # create a task with a forced interval of 1 second.
        forced_interval = Trabajo(force_interval=1)(some_callable)

        queued = forced_interval.currently_scheduled_keys()
        self.assertEqual(len(queued), 0)

        schedule_tasks_for_queueing()
        queued = forced_interval.currently_scheduled_keys()
        first_key = queued[0]
        last_key = queued[-1]

        self.assertEqual(len(queued), 100)

        # wait 2 seconds
        time.sleep(2)

        # schedule again
        schedule_tasks_for_queueing()
        queued = forced_interval.currently_scheduled_keys()

        # there should be no new tasks scheduled
        self.assertEqual(len(queued), 100)

        # the keys should still be the same
        self.assertEqual(queued[0], first_key)
        self.assertEqual(queued[-1], last_key)


    def test_consecutive_scheduling_periodic(self):
        window_begin = utc_now()
        window_end = window_begin + datetime.timedelta(hours=3, minutes=20)

        # this task should run once per hour so if we have a window of 3 hours,
        # it should get scheduled to run 3x.

        periodic = Trabajo(
            periodic=True,
            minute=window_begin.minute+3
        )(some_callable)

        queued = periodic.currently_scheduled_keys()
        self.assertEqual(len(queued), 0)

        schedule_tasks_for_queueing()
        queued = periodic.currently_scheduled_keys()
        first_key = queued[0]
        last_key = queued[-1]

        self.assertEqual(len(periodic.currently_scheduled_keys()), 3)

        # wait 2 seconds
        time.sleep(2)

        # schedule again
        schedule_tasks_for_queueing()
        # there should be no new tasks scheduled
        self.assertEqual(len(periodic.currently_scheduled_keys()), 3)

        # the keys should still be the same
        self.assertEqual(queued[0], first_key)
        self.assertEqual(queued[-1], last_key)
