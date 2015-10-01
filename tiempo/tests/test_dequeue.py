from unittest.case import TestCase
import chalk
from tiempo import TIEMPO_REGISTRY
from tiempo.conn import REDIS
from tiempo.execution import thread_init, ThreadManager
from tiempo.task import Task, resolve_group_namespace
from tiempo.tests.sample_tasks import some_callable
from twisted.internet import reactor


def something():
    chalk.green("Called the task.")


def bad_if_this_is_called():
    chalk.red("Why is this called?!")
    reactor.stop()

something._thaw = bad_if_this_is_called


class TestTaskInQueue(TestCase):

    def test_task_is_in_queue(self):
        REDIS.flushall()
        task = Task(periodic=True, force_interval=1)
        decorated = task(something)  # Pushes onto registry dict

        # We have the task
        # We go through the process of getting wire-data representation of the task (ie, Redis.lpop)
        # We reverse that process
        # We show that we have the same task with which we started.
        # thread_init()

        tr = TIEMPO_REGISTRY
        # tm = ThreadManager(1, [('1')])
        # tm.start()
        # task_data_string = REDIS.lpop(resolve_group_namespace(1))

        thread_init()

        reactor.run()

        self.fail()