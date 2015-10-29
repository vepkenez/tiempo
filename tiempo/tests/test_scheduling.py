import json
import uuid
from twisted.trial.unittest import TestCase
from tiempo import TIEMPO_REGISTRY
from sample_tasks import some_callable
from tiempo.conn import REDIS
from tiempo.utils import namespace, utc_now
from tiempo.work import Trabajo, Job


class TaskScheduleTests(TestCase):

    def setUp(self):
        TIEMPO_REGISTRY.clear()
        REDIS.flushall()

    def test_scheduled_tasks_have_proper_schedule(self):
        minutely_decorator = Trabajo(priority=1, periodic=True)
        hourly_decorator = Trabajo(priority=1, periodic=True, minute=1)
        daily_decorator = Trabajo(priority=1, periodic=True, hour=1)
        weekly_decorator = Trabajo(priority=1, periodic=True, day=1)

        minutely = minutely_decorator(some_callable)
        hourly = hourly_decorator(some_callable)
        daily = daily_decorator(some_callable)
        weekly = weekly_decorator(some_callable)

        self.assertEqual(minutely.get_schedule(), '*.*.*')
        self.assertEqual(hourly.get_schedule(), '*.*.01')
        self.assertEqual(daily.get_schedule(), '*.01.*')
        self.assertEqual(weekly.get_schedule(), '01.*.*')

    def test_minutely_tasks_expire_in_60_seconds(self):
        minutely_decorator = Trabajo(priority=1, periodic=True)

        now = utc_now()

        minutely_expiration = minutely.next_expiration_dt()
        minutely_delta = minutely_expiration - now
        self.assertEqual(minutely_delta.seconds, 60)

    def test_now_with_args_runs_with_args(self):
        no_schedule_decorator = Trabajo(priority=1)
        unscheduled_task = no_schedule_decorator(some_callable)
        random_arg = uuid.uuid4()
        result = unscheduled_task.now(random_arg)

        # Our callable simply returns a tuple of args and kwargs passed to it, so we'll have gotten the uuid back.
        args, kwargs = result
        self.assertEqual(args[0], random_arg)

    def test_dependent_task_gets_scheduled_by_soon(self):
        no_schedule_decorator = Trabajo(priority=1)
        this_may_as_well_be_a_uid = "a large farva"

        dependent_task = no_schedule_decorator(some_callable)

        dependent_task.soon(tiempo_wait_for=this_may_as_well_be_a_uid, run_with="something_very_unique_and_strange")
        task_dict_json = REDIS.lpop(namespace(this_may_as_well_be_a_uid))
        task_dict = json.loads(task_dict_json)
        self.assertEqual(task_dict['kwargs_to_function']['run_with'], "something_very_unique_and_strange")

    def test_dependent_task_is_queued_after_trigger_task(self):
        no_schedule_decorator = Trabajo(priority=1)
        unscheduled_task = no_schedule_decorator(some_callable)

        dependent_task = no_schedule_decorator(some_callable)

        # Mark this task as dependent on unscheduled_task.
        dependent_task.soon(tiempo_wait_for=unscheduled_task.uid, litre_cola=True)

        # When we run unscheduled_task, dependent_task will have been queued.
        result = unscheduled_task.now()

        # Now, the dependent task will have been queued.
        task_dict_json = REDIS.lpop(dependent_task.group_key)
        task_dict = json.loads(task_dict_json)

        self.assertTrue(task_dict['kwargs_to_function'].has_key('litre_cola'))

