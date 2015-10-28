from twisted.trial.unittest import TestCase
from tiempo import TIEMPO_REGISTRY
from sample_tasks import some_callable
from tiempo.work import Trabajo


class TaskScheduleTests(TestCase):

    def setUp(self):
        TIEMPO_REGISTRY.clear()

    def test_hourly_task(self):
        minutely_decorator = Trabajo(priority=1, periodic=True, minute=1)
        hourly_decorator = Trabajo(priority=1, periodic=True, hour=1)
        daily_decorator = Trabajo(priority=1, periodic=True, day=1)

        minutely = minutely_decorator(some_callable)
        hourly = hourly_decorator(some_callable)
        daily = daily_decorator(some_callable)

        self.assertEqual(minutely.get_schedule(), '*.*.01')
        self.assertEqual(hourly.get_schedule(), '*.01.*')
        self.assertEqual(daily.get_schedule(), '01.*.*')

