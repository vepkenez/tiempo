from unittest.case import TestCase
from tiempo.task import Task
from sample_tasks import some_callable
from tiempo import TIEMPO_REGISTRY


class RegistryTests(TestCase):

    def test_task_decorator_adds_callable_to_registry(self):
        task = Task(priority=1, periodic=True, force_interval=1)
        decorated = task(some_callable)  # Pushes onto registry dict

        self.assertEqual(len(TIEMPO_REGISTRY), 1)

        key = TIEMPO_REGISTRY.keys()[0]

        self.assertEqual(key.split('.')[-1], 'some_callable')