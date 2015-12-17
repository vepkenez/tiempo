from twisted.trial.unittest import TestCase
from tiempo import TIEMPO_REGISTRY
from tiempo.conn import REDIS
from tiempo.work import Trabajo, Job, report_handler
from tiempo.tests.sample_tasks import some_callable


class JobReportingTests(TestCase):
    """
    Tests for Job instances and report_handlers
    """

    decorated = Trabajo()(some_callable)
    simple_job = decorated.just_spawn_job()

    def setup(self):
        TIEMPO_REGISTRY.clear()
        REDIS.flushall()

    def test_just_spawn_job_is_Job(self):
        self.assertIsInstance(self.simple_job, Job)
