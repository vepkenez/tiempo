from twisted.trial.unittest import TestCase
from tiempo import TIEMPO_REGISTRY
from tiempo.conn import REDIS

from redlock.lock import RedLock, RedLockFactory

class LockingTests(TestCase):


    bad_redis_list = [{'host': 'localhost', 'port': 8000}]
    good_redis_list = [{'host': 'localhost', 'port': 6379}]
    factory = RedLockFactory(good_redis_list)
    testing_lock = factory.create_lock("testing_lock")

    def setup(self):
        TIEMPO_REGISTRY.clear()
        REDIS.flushall()

    def test_that_lock_factory_returns_lock(self):
        self.assertIsInstance(self.testing_lock, RedLock)

    def test_that_lock_behaves_as_expected(self):
        acquired_first = self.testing_lock.acquire()
        self.assertTrue(acquired_first)
        acquired_second = self.testing_lock.acquire()
        self.assertFalse(acquired_second)
        self.testing_lock.release()
