from tiempo.conf import REDIS_HOST, REDIS_PORT, REDIS_QUEUE_DB
from redlock import RedLockFactory


connection_details = [{'host': REDIS_HOST, 'port': REDIS_PORT, 'db': REDIS_QUEUE_DB}]

lock_factory = RedLockFactory(connection_details)

schedule_lock = lock_factory.create_lock("schedule_tasks_for_queueing")
