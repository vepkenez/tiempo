from twisted.internet import task
from tiempo import RUNNERS, TIEMPO_REGISTRY
from twisted.logger import Logger
from tiempo.conn import REDIS
from tiempo.utils import utc_now, task_time_keys
import datetime

logger = Logger()


def run():
    this_loop_runtime = utc_now()

    # This loop basically does two things:
    for runner in RUNNERS:
        # 1) Let the runners pick up any queued tasks.
        runner.run()

    # 2) Queue up new tasks.
    for task_string, task in TIEMPO_REGISTRY.items():
        if hasattr(task, 'force_interval'):
            expire_key = this_loop_runtime + datetime.timedelta(
                    seconds=task.force_interval
                )
        else:
            expire_key = task_time_keys().get(task.get_schedule())

        if expire_key:
            stop_key_has_expired = REDIS.setnx(task.stop_key, 0)

            if stop_key_has_expired:

                REDIS.expire(
                    task.stop_key,
                    int(float((expire_key - this_loop_runtime).total_seconds())) - 1
                )

                task.spawn_job()

















looper = task.LoopingCall(run)


def start():
    logger.info("tiempo_loop start() called.")

    if not looper.running:
        looper.start(1) #INTERVAL)
    else:
        logger.warning("Tried to call tiempo_loop start() while the loop is already running.")