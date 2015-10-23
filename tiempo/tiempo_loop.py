import datetime
from hendrix.contrib.async.messaging import hxdispatcher

from twisted.internet import task
from twisted.logger import Logger

from constants import BUSY, IDLE
from tiempo import TIEMPO_REGISTRY, all_runners
from tiempo.conn import REDIS, subscribe_to_backend_notifications, hear_from_backend
from tiempo.utils import utc_now, task_time_keys
from tiempo.work import announce_tasks_to_client

logger = Logger()
default_report_handler = None
ps = REDIS.pubsub()


def cycle():
    this_loop_runtime = utc_now()

    # This loop basically does two things:

    # Thing 1) Let the runners pick up any queued tasks.
    for runner in all_runners():

        result = runner.cycle()

        if not result in (BUSY, IDLE):
            # If the runner is neither busy nor idle, it will have returned a Deferred.
            # We add our paths for success and failure here.
            result.addCallbacks(runner.handle_success, runner.handle_error)

        runner.announce('runners')  # The runner may have changed state; announce it.

    # Thing 2) Queue up new tasks.
    for task_string, task in TIEMPO_REGISTRY.items():

        ### REPLACE with task.next_expiration_dt()
        if hasattr(task, 'force_interval'):
            expire_key = this_loop_runtime + datetime.timedelta(
                    seconds=task.force_interval
            )
        else:
            expire_key = task_time_keys().get(task.get_schedule())
        #########

        if expire_key:
            stop_key_has_expired = REDIS.setnx(task.stop_key, 0)

            if stop_key_has_expired:

                seconds_until_expiration = int(float((expire_key - this_loop_runtime).total_seconds())) - 1
                REDIS.expire(
                    task.stop_key,
                    seconds_until_expiration
                )

                # OK, we're ready to queue up a new job for this task!
                task.spawn_job(default_report_handler=default_report_handler)

    events = hear_from_backend()
    if events:
        for event in events:
            if not event['type'] == 'psubscribe':
                key = event['channel'].split(':', 1)[1]
                new_value = REDIS.hgetall(key)
                channel_to_announce = key.split(':', 1)[0]
                if new_value.has_key('jobUid'):
                    hxdispatcher.send(channel_to_announce, {channel_to_announce: {new_value['jobUid']: new_value}})
                else:
                    hxdispatcher.send(channel_to_announce, {channel_to_announce: new_value})

looper = task.LoopingCall(cycle)


def start():

    subscribe_to_backend_notifications()

    logger.info("tiempo_loop start() called.")

    if not looper.running:
        looper.start(1) #INTERVAL)
        task.LoopingCall(announce_tasks_to_client).start(5)
    else:
        logger.warning("Tried to call tiempo_loop start() while the loop is already running.")