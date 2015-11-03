import calendar
from tiempo import TIEMPO_REGISTRY, all_runners

from hendrix.contrib.async.messaging import hxdispatcher
from twisted.internet import task
from twisted.logger import Logger

from constants import BUSY, IDLE
from tiempo.conn import REDIS, subscribe_to_backend_notifications, hear_from_backend
from tiempo.utils import namespace
from tiempo.work import announce_tasks_to_client

logger = Logger()
default_report_handler = None
ps = REDIS.pubsub()


def cycle():
    # This loop does four things:

    # Thing 1) Let the runners pick up any queued tasks.
    let_runners_pick_up_queued_tasks()
    # Thing 2) Queue up new tasks.
    queue_scheduled_tasks()
    # Thing 3) Schedule new tasks for enqueing.
    schedule_tasks_for_queueing()

    # Thing 4) Broadcast any new announcements to listeners.
    broadcast_new_announcements_to_listeners()

looper = task.LoopingCall(cycle)


def let_runners_pick_up_queued_tasks():
    for runner in all_runners():

        result = runner.cycle()

        if not result in (BUSY, IDLE):
            # If the runner is neither busy nor idle, it will have returned a Deferred.
            # We add our paths for success and failure here.
            result.addCallbacks(runner.handle_success, runner.handle_error)

        runner.announce('runners')  # The runner may have changed state; announce it.


def schedule_tasks_for_queueing():
    pipe = REDIS.pipeline()  # TODO: Implement distrubted locking.
    for task in TIEMPO_REGISTRY.values():
        run_times = task.check_schedule()

        for run_time in run_times:
            # TODO: There's probably a better namespace for this - maybe a UUID to assigned to the job that eventually gets spawned.
            unix_time = calendar.timegm(run_time.timetuple())
            key = namespace('scheduled:%s:%s' % (task.key, unix_time))
            pipe.set(key, 0)
            pipe.expireat(key, unix_time)

        # After loop, set final time.
        pipe.set(namespace('lattermost_run_time:%s' % task.key), run_time.isoformat())

        pipe.execute()

def queue_up_new_tasks():
    for task_string, task in TIEMPO_REGISTRY.items():
        now = utc_now()
        ### REPLACE with task.next_expiration_dt()
        if task.force_interval:
            expire_key = now + datetime.timedelta(
                    seconds=task.force_interval
            )
        else:
            expire_key = task_time_keys().get(task.get_schedule())
        #########

        if expire_key:
            stop_key_has_expired = REDIS.setnx(task.stop_key, 0)

            if stop_key_has_expired:

                seconds_until_expiration = int(float((expire_key - now).total_seconds())) - 1
                REDIS.expire(
                    task.stop_key,
                    seconds_until_expiration
                )

                # OK, we're ready to queue up a new job for this task!
                return task.spawn_job_and_run_soon(default_report_handler=default_report_handler)


def broadcast_new_announcements_to_listeners():
    try:
        events = hear_from_backend()
    except AttributeError, e:
        if e.args[0] == "'NoneType' object has no attribute 'can_read'":
            logger.warn("Tried to listen to redis pubsub that wasn't subscribed.")
            events = None

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


def start():

    subscribe_to_backend_notifications()

    logger.info("tiempo_loop start() called.")

    if not looper.running:
        looper.start(1) #INTERVAL)
        task.LoopingCall(announce_tasks_to_client).start(5)
    else:
        logger.warning("Tried to call tiempo_loop start() while the loop is already running.")