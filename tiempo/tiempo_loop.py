import calendar
from tiempo import TIEMPO_REGISTRY, all_runners

from hendrix.contrib.async.messaging import hxdispatcher
from twisted.internet import task
from twisted.logger import Logger

from constants import BUSY, IDLE
from tiempo.conn import REDIS, subscribe_to_backend_notifications, hear_from_backend
from tiempo.utils import namespace, utc_now
from tiempo.work import announce_tasks_to_client

logger = Logger()
default_report_handler = None
ps = REDIS.pubsub()


def cycle():
    """An event loop for tiempo"""
    # This loop does five things:

    # Thing 1) Harvest events that have come in from the backend.
    events = glean_events_from_backend()
    # Thing 2) Let the runners pick up any queued tasks.
    let_runners_pick_up_queued_tasks()
    # Thing 3) Queue up new tasks.
    queue_scheduled_tasks(events)
    # Thing 4) Schedule new tasks for enqueing.
    schedule_tasks_for_queueing()
    # Thing 5) Broadcast any new announcements to listeners.
    broadcast_new_announcements_to_listeners(events)

looper = task.LoopingCall(cycle)


def glean_events_from_backend():
    print("glean_events_from_backend called")
    try:
        events = hear_from_backend()
    except AttributeError, e:
        if e.args[0] == "'NoneType' object has no attribute 'can_read'":
            logger.warn("Tried to listen to redis pubsub that wasn't subscribed.")
        events = None
    return events


def let_runners_pick_up_queued_tasks():
    print("let_runners_pick_up_queued_tasks called")
    for runner in all_runners():

        result = runner.cycle()

        if not result in (BUSY, IDLE):
            # If the runner is neither busy nor idle, it will have returned a Deferred.
            # We add our paths for success and failure here.
            result.addCallbacks(runner.handle_success, runner.handle_error)

        runner.announce('runners')  # The runner may have changed state; announce it.


def schedule_tasks_for_queueing():
    print("schedule_tasks_for_queueing called")
    pipe = REDIS.pipeline()  # TODO: Implement distributed locking.
    for task in TIEMPO_REGISTRY.values():
        # TODO: Does this belong in Trabajo?  With pipe as an optional argument?
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


def queue_scheduled_tasks(backend_events):
    print("queue_scheduled_tasks called")
    # TODO: What happens if this is running on the same machine?
    run_now = {}
    for task_string, task in TIEMPO_REGISTRY.items():
        run_now[task_string] = False

        for event in backend_events:

            if event['type'] == 'psubscribe':
                # ignore subscribe events.
                continue

            # If this is a scheduled event and it has now expired....
            if event['pattern'].split(':')[1] == 'expired' and event['data'].split(':')[1] == "scheduled":
                data = event['data'].split(':')
                # ...then it's time to run the corresponding task.
                task_key_that_expired = data[2]
                run_now[task_key_that_expired] = True
                logger.info("Heard expiry %s." % data)

        # We now know which jobs need to be run.  Run them if marked.
        queued_jobs = {}
        for candidate, go_flag in run_now.items():
            if go_flag:
                task = TIEMPO_REGISTRY[candidate]
                queued_jobs[candidate] = task.spawn_job_and_run_soon(default_report_handler=default_report_handler)
            else:
                queued_jobs[candidate] = False


def broadcast_new_announcements_to_listeners(events):
    print("broadcast_new_announcements_to_listeners")
    for event in events:
        if not event['type'] == 'psubscribe':
            key = event['channel'].split(':', 1)[1]
            if key == "expired":
                continue # We aren't handling expired notifications here.
            new_value = REDIS.hgetall(key)
            channel_to_announce = key.split(':', 1)[0]
            if new_value.has_key('jobUid'):
                hxdispatcher.send(channel_to_announce, {channel_to_announce: {new_value['jobUid']: new_value}})
            else:
                hxdispatcher.send(channel_to_announce, {channel_to_announce: new_value})


def start():
    print("tiempo_loop start() called.")
    subscribe_to_backend_notifications()

    logger.info("tiempo_loop start() called.")

    if not looper.running:
        looper.start(1)  # TODO: Customize interval
        task.LoopingCall(announce_tasks_to_client).start(5)
    else:
        logger.warning("Tried to call tiempo_loop start() while the loop is already running.")
