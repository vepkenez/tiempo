import redis
from .conf import REDIS_HOST, REDIS_PORT, REDIS_QUEUE_DB, REDIS_PW


REDIS = redis.StrictRedis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_QUEUE_DB,
    password=REDIS_PW
)

NOTIFY_PUBSUB = REDIS.pubsub()


def subscribe_to_backend_notifications(db=REDIS_QUEUE_DB):
    print("subscribe_to_backend_notifications called")
    REDIS.config_set('notify-keyspace-events', 'KEA')
    NOTIFY_PUBSUB.psubscribe('__keyspace@%s__:results*' % db)
    NOTIFY_PUBSUB.psubscribe('__keyevent@%s__:expired' % db)


def hear_from_backend():
    print("hear_from_backend called")
    events = []
    while True:
        message = NOTIFY_PUBSUB.parse_response(block=False)
        if message:
            event = NOTIFY_PUBSUB.handle_message(message)
            events.append(event)
        else:
            return events
