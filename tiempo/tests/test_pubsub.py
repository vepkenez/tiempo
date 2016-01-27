from twisted.trial.unittest import TestCase
import time
from twisted.internet import task
from tiempo.conn import REDIS, create_event_queue, check_backend, subscribe_to_backend_notifications

update_queue = create_event_queue()

class EventsBroadCastTests(TestCase):


    def test_subscribing_causes_notifications(self):
        REDIS.flushall()
        REDIS.config_set('notify-keyspace-events', '')
        REDIS.config_get('notify-keyspace-events')
        # Notifications are now turned off.

        # However, subscribing turns them back on.
        subscribe_to_backend_notifications()
        config = REDIS.config_get('notify-keyspace-events')
        self.assertEqual(config['notify-keyspace-events'], 'AKE')

    def test_subscribe_notice(self):
        REDIS.flushall()
        subscribe_to_backend_notifications()
        time.sleep(.1)

        update_queue()
        update_queue()
        update_queue()
        event_queue = update_queue()
        subscribe_event = event_queue.pop()
        self.assertEqual(subscribe_event['type'], 'psubscribe')

    def test_result_is_properly_reported(self):
        REDIS.flushall()
        subscribe_to_backend_notifications()

        REDIS.set('results:whatever', 'a large farva')

        update_queue()
        update_queue()
        update_queue()
        event_queue = update_queue()
        for event in event_queue:
            key = event['channel'].split(':', 1)[1]
            new_value = REDIS.get(key)
            if new_value == 'a large farva':
                self.assertEqual(new_value, 'a large farva')

    def teardown(self):
        REDIS.flushall()
