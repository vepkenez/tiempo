from twisted.trial.unittest import TestCase

from tiempo.conn import REDIS, hear_from_backend, subscribe_to_backend_notifications


class EventsBroadCastTests(TestCase):

    def test_subscribe_notice(self):
        REDIS.flushall()
        subscribe_to_backend_notifications()

        subscribe_event = hear_from_backend()[0]
        self.assertEqual(subscribe_event['type'], 'psubscribe')

    def test_result_is_properly_reported(self):
        REDIS.flushall()
        subscribe_to_backend_notifications()

        REDIS.set('results:whatever', 'a large farva')

        set_event = hear_from_backend()[1]
        key = set_event['channel'].split(':', 1)[1]
        new_value = REDIS.get(key)

        self.assertEqual(new_value, 'a large farva')