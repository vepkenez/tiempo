from Queue import Empty

from twisted.trial.unittest import TestCase
from twisted.internet import threads
from twisted.internet import task

from tiempo.conf import REDIS_QUEUE_DB
from tiempo.conn import REDIS
from six.moves.queue import Queue


class ResultsBroadCastTests(TestCase):

    def test_result_is_properly_reported(self):
        REDIS.flushall()
        q = Queue()
        self.failing = False
        ps = REDIS.pubsub()
        ps.psubscribe('__keyspace@%s__:results*' % REDIS_QUEUE_DB)

        def listen_for_event():
            message = ps.parse_response(block=False)
            if not message:
                return

            event = ps.handle_message(message)
            if event['type'] == 'pmessage' and event['data'] == 'set':
                key = event['channel'].split(':', 1)[1]
                new_value = REDIS.get(key)
                print new_value
                q.put(new_value)

        listen_loop = task.LoopingCall(listen_for_event)
        listen_loop.start(.5)

        def set_value_and_assert():
            try:
                REDIS.set('results:whatever', 'a large farva')
                new_value = q.get(True, 1)
            except Empty:
                self.fail("Key did not have expected value set.")
            else:
                self.assertEqual(new_value, 'a large farva')
            finally:
                listen_loop.stop()

        d1 = threads.deferToThread(set_value_and_assert)

        return d1


