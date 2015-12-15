from twisted.logger import Logger
from tiempo.conn import REDIS

logger = Logger()


class Announcer(object):

    progress_increments = None
    progress = 0

    def __init__(self):
        print("Anouncer initialized")
        super(Announcer, self).__init__()
        self.results_brief = []
        self.results_detail = []

    def set_progress_increments(self, progress_increments):
        print("Announcer.set_progress_increments called")
        self.progress_increments = progress_increments

    def report_progress(self, progress):
        print("Announcer.report_progress called")
        self.progress = progress

    def brief(self, message):
        """Takes a message and appends it to the results_brief dictionary"""
        print("Announcer.brief called")
        self.results_brief.append(message)

    def detail(self, message):
        """Takes a message and appends it to the results_detail dictionary"""
        print("Announcer.detail called")
        self.results_detail.append(message)


def subscribe_to_channel(key):
    print("subscribe_to_channel called")
    ps = REDIS.pubsub()
    ps.psubscribe('__keyspace__@*:%s*' % key)
    return ps.listen()
