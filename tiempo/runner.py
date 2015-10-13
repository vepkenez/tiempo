import datetime

from hendrix.contrib.async.messaging import hxdispatcher
from twisted.internet import threads
from constants import BUSY, IDLE

from tiempo.conn import REDIS
from tiempo.utils import utc_now, namespace
from tiempo.work import Job
from tiempo import RUNNERS
from twisted.logger import Logger
logger = Logger()
import traceback


class Runner(object):
    '''
    Runs Jobs.

    During tiempo_loop, each runner is given a chance to run a job in the queue.
    '''

    def __init__(self, number, thread_group_list):
        logger.info("Starting Thread manager %s with threads %s (%s)" % (number, thread_group_list, id(self)))
        for i in thread_group_list:
            if RUNNERS.has_key(i):
                RUNNERS[i].append(self)
            else:
                RUNNERS[i] = [self]

        self.action_time = utc_now()
        self.current_job = None
        self.task_groups = thread_group_list
        self.number = number

    def __repr__(self):
        return 'Tiempo Runner %d' % self.number

    def cleanup(self):
        pass

    def run(self):

        # If we have a current Job, return BUSY and go no further.
        if self.current_job:
            logger.debug("Worker %s is busy with %s (%s / %s)" % (
                self.number,
                self.current_job.code_word,
                self.current_job.task.key,
                self.current_job.uid)
            )
            return BUSY

        # ...otherwise, look for a Job to run...
        job_string = self.seek_job()

        # ...and run it.
        if job_string:
            self.action_time = utc_now()
            logger.debug('%s adopting task: %s' % (self, job_string))
            d = threads.deferToThread(run_task, job_string, self)
            return d
        else:
            return IDLE

    def seek_job(self):
        for g in self.task_groups:

            logger.debug('%r checking for a Job in group %r' % (self, g))
            group_key = namespace(g)
            job_string = REDIS.lpop(group_key)
            if job_string:
                logger.info('%s found a Job in group %s: %s' % (self, g, job_string))
                return job_string

    def finish_job(self, result):
        self.action_time = utc_now()
        self.current_job.finish()
        self.current_job = None

    def handle_error(self, failure):
            failure.value

    def serialize_to_dict(self):
        if self.current_job:
            code_word = self.current_job.code_word
        else:
            code_word = None

        if self.current_job:
            message = self.current_job.task.key
        else:
            message = "Idle"
        d = {
            'runner': self.number,
            'code_word': code_word,
            'time': self.action_time.isoformat(),
            'message': message,
        }
        return d

    def announce(self, channel):
        hxdispatcher.send(channel,
                          {
                              'runners':
                                  {
                                      self.number: self.serialize_to_dict()
                                  }
                          }
                          )


def run_task(job_string, runner):
    try:
        logger.debug("%s rehydrating %s" % (runner, job_string))
        task = Job.rehydrate(job_string)
        logger.debug('%s running task: %s (%s)' % (runner, task.current_job.code_word, job_string))

        try:
            job = task.current_job  # TODO: Why do we expect this to be a Job object, rather than None?

            if not job:
                raise RuntimeError("Job isn't set yet.")

            runner.current_job = job
            task.run()
        except Exception, e:
            # print traceback.format_exc()
            # task.finish(traceback.format_exc())
            raise  #####

        hxdispatcher.send('all_tasks', {'runner': runner.number,
                                        'time': utc_now().isoformat(),
                                        'message': task.key,
                                        'code_word': task.current_job.code_word})

        runner.running_task = None
    except AttributeError as e:
        runner.running_task = None
        print traceback.format_exc()
