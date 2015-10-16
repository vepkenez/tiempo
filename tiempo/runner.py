import datetime
import json

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
        logger.info("Starting Runner %s for groups %s (%s)" % (number, thread_group_list, id(self)))
        for i in thread_group_list:
            if RUNNERS.has_key(i):
                RUNNERS[i].append(self)
            else:
                RUNNERS[i] = [self]

        self.action_time = utc_now()
        self.current_job = None
        self.task_groups = thread_group_list
        self.number = number
        self.error_state = False

    def __repr__(self):
        return 'Tiempo Runner %d' % self.number

    def cleanup(self):
        pass

    def cycle(self):
        '''
        If idle, find a job and run it.3
        '''

        # If we have a current Job, return BUSY and go no further.
        if self.current_job:
            logger.debug("Worker %s is busy with %s (%s / %s)" % (
                self.number,
                self.current_job.code_word,
                self.current_job.task.key,
                self.current_job.uid)
            )
            return BUSY

        # ...otherwise, look for a Job to run...,
        job_string = self.seek_job()

        # ...and run it.
        if job_string:
            self.action_time = utc_now()
            self.current_job = job = Job.rehydrate(job_string)
            logger.info("%s adopting %s" % (self, job))
            d = threads.deferToThread(self.run)
            return d
        else:
            # If we didn't get a job, we're IDLE.
            return IDLE

    def seek_job(self):
        for g in self.task_groups:

            logger.debug('%r checking for a Job in group %r' % (self, g))
            group_key = namespace(g)
            job_string = REDIS.lpop(group_key)
            if job_string:
                job_dict = json.loads(job_string)
                logger.info('%s found Job %s (%s) in group %s: %s' % (
                    self,
                    job_dict['codeWord'],
                    job_dict['uid'],
                    g,
                    job_dict['function_name'],
                    ))
                return job_string

    def run(self):
        '''
        Run the current job's task.
        '''
        logger.debug('%s running task: %s' % (self, self.current_job.code_word))
        self.announce('runners', alert=True)
        self.current_job.start()
        task = self.current_job.task

        return task.run()

    def finish_job(self, result):
        self.action_time = utc_now()
        self.current_job.finish()
        self.current_job = None
        self.announce('runners')
        self.error_state = False
        return  # And go back to cycling.

    def handle_error(self, failure):
        self.error_state = True
        logger.error(failure.value)
        d = self.serialize_to_dict()
        d.update({'error_message': str(failure.value)})
        hxdispatcher.send('errors', {'errors': {self.current_job.uid: d}})
        return self.finish_job(failure)

    def serialize_to_dict(self, alert=False):
        if self.current_job:
            code_word = self.current_job.code_word
            job_uid = self.current_job.uid
        else:
            code_word = None
            job_uid = None

        if self.current_job:
            message = self.current_job.task.key
        else:
            message = "Idle"
        d = {
            'runner': self.number,
            'codeWord': code_word,
            'time': self.action_time.isoformat(),
            'message': message,
            'jobUid': job_uid,
            'alert': alert,
            'error': self.error_state,
        }
        return d

    def announce(self, channel, alert=False):
        hxdispatcher.send(channel,
                          {
                              'runners':
                                  {
                                      self.number: self.serialize_to_dict(alert=alert)
                                  }
                          }
                          )
