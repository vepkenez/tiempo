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

    def __init__(self, number, thread_group_list):
        logger.info("Starting Thread manager %s with threads %s (%s)" % (number, thread_group_list, id(self)))
        for i in thread_group_list:
            if RUNNERS.has_key(i):
                RUNNERS[i].append(self)
            else:
                RUNNERS[i] = [self]

        self.active_task_string = None
        self.current_job = None
        self.task_groups = thread_group_list
        self.number = number

    def __repr__(self):
        return 'tiempo thread %d' % self.number

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

        # ...otherwise, look for a Job to run.
        for g in self.task_groups:

                msg = '%r checking for work in group %r' % (self, g)
                # logger.debug(msg)
                name = namespace(g)
                task = REDIS.lpop(name)
                if task:
                    logger.debug(
                        'RUNNING TASK on thread %r: %s' % (self, task)
                    )
                    self.active_task_string = task
                    break

        if self.active_task_string:
            d = threads.deferToThread(run_task, self.active_task_string, self)
            return d
        else:
            return IDLE

    def finish_job(self, result):
        self


def run_task(job_string, runner):
    try:
        task = Job.rehydrate(job_string)

        try:
            runner.current_job = task.current_job
            task.run()
        except Exception, e:
            # print traceback.format_exc()
            # task.finish(traceback.format_exc())
            raise  #####

        hxdispatcher.send('all_tasks', {'runner': runner.number,
                                        'time': utc_now().isoformat(),
                                        'message': task.key,
                                        'code_word': task.current_job.code_word})

        thread.running_task = None
    except AttributeError as e:
        thread.running_task = None
        print traceback.format_exc()
