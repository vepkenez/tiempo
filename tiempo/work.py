from tiempo import RUNNERS
from tiempo.utils import utc_now, namespace
from hendrix.contrib.async.messaging import hxdispatcher
from twisted.internet import task

try:
    from django.utils.encoding import force_bytes
except ImportError:
    from .utils import force_bytes

try:
    from six.moves import cPickle as pickle
except ImportError:
    import pickle

from . import TIEMPO_REGISTRY

from .conn import REDIS

import inspect
import uuid
import base64
import importlib
import functools
import datetime
import traceback
import json
import random
from twisted.logger import Logger
logger = Logger()

word_file = "/usr/share/dict/words"
WORDS = open(word_file).read().splitlines()


def announce_tasks_to_client():
        '''
        Push the list of tasks to the client.
        '''
        for task in TIEMPO_REGISTRY.values():
            serialized_task = task.serialize_to_dict()
            hxdispatcher.send('all_tasks', serialized_task)

task.LoopingCall(announce_tasks_to_client).start(2)


class Job(object):
    '''
    A task running right now.
    '''

    def __init__(self, task):
        self.uid = str(uuid.uuid1())
        self.code_word = task.code_word
        self.task = task
        self.status = 'waiting'
        self.enqueued = False

    def serialize_to_dict(self):
        d = {'job': self.code_word,
             'job_uid': self.uid,
             'key': self.task.key,
             'enqueued': self.enqueued,
             'status': self.status,
             }
        return d

    def soon(self, *args, **kwargs):
        """
        schedules this task to be run with the args and kwargs
        whenever a worker participating in this task's groups
        comes up as available
        """
        logger.debug('scheduling task %s for next available execution' % self)

        queue_name = kwargs.pop('tiempo_wait_for', None)

        if queue_name:
            queue_name = namespace(queue_name)
        else:
            queue_name = self.task.group_key

        self._freeze(*args, **kwargs)
        self._enqueue(queue_name)
        return self

    def now(self, *args, **kwargs):
        """
        runs this task NOW with the args and kwargs
        """
        self._freeze(*args, **kwargs)
        self._thaw()
        return self.run()

    def finish(self, error=None):
        try:
            logger.debug('finished: %s (%s)' % (self.code_word, utc_now() - self.start_time))
        except AttributeError:
            # Somehow this job finished without ever being started.
            self

        self._enqueue_dependents()

        now = utc_now()

        data = {
            'key': self.task.key,
            'uid':self.uid,
            'start': self.start_time.strftime('%y/%m/%d %I:%M%p'),
            'finished': now.strftime('%y/%m/%d %I:%M%p'),
            'elapsed': (now - self.start_time).seconds,
            'errors': 'with errors: %s' % error if error else ''
        }

        data['text'] =  """
            %(key)s:
            started at %(start)s
            finished in %(elapsed)s seconds
            %(errors)s""" % data

        REDIS.set('tiempo_last_finished_%s' % self.task.group_key, json.dumps(data))

        self.status = 'finished'
        self.announce('job_queue')

    def start(self, error=None):

        self.start_time = utc_now()

        logger.debug('Starting %s' % self.code_word)

        data = {
            'key': self.task.key,
            'uid':self.uid,
            'start': self.start_time.strftime('%y/%m/%d %I:%M%p'),
        }

        data['text'] =  """
%(key)s:
starting at %(start)s"""%data

        REDIS.set('tiempo_last_started_%s' % self.task.group_key, json.dumps(data))

        self.status = "running"
        self.announce('job_queue')

    def announce(self, channel):
        return hxdispatcher.send(channel, self.serialize_to_dict())

    def _freeze(self, *args, **kwargs):

        """
            creates a 'data' object which will be serialized and pushed into redis
            when this task is queued for execution.

            this data object should contain everything needed to reconstitute and
            execute the original function with args and kwargs in the context
            of a worker process
        """

        # make a fresh uid specific to this instance that will persist
        # through getting saved to the queue
        self.uid = str(uuid.uuid1())

        self.data = {
            'function_module_path': inspect.getmodule(self.task._get_function()).__name__,
            'function_name': self.task.func.__name__,
            'args_to_function': args,
            'kwargs_to_function': kwargs,
            'schedule': self.task.get_schedule(),
            'uid': self.uid,
        }
        self.frozen = True

        return self.data

    def _enqueue(self, queue_name):
        if not self.frozen:
            raise ValueError(
                'need to freeze this task before enqueuing'
            )

        # Announce that job is queued.
        self.enqueued = utc_now().isoformat()
        self.status = "queued"
        self.announce('job_queue')
        logger.debug("Queueing %s" % self.code_word)


        d = self._encode(self.data)
        REDIS.rpush(queue_name, d)

        return self.data['uid']

    def _enqueue_dependents(self):

        obj = REDIS.lpop(self.task.waitfor_key)
        if obj:
            awaiting_task = Task.rehydrate(obj)

            print 'enqueing:', awaiting_task,'uid',awaiting_task.uid, 'with waitfor:', self.waitfor_key

            awaiting_task.soon(
                *getattr(awaiting_task, 'args_to_function', ()),
                **getattr(awaiting_task, 'kwargs_to_function', {})
            )

            self._enqueue_dependents()

    @classmethod
    def _encode(cls, dictionary):
        "Returns the given session dictionary pickled and encoded as a string."
        try:
            pickled = pickle.dumps(dictionary, pickle.HIGHEST_PROTOCOL)
        except pickle.PicklingError as E:
            msg = "PICKLING ERROR.  You have some complex data in the arguments to %r that cannot be pickled"%self
            print msg
            raise E(msg)
        return base64.b64encode(pickled).decode('ascii')

    @staticmethod
    def _decode(data):

        encoded_data = base64.b64decode(force_bytes(data))
        try:
            # could produce ValueError if there is no ':'
            pickled = encoded_data
            dec = pickle.loads(pickled)

            return dec

        except Exception:
            raise
            # ValueError, SuspiciousOperation, unpickling exceptions. If any of
            # these happen, just return an empty dictionary (an empty session).
            return {}

    @staticmethod
    def rehydrate(base_64):

        d = Job._decode(base_64)

        module = importlib.import_module(d['function_module_path'])
        T = getattr(module, d['function_name'])

        if not isinstance(T, Task):
            # if the function that this task decorates is imported from
            # a different file path than where the decorating task is instantiated,
            # it will not be wrapped normally
            # so we wrap it.

            T = Task(T)

        T.data = d
        T._thaw()
        return T


class TaskBase(object):
    pass


class Task(TaskBase):

    def __repr__(self):
        return self.key

    def __init__(self, *args, **kwargs):

        self.day = None
        self.hour = None
        self.minute = None
        self.periodic = False
        self.current_job = None

        self.code_word = None
        self.generate_code_word()

        self.key = ''
        self.function_name = None
        self.group = unicode(kwargs.get('priority', kwargs.get('group','1')))  # TODO: refactor priority

        self.frozen = False
        # group and other attrs may be overridden here.
        for key, val in kwargs.items():
            setattr(self, key, val)

        if args and hasattr(args[0], '__call__'):
            self.__setup(args)

    def __call__(self, *args, **kwargs):

        # we only want this to happen if this is being called
        # as a decorator, otherwise all "calls" are performed as
        # special functions ie. "now" or "soon" etc.
        if args and hasattr(args[0], '__call__'):
            self.__setup(args)

        return self

    def __setup(self, args):
        self.func = args[0]
        self.cache = {}
        functools.update_wrapper(self, self.func)
        self.key = '%s.%s' % (
            inspect.getmodule(self.func).__name__, self.func.__name__
        )
        TIEMPO_REGISTRY[self.key] = self
        return self

    @property
    def group_key(self):
        return namespace(self.group)

    @property
    def waitfor_key(self):
        return namespace(self.uid)

    @property
    def stop_key(self):
        return '%s:schedule:%s:stop' % (namespace(self.group), self.key)

    def generate_code_word(self):
        self.code_word = random.choice(WORDS)
        return self.code_word

    def serialize_to_dict(self):
        task_as_dict = {
            'canonical': True,
            'code_word': self.code_word,
            'path': self.key
        }
        return task_as_dict

    def run(self, job=None):
        """
        run right now
        """

        if job:
            self.current_job = job

        if not self.current_job:
            raise TypeError("Task must be run by a Job.")

        self.current_job.start()

        try:
            func = self._get_function()
            result = func(
                *getattr(self, 'args_to_function', ()),
                **getattr(self, 'kwargs_to_function', {})
            )
            return result

        except Exception as e:
            raise #############
            errtext = traceback.format_exc()
            self.finish(error=errtext)

    def _thaw(self, data=None):
        """
            If this is called it is after a task has been instantiated by
            a worker process after being pulled as serialized data from redis
            and decoded.
            
            the for loop where the attrs are set from the data dict 
            will set this task to the same state as if it was 
            __init__ed as a decorator 
        """
        
        if not data and hasattr(self, 'data'):
            data = self.data

        if data:
            for key, val in data.items():
                setattr(self, key, val)

    def _get_function(self):
        if hasattr(self, 'func'):
            return self.func

        if not self.function_name:
            self._thaw()

        if hasattr(self, 'function_module_path'):

            module = importlib.import_module(self.function_module_path)
            obj = getattr(module, self.function_name)

            if hasattr(obj, 'func'):
                return obj.func
            return obj
        else:
            print "could not find function", self.func

    def get_schedule(self):
        if self.periodic:
            sched = [
                '*',
                '*',
                '*'
            ]
            for i, inc in enumerate(['day', 'hour', 'minute']):
                attr = getattr(self, inc, None)
                if attr is not None:
                    if attr != '*':
                        attr = '%02d' % int(attr)
                    sched[i] = attr

            return '.'.join(sched)

    def next_expiration_dt(self):
        '''
        The next future datetime at which this trabajo's waiting period will expire.
        '''
        if hasattr(self, 'force_interval'):
            expiration_dt = utc_now() + datetime.timedelta(
                seconds=self.force_interval
            )
        else:
            run_times = get_task_keys()
            expiration_dt = run_times.get(self.get_schedule())

        return expiration_dt
    
    def spawn_job(self):
        job = Job(self)
        job.soon()
        self.current_job = job

        # Now we generate a new code word for next time.
        self.generate_code_word()

        return job