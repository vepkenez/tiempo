
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

from logging import getLogger

import inspect
import uuid
import base64
import importlib
import functools
import datetime
import traceback
import json


logger = getLogger(__name__)


class TaskBase(object):

    @property
    def html(self):
        return '<task>%s</task>' % self.uid

    @classmethod
    def _encode(cls, dictionary):
        "Returns the given session dictionary pickled and encoded as a string."
        pickled = pickle.dumps(dictionary, pickle.HIGHEST_PROTOCOL)
        return base64.b64encode(pickled).decode('ascii')

    @staticmethod
    def _decode(data):

        encoded_data = base64.b64decode(force_bytes(data))
        try:
            # could produce ValueError if there is no ':'
            pickled = encoded_data
            dec = pickle.loads(pickled)
            # print 'pickle:', dec
            return dec

        except Exception:
            raise
            # ValueError, SuspiciousOperation, unpickling exceptions. If any of
            # these happen, just return an empty dictionary (an empty session).
            return {}

    @staticmethod
    def rehydrate(base_64):

        d = Task._decode(base_64)

        module = importlib.import_module(d['function_module_path'])
        T = getattr(module, d['function_name'])
        T.data = d
        T._thaw()
        return T


class Task(TaskBase):

    def __repr__(self):
        return self.key

    def __init__(self, *args, **kwargs):

        self.groups = ['ALL']

        self.day = None
        self.hour = None
        self.minute = None
        self.periodic = False

        self.uid = str(uuid.uuid1())

        self.key = self.uid
        self.function_name = None
        self.group = 'ALL'

        self.frozen = False
        # group and other attrs may be overridden here.
        for key, val in kwargs.items():
            setattr(self, key, val)

    def __call__(self, *args, **kwargs):

        # we only want this to happen if this is being called
        # as a decorator, otherwise all "calls" are performed as
        # special functions ie. "now" or "soon" etc.
        if args and hasattr(args[0], '__call__'):
            self.func = args[0]
            self.cache = {}
            functools.update_wrapper(self, self.func)
            self.key = '%s.%s' % (
                inspect.getmodule(self.func).__name__, self.func.__name__
            )
            TIEMPO_REGISTRY[self.key] = self
            return self

        return self

    def _freeze(self, *args, **kwargs):

        self.data = {
            'function_module_path': inspect.getmodule(self.func).__name__,
            'function_name': self.func.__name__,
            'args_to_function': args,
            'kwargs_to_function': kwargs,
            'schedule': self.get_schedule(),
            'uid': str(uuid.uuid1()),
        }
        self.frozen = True

        return self.data

    def _thaw(self, data=None):
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

        module = importlib.import_module(self.function_module_path)
        obj = getattr(module, self.function_name)

        if hasattr(obj, 'func'):
            return obj.func
        return obj

    def _enqueue(self):
        if not self.frozen:
            raise ValueError(
                'need to freeze this task before enqueuing'
            )

        d = self._encode(self.data)
        REDIS.rpush(self.group, d)
        return self.data['uid']

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

    def run(self):
        """
            runs this task right now
        """
        self.start()

        try:
            func = self._get_function()
            func(
                *getattr(self, 'args_to_function', ()),
                **getattr(self, 'kwargs_to_function', {})
            )
            self.finish()
        except Exception as e:
            errtext = traceback.format_exc()
            print errtext
            self.finish(error=errtext)

    def soon(self, *args, **kwargs):
        """
            schedules this task to be run with the args and kwargs
            whenever a worker participating in this task's groups
            comes up as available

        """
        logger.debug('scheduling task %r for next available execution', self)
        self._freeze(*args, **kwargs)
        self._enqueue()
        return self

    def now(self, *args, **kwargs):
        """
            runs this task NOW with the args and kwargs
        """
        self._freeze(*args, **kwargs)
        self._thaw()
        self.run()
        return self

    def finish(self, error=None):

        now = datetime.datetime.now()

        data = {
            'key': self.key,
            'uuid':self.uid,
            'start': self.start_time.strftime('%y/%m/%d %I:%M%p'),
            'finished': now.strftime('%y/%m/%d %I:%M%p'),
            'elapsed':(now-self.start_time).seconds,
            'errors': 'with errors: %s'%error if error else ''
        }

        data['text'] =  """
%(key)s:
started at %(start)s
finished in %(elapsed)s seconds
%(errors)s"""%data

        REDIS.set('last_finished_%s'%self.group, json.dumps(data))


    def start(self, error=None):

        self.start_time = datetime.datetime.now()
        now = datetime.datetime.now()

        data = {
            'key': self.key,
            'uuid':self.uid,
            'start': self.start_time.strftime('%y/%m/%d %I:%M%p'),
        }

        data['text'] =  """
%(key)s:
starting at %(start)s"""%data

        REDIS.set('last_started_%s'%self.group, json.dumps(data))

task = Task
