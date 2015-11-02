from collections import OrderedDict
from tiempo.contrib.django.utils import six

from tiempo.conf import TASK_PATHS

import chalk
import importlib
import os
import datetime
import pytz
from dateutil.relativedelta import relativedelta
from tiempo import REDIS_GROUP_NAMESPACE
from tiempo.conn import REDIS


def utc_now():
    return datetime.datetime.now(pytz.utc)


def import_tasks():
    for app in TASK_PATHS:
        if not app.split('.')[-1] == 'tasks':
            module = importlib.import_module(app)
            filename = os.path.join(module.__path__[0], 'tasks.py')
            if not os.path.isfile(filename):
                chalk.yellow(app + ' does not have a tasks module.')
                continue
            else:
                app = app + '.tasks'

        chalk.blue(app.upper() + ': imported tasks from %s' % app)
        importlib.import_module(app)


# /////////////////////////////////////////////////////////////////////////////
# Django Rubbish
class Promise(object):
    """
    This is just a base class for the proxy class created in
    the closure of the lazy function. It can be used to recognize
    promises in code.
    """
    pass


def force_bytes(s, encoding='utf-8', strings_only=False, errors='strict'):
    """
    Similar to smart_bytes, except that lazy instances are resolved to
    strings, rather than kept as lazy objects.

    If strings_only is True, don't convert (some) non-string-like objects.
    """
    if isinstance(s, six.memoryview):
        s = bytes(s)
    if isinstance(s, bytes):
        if encoding == 'utf-8':
            return s
        else:
            return s.decode('utf-8', errors).encode(encoding, errors)
    if strings_only and (s is None or isinstance(s, int)):
        return s
    if isinstance(s, Promise):
        return six.text_type(s).encode(encoding, errors)
    if not isinstance(s, six.string_types):
        try:
            if six.PY3:
                return six.text_type(s).encode(encoding)
            else:
                return bytes(s)
        except UnicodeEncodeError:
            if isinstance(s, Exception):
                # An Exception subclass containing non-ASCII data that doesn't
                # know how to print itself properly. We shouldn't raise a
                # further exception.
                return b' '.join(
                    [
                        force_bytes(arg, encoding, strings_only, errors)
                        for arg in s
                    ]
                )
            return six.text_type(s).encode(encoding, errors)
    else:
        return s.encode(encoding, errors)


def task_time_keys():
    """
        creates a dictionary containing a mapping every combination of
        cron like keys with the corresponding amount of time
        until the next execution of such a task if it were to be executed
        now.


        ie:

        {'*.*.*': <1 minute from now>,}
        {'*.5.*': <1 minute from now>,}
        {'*.5.11': <24 hours from now>,}

    """

    time_keys = {}

    now = utc_now()

    # every day, every hour, every minute
    time_keys['*.*.*'] = now + datetime.timedelta(minutes=1)

    # every day, every hour, this minute
    time_keys[now.strftime('*.*.%M')] = now + datetime.timedelta(hours=1)

    # every day, this hour, this minute
    time_keys[now.strftime('*.%H.%M')] = now + datetime.timedelta(days=1)

    # this day, this hour, this minute
    time_keys[now.strftime('%d.%H.%M')] = now + relativedelta(months=1)

    # this day, this hour, every minute
    time_keys[now.strftime('%d.%H.*')] = now + datetime.timedelta(minutes=1)

    # this day, every hour, every minute
    time_keys[now.strftime('%d.*.*')] = now + datetime.timedelta(minutes=1)

    # every day, this hour, every minute
    time_keys[now.strftime('*.%H.*')] = now + datetime.timedelta(minutes=1)

    # logger.debug(time_keys.keys())
    return time_keys


def namespace(group_name):
    if group_name:
        return '%s:%s' % (REDIS_GROUP_NAMESPACE, group_name)

    # returns None if passed something Falsey.


def all_jobs(groups):
    '''
    Find all Jobs in the list of groups, return them as a dict.
    '''
    jobs_dict = OrderedDict()
    for group in groups:
        name = namespace(group)
        jobs_dict[group] = REDIS.lrange(name, 0, -1)
    return jobs_dict
