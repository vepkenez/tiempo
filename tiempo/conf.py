from logging import getLogger
import os

logger = getLogger(__name__)

try:
    from django.conf import settings
    has_django = True
except (ImportError, Exception), e:
    logger.warning(str(e))
    has_django = False

import json


if has_django:
    INTERVAL = getattr(settings, 'TIEMPO_INTERVAL', 5)
    THREAD_COUNT = getattr(settings, 'TIEMPO_THREADS', 1)
    TASK_GROUPS = ['ALL'] + getattr(settings, 'TIEMPO_GROUPS', [])
    RESULT_LIFESPAN = getattr(settings, 'TIEMPO_RESULT_LIFESPAN_DAYS', 1)
    DEBUG = settings.DEBUG

    REDIS_HOST = settings.REDIS_HOST
    REDIS_PORT = settings.REDIS_PORT
    REDIS_QUEUE_DB = settings.REDIS_QUEUE_DB
    REDIS_PW = settings.REDIS_PW

else:
    INTERVAL = os.environ.get('TIEMPO_INTERVAL', 5)
    THREAD_COUNT = os.environ.get('TIEMPO_THREADS', 1)
    TASK_GROUPS = ['ALL'] + os.environ.get('TIEMPO_GROUPS', [])
    RESULT_LIFESPAN = os.environ.get('TIEMPO_RESULT_LIFESPAN_DAYS', 1)
    DEBUG = os.environ.get('TIEMPO_DEBUG', False)

    REDIS_HOST = os.environ.get('TIEMPO_REDIS_HOST', 'localhost')
    REDIS_PORT = os.environ.get('TIEMPO_REDIS_PORT', 6379)
    REDIS_QUEUE_DB = os.environ.get('TIEMPO_REDIS_QUEUE_DB', 12)
    REDIS_PW = os.environ.get('TIEMPO_REDIS_PW', None)

TASK_PATHS = json.loads(os.environ.get('TIEMPO_TASK_PATHS', '["tiempo.demo"]'))
