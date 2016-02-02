import os
import sys

from tiempo.execution import thread_init
from .utils.loader import auto_load_tasks
from django.apps import AppConfig, apps

class TiempoAppConfig(AppConfig):
    name = 'tiempo.contrib.django_app'
    verbose_name = 'tiempo task running app'
    path = os.path.dirname(__file__)

    def ready(self):
        if not 'runtiempo' in sys.argv:
            thread_init()
            auto_load_tasks()
