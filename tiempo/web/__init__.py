from .. import PROJECT_PATH

from jinja2 import Environment, FileSystemLoader
import os

TEMPLATE_DIRS = [os.path.join(PROJECT_PATH, 'templates'), ]
JINJA_ENV = Environment(loader=FileSystemLoader(TEMPLATE_DIRS))
