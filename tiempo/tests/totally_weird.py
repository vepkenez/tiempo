import chalk
from tiempo.execution import thread_init
from tiempo.task import Task

@Task(priority=1, periodic=True, force_interval=1)
def something():
    chalk.red("something")

thread_init()

from twisted.internet import reactor
reactor.run()

