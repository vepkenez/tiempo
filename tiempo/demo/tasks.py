
import time
from tiempo.task import task


@task(periodic=True, minute=55)
def test_task_logging():
    print 'test task logging is running'

    for i in range(1, 20):
        print 'doing nothing: %d' % i

    print 'spawned a task:', test_task_spawning.soon(
        True, veggies=['celery', 'carrot', 'beets']
    )
    time.sleep(3)
    print 'spawned a task:', test_task_spawning.soon(
        False, fruits=['banana', 'apple', 'cherry']
    )

    print 'sleeping for 60 secs'
    time.sleep(61)
    print 'slept for 60'



@task(priority=1)
def test_task_spawning_priority1(*args, **kwargs):
    time.sleep(5)
    print 'this task ran just fine with args, kwargs: %r, %r' % (args, kwargs)

@task(priority=2)
def test_task_spawning_priority2(*args, **kwargs):
    time.sleep(5)
    print 'this task ran just fine with args, kwargs: %r, %r' % (args, kwargs)

@task(priority=3)
def test_task_spawning_priority3(*args, **kwargs):
    time.sleep(5)
    print 'this task ran just fine with args, kwargs: %r, %r' % (args, kwargs)
