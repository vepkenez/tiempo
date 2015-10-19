from tiempo.work import Task
import random
import time

@Task(periodic=True, force_interval=5)
def llama():
    number = random.choice(range(5, 11))
    time.sleep(number)
    if number in [5, 6, 7, 8, 9, 10]:
        raise RuntimeError("I am error.  Friend of bagu.")
    return