from tiempo.work import Trabajo
import random
import time


@Trabajo(periodic=True, force_interval=4)
def llama():
    number = random.choice(range(5, 12))
    time.sleep(number)
    if number in [5, 7, 9, 11]:
        raise RuntimeError("I am error.  Friend of bagu.")
    return