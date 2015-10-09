PROJECT_PATH = __path__[0]

TIEMPO_REGISTRY = {}
RUNNERS = {}

def all_runners():
    all_runners = set()
    for runner_list in RUNNERS.values():
        for runner in runner_list:
            all_runners.add(runner)

    return all_runners


REDIS_GROUP_NAMESPACE = 'tiempogroup'
RECENT_KEY = 'tiempo:recent_tasks'
RESULT_PREFIX = 'tiempo:task_result'

__version__ = "1.1.8"
