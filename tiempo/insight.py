from collections import OrderedDict
from tiempo.conn import REDIS
import json


def completed_jobs(include_errors=True,
                   include_successes=True):

    jobs = {}
    if include_errors:
        errors = REDIS.hgetall('errors')
        jobs.update(errors)

    if include_successes:
        successes = REDIS.hgetall('successes')
        jobs.update(successes)

    jobs_list = []
    for uid, job_dict_string in jobs.items():
        job_dict = json.loads(job_dict_string)

        jobs_list.append(
            (uid, job_dict)
        )
    jobs_list.sort(key=lambda job: job[1]['finish_time'])

    completed_jobs_dict = OrderedDict(jobs_list)

    return completed_jobs_dict
