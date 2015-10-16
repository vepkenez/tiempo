from hendrix.contrib.async.resources import MessageHandlerProtocol
from hendrix.contrib.async.messaging import hxdispatcher

class TiempoMessageProtocol(MessageHandlerProtocol):

    def dataReceived(self, data):
        # Unsettings workaround
        from tiempo.utils import all_jobs
        from tiempo.work import Job

        if data == "updateJobs":
            # TODO: Better way to announce jobs
            jobs_to_announce = {}
            jobs_dict = all_jobs([1, 2, 3])
            for job_list in jobs_dict.values():
                for job_string in job_list:
                    job = Job.rehydrate(job_string)
                    jobs_to_announce[job.uid] = job.serialize_to_dict()
            hxdispatcher.send('all_tasks', {'jobs': jobs_to_announce})
        else:
            return MessageHandlerProtocol.dataReceived(self, data)