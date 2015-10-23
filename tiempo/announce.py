class Announcer(object):

    progress_increments = None
    progress = 0

    def set_progress_increments(self, progress_increments):
        self.progress_increments = progress_increments

    def report_progress(self, progress):
        self.progress = progress

