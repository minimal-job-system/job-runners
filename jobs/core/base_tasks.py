#!/usr/bin/env python3

import luigi
from core.global_params import GlobalTrackingParams


class TrackableTask(luigi.Task):
    """
    Trackable luigi task.
    Wrapper class for updating remote task's status and progress on an external
    monitoring system.
    """
    progress_fraction = luigi.FloatParameter(default=0.0, significant=False)
    """
    task's progress fraction with respect to the whole workflow (in percentage)
    """

    @property
    def tracking_id(self):
        return GlobalTrackingParams().tracking_id
    
    @property
    def tracking_url(self):
        return GlobalTrackingParams().tracking_url

    def set_status(self, message):
        self.trigger_event("event.lgrunner.status.notification", self, message)

    def set_progress(self, progress):
        self.trigger_event(
            "event.lgrunner.progress.notification",
            self, "set_progress", progress
        )

    def add_progress(self, progress):
        self.trigger_event(
            "event.lgrunner.progress.notification",
            self, "add_progress", progress
        )

    def sub_progress(self, progress):
        self.trigger_event(
            "event.lgrunner.progress.notification",
            self, "sub_progress", progress
        )


class JobSystemTask(TrackableTask):
    """
    Default job system task.
    """
    def log(self, level, message):
        self.trigger_event(
            "event.lgrunner.log.notification", self, level, message
        )


class JobSystemWorkflow(TrackableTask):
    """
    Default job system workflow.
    """
    def complete(self):
        outputs = luigi.task.flatten(self.output())
        # given a workflow with dynamic dependencies, it may not define any
        # outputs within 'requires'. The outputs are then generated while
        # executing the 'run' method. Hence, we enforce a 'run' if no outputs
        # are given.
        if len(outputs) == 0:
            return False

        return all(map(lambda output: output.exists(), outputs))
