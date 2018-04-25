#!/usr/bin/env python3

import luigi
from core.global_params import GlobalTrackingParams


class TrackableTask(luigi.Task):
    """
    Trackable luigi task.
    Wrapper class for updating a task's status and progress on an external
    monitoring system.
    """
    progress_fraction = luigi.FloatParameter(default=0.0, significant=False)
    """
    task's progress fraction with respect to the whole workflow (in percentage)
    """

    def set_status(self, message):
        self.trigger_event(
            "event.lgrunner.status.notification",
            self, GlobalTrackingParams().tracking_id, "set_status", message
        )

    def set_progress(self, progress):
        self.trigger_event(
            "event.lgrunner.progress.notification",
            self, GlobalTrackingParams().tracking_id, "set_progress", progress
        )

    def add_progress(self, progress):
        self.trigger_event(
            "event.lgrunner.progress.notification",
            self, GlobalTrackingParams().tracking_id, "add_progress", progress
        )

    def sub_progress(self, progress):
        self.trigger_event(
            "event.lgrunner.progress.notification",
            self, GlobalTrackingParams().tracking_id, "sub_progress", progress
        )


class JobSystemWorkflow(luigi.Task):
    """
    Default luigi workflow task.
    """
    pass
