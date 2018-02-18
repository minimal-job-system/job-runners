#!/usr/bin/env python3

import luigi
from global_params import GlobalTrackingParams
from tracking_proxy import TrackingProxy


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
    
    _tracking_proxy = TrackingProxy()
    """
    instance wide tracking proxy
    """
    
    _tracking_proxy.daemon = True
    # the tracking proxy runns until termination of the python application...
    _tracking_proxy.start()
    
    def __init__(self, *args, **kwargs):
        super(TrackableTask, self).__init__(*args, **kwargs)
        # the _tracking_proxy has to be initialized on class level
        # (not instance level) to be shared among all TrackableTasks.
        # by using a single proxy, we omit race conditions.
    
    def set_status(self, message):
        if GlobalTrackingParams().tracking_url != "":
            self._tracking_proxy.set_status(
                GlobalTrackingParams().tracking_url,
                GlobalTrackingParams().tracking_id,
                message
            )

    def set_progress(self, percentage):
        if GlobalTrackingParams().tracking_url != "":
            self._tracking_proxy.set_progress(
                GlobalTrackingParams().tracking_url,
                GlobalTrackingParams().tracking_id,
                percentage
            )

    def add_progress(self, percentage):
        if GlobalTrackingParams().tracking_url != "":
            self._tracking_proxy.add_progress(
                GlobalTrackingParams().tracking_url,
                GlobalTrackingParams().tracking_id,
                percentage
            )

    def sub_progress(self, percentage):
        if GlobalTrackingParams().tracking_url != "":
            self._tracking_proxy.sub_progress(
                GlobalTrackingParams().tracking_url,
                GlobalTrackingParams().tracking_id,
                percentage
            )


class JobSystemWorkflow(luigi.WrapperTask):
    """
    Default luigi workflow task.
    """
    pass
