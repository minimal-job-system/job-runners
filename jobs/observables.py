#!/usr/bin/env python

import luigi

import observers


class Observable(object):
    """
    Observable base class.
    """
    __observers = []

    def register_observer(self, observer):
        if not isinstance(observer, observers.Observer):
            raise TypeError(
                "Can only register Observer classes for notification!"
            )
        self.__observers.append(observer)

    def remove_observer(self, observer):
        self.__observers.remove(observer)

    def notify_observers(self, *args, **kwargs):
        for observer in self.__observers:
            observer.notify(self, *args, **kwargs)


class ObservableJobSystemWorkflow(Observable, luigi.WrapperTask):
    """
    Luigi workflow task which automatically registers a
    JobSystemUpdater observer.
    """
    job_system_url = luigi.Parameter(default="http://localhost:8000")
    job_id = luigi.IntParameter(default=0)

    def __init__(self, *args, **kwargs):
        Observable.__init__(self)
        luigi.WrapperTask.__init__(self, *args, **kwargs)
        observers.JobSystemUpdater(self)

    def notify_observers(self, *args, **kwargs):
        for key in kwargs:
            kwargs[key] = kwargs[key]
        kwargs["job_system_url"] = self.job_system_url
        kwargs["job_id"] = self.job_id
        super(ObservableJobSystemWorkflow, self).notify_observers(
            *args, **kwargs
        )
