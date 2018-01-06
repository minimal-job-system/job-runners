#!/usr/bin/env python

import abc
import requests
import six

import observables


@six.add_metaclass(abc.ABCMeta)
class Observer(object):
    """
    Observer base class.
    """
    def __init__(self, observable):
        observable.register_observer(self)

    @abc.abstractmethod
    def notify(self, observable, *args, **kwargs):
        pass


class JobSystemUpdater(Observer):
    """
    Concrete observer for updating the job system with new task status
    """
    __job_system_url = "http://127.0.0.1:8000"

    def __init__(self, observable):
        super(JobSystemUpdater, self).__init__(observable)

    def notify(self, observable, *args, **kwargs):
        if not isinstance(observable, observables.Observable):
            return
        if "job_id" not in kwargs or "status" not in kwargs:
            return

        response = requests.get(
            "%s/api/jobs/?id=%s" % (self.__job_system_url, kwargs["job_id"]),
            headers={'content-type': 'application/json'}
        )
        response.raise_for_status()

        jobs = response.json()
        if len(jobs) == 1:
            job = jobs[0]
            job["status"] = kwargs["status"]
            job["progress"] = kwargs["progress"] if "progress" in kwargs else 0
            requests.put(
                "%s/api/jobs/%s/" % (self.__job_system_url, kwargs["job_id"]),
                json=job
            )
