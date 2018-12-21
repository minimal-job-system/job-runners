#!/usr/bin/env python3

import daemon
from daemon import pidfile

import configparser
# import dateutil
import datetime
import glob
import importlib
import logging
import luigi
# from luigi.task_status import PENDING, FAILED, DONE, RUNNING, \
# BATCH_RUNNING, UNKNOWN, DISABLED
import os
import psutil
import requests
import signal
import sys
import time


# helper functions
def fetch_remote_job(job_system_url, job_id):
    for retries in range(3):
        try:
            response = requests.get(
                "%s/api/jobs/%s" % (job_system_url, job_id),
                headers={'content-type': 'application/json'}
            )
            response.raise_for_status()
            break
        except Exception as ex:
            pass
    else:
        raise ex

    return response.json()


# event handlers
@luigi.Task.event_handler(luigi.Event.BROKEN_TASK)
def on_broken_task(task, exception):
    """Will be called directly after a broken dependency
       of `require` on any Task subclass (i.e. all luigi Tasks)
    """
    on_failure(task, exception)


@luigi.Task.event_handler(luigi.Event.FAILURE)
def on_failure(task, exception):
    """Will be called directly after a failed execution
       of `run` on any Task subclass (i.e. all luigi Tasks)
    """
    if not hasattr(task, 'tracking_url') or not hasattr(task, 'tracking_id'):
        return  # non-trackable tasks cannot be updated via the job system

    log_event = {
        "level": logging.ERROR,
        "message": str(exception),
        "date_created": datetime.datetime.now().isoformat()
    }
    response = requests.post(
        "%s/api/jobs/%s/logs/" % (task.tracking_url, task.tracking_id),
        json=log_event
    )
    response.raise_for_status()

    job = fetch_remote_job(task.tracking_url, task.tracking_id)
    job["status"] = "failed"
    response = requests.put(
        "%s/api/jobs/%s/" % (task.tracking_url, task.tracking_id),
        json=job
    )
    response.raise_for_status()


@luigi.Task.event_handler("event.lgrunner.status.notification")
def on_status_notification(task, message):
    """
    User-defined callback function for status notifications.
    """
    if not hasattr(task, 'tracking_url') or not hasattr(task, 'tracking_id'):
        raise Exception(
            "Non-trackable tasks cannot be updated via the job system"
        )

    job = fetch_remote_job(task.tracking_url, task.tracking_id)
    job["status"] = message
    response = requests.put(
        "%s/api/jobs/%s/" % (task.tracking_url, task.tracking_id),
        json=job
    )
    response.raise_for_status()


@luigi.Task.event_handler("event.lgrunner.progress.notification")
def on_progress_notification(task, notif_type, progress):
    if not hasattr(task, 'tracking_url') or not hasattr(task, 'tracking_id'):
        raise Exception(
            "Non-trackable tasks cannot be updated via the job system"
        )

    job = fetch_remote_job(task.tracking_url, task.tracking_id)

    if job["status"] == "in progress":
        if notif_type == "set_progress":
            job["progress"] = progress
        if notif_type == "add_progress":
            job["progress"] += progress
        if notif_type == "sub_progress":
            job["progress"] -= progress

    response = requests.put(
        "%s/api/jobs/%s/" % (task.tracking_url, task.tracking_id),
        json=job
    )
    response.raise_for_status()


@luigi.Task.event_handler("event.lgrunner.log.notification")
def on_log_notification(task, level, message):
    """
    User-defined callback function for log notifications.
    """
    if level == logging.WARNING:
        logging.getLogger('luigi').warning(message)
    if level == logging.ERROR:
        logging.getLogger('luigi').error(message)

    if not hasattr(task, 'tracking_url') or not hasattr(task, 'tracking_id'):
        raise Exception(
            "Non-trackable tasks cannot be updated via the job system"
        )

    log_event = {
        "level": level,
        "message": message,
        "date_created": datetime.datetime.now().isoformat()
    }
    response = requests.post(
        "%s/api/jobs/%s/logs/" % (task.tracking_url, task.tracking_id),
        json=log_event
    )
    response.raise_for_status()


class WorkflowRunner(object):
    def __init__(
        self,
        lgrunnerd_conf={}, luigid_conf={}, jobsystem_conf={}, luigi_conf={}
    ):
        self.logger = logging.getLogger('lgrunnerd')
        self.logger.setLevel(logging.INFO)

        formatstr = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(formatstr)

        fh = logging.FileHandler(lgrunnerd_conf["log_file"])
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        if lgrunnerd_conf['background'].lower() != "true":
            sh = logging.StreamHandler(sys.stdout)
            sh.setLevel(logging.INFO)
            sh.setFormatter(formatter)
            self.logger.addHandler(sh)

        self.is_running = False

        self.logger.info("lgrunnerd: initializing")

        self.luigid_config = {
            "use_central_scheduler":
                luigid_conf["use_central_scheduler"] or True,
            "workers": luigid_conf["workers"] or 2,
            "host": luigid_conf["host"] or "localhost",
            "port": luigid_conf["port"] or 8082
        }

        if (self.luigid_config["use_central_scheduler"].lower() == "true"):
            self.scheduler_params = {
                "scheduler_host": self.luigid_config["host"],
                "scheduler_port": int(self.luigid_config["port"])
            }
        else:
            self.scheduler_params = {"local_scheduler": True}

        self.jobsystem_conf = {
            "host": jobsystem_conf["host"] or "localhost",
            "port": jobsystem_conf["port"] or 8000
        }

        self.job_system_url = "http://%s:%s" % (
            self.jobsystem_conf["host"], self.jobsystem_conf["port"]
        )

        if luigi_conf["modules"] == '':
            self.logger.info("    no modules to import!" % module)
        else:
            for module in luigi_conf["modules"].split(','):
                self.logger.info("    importing module: %s" % module)
                importlib.import_module(module, package=None)

        self.luigi_conf = luigi_conf

        self.logger.info("lgrunnerd: initialized")
        self.logger.info("----------------------")
        self.logger.info("configuration: luigi daemon")
        self.logger.info("    %s" % self.luigid_config)
        self.logger.info("configuration: job system")
        self.logger.info("    %s" % self.jobsystem_conf)
        self.logger.info("----------------------")

    def run(self):
        while self.is_running:
            try:
                # request unprocessed luigi-workflow jobs
                response = requests.get(
                    (
                        "%s/api/jobs/" +
                        "?type_name=Luigi Workflow&status=initialized"
                    ) % (
                        self.job_system_url,
                    ),
                    headers={'content-type': 'application/json'}
                )
                response.raise_for_status()

                job_queue = response.json()

                while len(job_queue) > 0:
                    job = job_queue.pop()

                    # update job status
                    job["status"] = "in progress"
                    job["progress"] = 0
                    requests.put(
                        "%s/api/jobs/%s/" % (self.job_system_url, job["id"]),
                        json=job
                    )

                    start_time = time.time()

                    # setup luigi configurations
                    config = luigi.configuration.get_config()
                    config.set(
                        'GlobalTrackingParams', 'tracking_url',
                        self.job_system_url
                    )
                    config.set(
                        'GlobalTrackingParams', 'tracking_id',
                        str(job["id"])
                    )
                    
                    # setup luigi resources
                    if 'memory_limit' not in self.luigi_conf:
                        # use all available memory (in MB)
                        config.set(
                            'resources', 'memory',
                            str(
                                psutil.virtual_memory().available / 1024 / 1024
                            )
                        )
                    else:
                        config.set(
                            'resources', 'memory',
                            self.luigi_conf['memory_limit']
                        )

                    workflow_name = '%s.%s' % (
                        job["namespace"],
                        job["name"]
                    )
                    workflow_params = {}
                    for param in job["parameters"]:
                        if param["type"] == 0:  # integer
                            workflow_params[param["name"]] = \
                                int(param["value"])
                        if param["type"] == 1:  # float
                            workflow_params[param["name"]] = \
                                float(param["value"])
                        if param["type"] == 2:  # string
                            workflow_params[param["name"]] = param["value"]
                        if param["type"] == 3:  # boolean
                            workflow_params[param["name"]] = \
                                param["value"].lower() == "true"
                        if param["type"] == 4:  # datetime
                            workflow_params[param["name"]] = param["value"]

                    self.logger.info(
                        "running workflow '%s'..." % workflow_name
                    )
                    self.logger.info("    parameters: %s" % workflow_params)

                    workflow_class = luigi.task_register.Register.get_task_cls(
                        workflow_name
                    )

                    run_success = luigi.build(
                        [workflow_class(**workflow_params)],
                        workers=self.luigid_config['workers'],
                        parallel_scheduling=True,
                        no_lock=True,
                        logging_conf_file="/etc/lgrunner.d/luigi.conf",
                        **self.scheduler_params
                    )

                    end_time = time.time()

                    # update job information
                    job = fetch_remote_job(self.job_system_url, job['id'])

                    no_warnings = len([
                        entry for entry in job["log_entries"]
                        if entry["level"] == 30
                    ])
                    no_errors = len([
                        entry for entry in job["log_entries"]
                        if entry["level"] == 40
                    ])

                    self.logger.info(
                        "    %s after %s",
                        "succeeded" if run_success else "failed",
                        end_time - start_time
                    )

                    # update job status
                    status = "completed" if run_success else "failed"
                    if (no_errors > 0 or no_warnings > 0):
                        status += " [%s]" % (','.join([
                            "%s=%s" % (event, occurrences)
                            for event, occurrences in (
                                ("Errors", no_errors),
                                ("Warnings", no_warnings)
                            )
                            if occurrences > 0
                        ]))
                    job["status"] = status
                    job["progress"] = 100
                    # job["duration"] = end_time - start_time
                    # job["exit_code"] = exit_code
                    requests.put(
                        "%s/api/jobs/%s/" % (self.job_system_url, job["id"]),
                        json=job
                    )

                time.sleep(2)
            except BaseException as ex:
                if isinstance(ex, requests.exceptions.RequestException):
                    if isinstance(ex, requests.exceptions.ConnectionError):
                        self.logger.warning(
                            "could not connect to url '%s'!" % ex.request.url
                        )
                    elif isinstance(ex, requests.exceptions.HTTPError):
                        self.logger.warning(
                            "unexpected response for url '%s': [%s] %s!" % (
                                ex.request.url, ex.response.status_code,
                                ex.response.reason
                            )
                        )
                    else:
                        self.logger.warning(
                            "unexpected error while calling url '%s'!" % (
                                ex.request.url
                            )
                        )

                    time.sleep(10)
                    continue

                self.logger.error(ex, exc_info=True)
                self.stop()

        self.logger.info("lgrunnerd: terminated")

    def start(self):
        self.logger.info("lgrunnerd: starting")
        self.is_running = True
        self.run()

    def stop(self, signum=signal.SIGTERM, stack=None):
        self.logger.info("lgrunnerd: stopping")
        self.is_running = False


def main(config):
    workflow_runner = WorkflowRunner(
        config['lgrunnerd'], config['luigid'],
        config['jobsystem'], config['luigi'],
    )
    signal.signal(signal.SIGINT, workflow_runner.stop)
    signal.signal(signal.SIGTERM, workflow_runner.stop)
    workflow_runner.start()


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('/etc/lgrunner.d/lgrunnerd.conf')

    if "--background" in sys.argv:
        config['lgrunnerd']['background'] = "True"

    if config['lgrunnerd']['background'].lower() == "true":
        with daemon.DaemonContext(
            working_directory="/",
            umask=0o002,
            pidfile=pidfile.TimeoutPIDLockFile(
                config['lgrunnerd']['pid_file']
            ),
            detach_process=True,
            stdout=open(config['lgrunnerd']['log_file'], "a"),
            stderr=open(config['lgrunnerd']['log_file'], "a"),
        ) as context:
            main(config)
    else:
        main(config)

    sys.exit(0)
