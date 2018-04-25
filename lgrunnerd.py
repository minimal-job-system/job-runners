#!/usr/bin/env python3

import daemon
from daemon import pidfile

import configparser
# import dateutil
import glob
import importlib
import logging
import luigi
import os
import requests
import signal
import sys
import time


class WorkflowRunner(object):
    def __init__(self, lgrunnerd_conf={}, luigid_conf={}, jobsystem_conf={}):
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
            self.scheduler_parameters = [
                '--scheduler-host', self.luigid_config["host"],
                '--scheduler-port', str(self.luigid_config["port"])
            ]
        else:
            self.scheduler_parameters = ['--local-scheduler']

        self.jobsystem_conf = {
            "host": jobsystem_conf["host"] or "localhost",
            "port": jobsystem_conf["port"] or 8000
        }

        self.job_system_url = "http://%s:%s" % (
            self.jobsystem_conf["host"], self.jobsystem_conf["port"]
        )

        self.register_event_handlers()

        sys.path.append(jobsystem_conf["module_path"])
        for module in glob.iglob(jobsystem_conf["module_path"] + '/*.py'):
            if module.endswith("__init__.py"):
                continue
            self.logger.info(
                "loading module: %s" % os.path.basename(module)[:-3]
            )
            importlib.import_module(os.path.basename(module)[:-3])

        self.logger.info("lgrunnerd: initialized")
        self.logger.info("----------------------")
        self.logger.info("configuration: luigi daemon")
        self.logger.info("    %s" % self.luigid_config)
        self.logger.info("configuration: job system")
        self.logger.info("    %s" % self.jobsystem_conf)
        self.logger.info("----------------------")

    def register_event_handlers(self):
        @luigi.Task.event_handler("event.lgrunner.status.notification")
        def on_status_notification(task, tracking_id, message):
            """
            User-defined callback function for status notifications.
            """
            response = requests.get(
                "%s/api/jobs/?id=%s" % (self.job_system_url, tracking_id),
                headers={'content-type': 'application/json'}
            )
            response.raise_for_status()

            jobs = response.json()
            if len(jobs) == 0:
                raise Exception("No job found for id '%s'!" % tracking_id)
            elif len(jobs) == 1:
                job = jobs[0]

                job["status"] = message

                requests.put(
                    "%s/api/jobs/%s/" % (self.job_system_url, tracking_id),
                    json=job
                )
            else:
                raise Exception(
                    "Multiple jobs found for id '%s'!" % tracking_id
                )

        @luigi.Task.event_handler("event.lgrunner.progress.notification")
        def on_progress_notification(task, tracking_id, notif_type, progress):
            """
            User-defined callback function for progress notifications.
            """
            response = requests.get(
                "%s/api/jobs/?id=%s" % (self.job_system_url, tracking_id),
                headers={'content-type': 'application/json'}
            )
            response.raise_for_status()

            jobs = response.json()
            if len(jobs) == 0:
                raise Exception("No job found for id '%s'!" % tracking_id)
            elif len(jobs) == 1:
                job = jobs[0]

                if job["status"] == "in progress":
                    if notif_type == "set_progress":
                        job["progress"] = progress
                    if notif_type == "add_progress":
                        job["progress"] += progress
                    if notif_type == "sub_progress":
                        job["progress"] -= progress

                requests.put(
                    "%s/api/jobs/%s/" % (self.job_system_url, tracking_id),
                    json=job
                )
            else:
                raise Exception(
                    "Multiple jobs found for id '%s'!" % tracking_id
                )

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

                    # setup luigi parameters
                    workflow_name = '%s.%s' % (
                        job["namespace"],
                        job["name"]
                    )
                    global_params = [
                        "--GlobalTrackingParams-tracking-id",
                        str(job["id"]),
                    ]
                    workflow_params = []
                    for param in job["parameters"]:
                        param_name = param["name"].replace('_', '-')

                        if param["type"] == 0:  # integer
                            workflow_params.append("--%s" % param_name)
                            workflow_params.append(param["value"])
                        if param["type"] == 1:  # float
                            workflow_params.append("--%s" % param_name)
                            workflow_params.append(param["value"])
                        if param["type"] == 2:  # string
                            workflow_params.append("--%s" % param_name)
                            workflow_params.append(param["value"])
                        if param["type"] == 3:  # boolean
                            if param["value"].lower() == "true":
                                workflow_params.append("--%s" % param_name)
                        if param["type"] == 4:  # datetime
                            workflow_params.append("--%s" % param_name)
                            workflow_params.append(param["value"])

                    self.logger.info(
                        "running workflow '%s'..." % workflow_name
                    )
                    self.logger.info("    parameters: %s" % workflow_params)
                    run_success = luigi.run(
                        [workflow_name] + workflow_params + global_params +
                        [
                         '--workers', str(self.luigid_config['workers']),
                         '--parallel-scheduling',
                         '--no-lock',
                         '--logging-conf-file', "/etc/lgrunner.d/luigi.conf"] +
                        self.scheduler_parameters
                    )

                    end_time = time.time()

                    self.logger.info(
                        "    %s after %ss" % (
                            "succeeded" if run_success else "failed",
                            end_time - start_time
                        )
                    )

                    # update job status
                    job["status"] = "completed" if run_success else "failed"
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
        config['lgrunnerd'], config['luigid'], config['jobsystem']
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
