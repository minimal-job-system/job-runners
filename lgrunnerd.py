#!/usr/bin/env python

import daemon
from daemon import pidfile

import configparser
# import dateutili
import glob
import importlib
import logging
import luigi
import os
import requests
import signal
import sys
import time


class WorkflowRunner():
    def __init__(self, log_file, scheduler_conf={}, jobsys_config={}):
        self.logger = logging.getLogger('lgrunnerd')
        self.logger.setLevel(logging.INFO)

        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.INFO)

        formatstr = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        formatter = logging.Formatter(formatstr)

        fh.setFormatter(formatter)
        self.logger.addHandler(fh)

        self.is_running = True

        self.logger.info("lgrunnerd: initializing")

        self.scheduler_config = {
            "use_central_scheduler":
                scheduler_conf["use_central_scheduler"] or True,
            "workers": scheduler_conf["workers"] or 2,
            "host": scheduler_conf["host"] or "localhost",
            "port": scheduler_conf["port"] or 8082
        }

        if (self.scheduler_config["use_central_scheduler"].lower() == "true"):
            self.scheduler_parameters = [
                '--scheduler-host', self.scheduler_config["host"],
                '--scheduler-port', str(self.scheduler_config["port"])
            ]
        else:
            self.scheduler_parameters = ['--local-scheduler']

        self.jobsys_config = {
            "host": jobsys_config["host"] or "localhost",
            "port": jobsys_config["port"] or 8000
        }

        self.job_system_url = "http://%s:%s" % (
            self.jobsys_config["host"], self.jobsys_config["port"]
        )

        sys.path.append(jobsys_config["module_path"])
        for module in glob.iglob(jobsys_config["module_path"] + '/*.py'):
            if module.endswith("__init__.py"):
                continue
            self.logger.info(
                "loading module: %s" % os.path.basename(module)[:-3]
            )
            importlib.import_module(os.path.basename(module)[:-3])

        self.logger.info("lgrunnerd: initialized")
        self.logger.info("----------------------")
        self.logger.info("configuration: scheduler")
        self.logger.info("    %s" % self.scheduler_config)
        self.logger.info("configuration: job system")
        self.logger.info("    %s" % self.jobsys_config)
        self.logger.info("----------------------")

    def start(self):
        self.logger.info("lgrunnerd: starting")
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
                    workflow_params = ["--job-id", str(job["id"])]
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
                        [workflow_name] + workflow_params +
                        [
                         '--workers', str(self.scheduler_config['workers']),
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
                    # job["duration"] = end_time - start_time
                    # job["exit_code"] = exit_code
                    requests.put(
                        "%s/api/jobs/%s/" % (self.job_system_url, job["id"]),
                        json=job
                    )

                time.sleep(2)
            except BaseException as ex:
                self.logger.error(ex, exc_info=True)
                self.stop()

        self.logger.info("lgrunnerd: terminated")

    def stop(self, signum=signal.SIGTERM, stack=None):
        self.logger.info("lgrunnerd: stopping")
        self.is_running = False


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('/etc/lgrunner.d/lgrunnerd.conf')

    with daemon.DaemonContext(
        working_directory="/var/lib/lgrunnerd",
        umask=0o002,
        pidfile=pidfile.TimeoutPIDLockFile(config['daemon']['pid_file']),
        stdout=open(config['daemon']['log_file'], "wb"),
        stderr=open(config['daemon']['log_file'], "wb"),
    ) as context:
        workflow_runner = WorkflowRunner(
            config['daemon']['log_file'],
            config['scheduler'], config['job_system']
        )

        signal.signal(signal.SIGTERM, workflow_runner.stop)
        workflow_runner.start()

    sys.exit(0)
