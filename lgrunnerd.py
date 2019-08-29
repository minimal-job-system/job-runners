#!/usr/bin/env python3

import daemon
from daemon import pidfile

import configparser
from contextlib import contextmanager
# import dateutil
import datetime
import functools
import glob
import importlib
import logging
import multiprocessing
import os
import psutil
import requests
import signal
import sys
import time

# define the default luigi config parser
os.environ['LUIGI_CONFIG_PARSER'] = 'conf'   # noqa: E402

import luigi
from luigi.interface import core as core_config
from luigi.task_status import PENDING, FAILED, DONE, RUNNING, \
    BATCH_RUNNING, SUSPENDED, UNKNOWN, DISABLED


# wrapper functions
def retry(attempts=3, non_retry_codes=[]):
    """
    Decorator for HTTP request functions with retry attempts.
    :param attempts: maximum number of retry attempts
    :param non_retry_codes: HTTP response codes which immediately raise an
        exception
    :returns: a decorator function
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            attempt = 0
            while True:
                attempt += 1
                try:
                    return func(*args, **kwargs)
                except Exception as ex:
                    if (
                        isinstance(ex, requests.exceptions.HTTPError) and
                        ex.response.status_code in non_retry_codes
                    ):
                        raise  # immediately forward exceptions for
                               # non-retry status codes
                    
                    if (attempt > attempts):
                        raise
                    time.sleep(5)
        return wrapper
    return decorator


# helper functions
@retry(3)
def fetch_remote_jobs(
    job_system_url, type='Luigi Workflow', status='initialized',
    days_since_creation=30
):
    """
    Helper funtion to fetch remote jobs.
    :param job_system_url: url to the job system
    :param type: only fetches jobs of this type
    :param status: only fetches jobs with this status
    :param days_since_creation: only fetches jobs which creation date is not
        too old
    :returns: a job, raises an HTTPError otherwise
    """
    response = requests.get(
        (
            "%s/api/jobs/?ordering=date_created"
            "&type_name=%s&status=%s&days_since_creation=%s"
        ) % (job_system_url, type, status, days_since_creation),
        headers={'content-type': 'application/json'}
    )
    response.raise_for_status()

    return response.json()


@retry(3)
def fetch_remote_job(job_system_url, job_id):
    """
    Helper funtion to fetch remote jobs.
    :param job_system_url: url to the job system
    :param job_id: only fetch the job whith this ID
    :returns: a job, raises an HTTPError otherwise
    """
    response = requests.get(
        "%s/api/jobs/%s" % (job_system_url, job_id),
        headers={'content-type': 'application/json'}
    )
    response.raise_for_status()

    return dict(
        response.json(),
        **{
            'etag': response.headers['ETag'],
            'last_modified': response.headers['Last-Modified']
        }
    )


@retry(3, non_retry_codes=[412])  # immediately forward precondition failures
def update_remote_job(job_system_url, job, headers={}):
    """
    Helper funtion to update remote jobs.
    :param job_system_url: url to the job system
    :param job: the job to update
    :param headers: optional HTTP headers
    :returns: None, raises an HTTPError otherwise
    """
    response = requests.put(
        "%s/api/jobs/%s/" % (job_system_url, job['id']),
        headers=headers,
        json=job
    )
    response.raise_for_status()


@retry(3)
def add_remote_log_entry(job_system_url, job_id, log_entry):
    """
    Helper funtion to add log entries to remote jobs.
    :param job_system_url: url to the job system
    :param job_id: ID of the job to add the log entry to
    :param log_entry: the log entry to add
    :returns: None, raises an HTTPError otherwise
    """
    response = requests.post(
        "%s/api/jobs/%s/logs/" % (job_system_url, job_id),
        json=log_entry
    )
    response.raise_for_status()


# event callbacks for luigi tasks
# @see: LuigiProcess.configure()
def on_broken_task(task, exception, tracking_url, job_id):
    """
    Luigi callback function for broken dependencies.
    :param task: the task which dependencies are broken
    :param exception: the corresponding exception
    :param tracking_url: url to the tracking system
    :param job_id: ID of the remote job to send the notification to
    :returns: None, raises an HTTPError otherwise
    """
    on_failure(task, exception, tracking_url, job_id)


def on_failure(task, exception, tracking_url, job_id):
    """
    Luigi callback function for failed tasks.
    :param task: the task which failed
    :param exception: the corresponding exception
    :param tracking_url: url to the tracking system
    :param job_id: ID of the remote job to send the notification to
    :returns: None, raises an HTTPError otherwise
    """
    log_entry = {
        "level": logging.ERROR,
        "message": str(exception),
        "date_created": datetime.datetime.now().isoformat()
    }
    add_remote_log_entry(tracking_url, job_id, log_entry)

    job = fetch_remote_job(tracking_url, job_id)
    job["status"] = "failed"
    update_remote_job(tracking_url, job)


def on_status_notification(task, message, tracking_url, job_id):
    """
    Luigi callback function for status notifications.
    :param task: the task which sends the notification
    :param message: the notification message
    :param tracking_url: url to the tracking system
    :param job_id: ID of the remote job to send the notification to
    :returns: None, raises an HTTPError otherwise
    """
    job = fetch_remote_job(tracking_url, job_id)
    job["status"] = message
    update_remote_job(tracking_url, job)

    # forward message to the luigi scheduler
    task.set_status_message(message)


def on_progress_notification(task, notif_type, value, tracking_url, job_id):
    """
    Luigi callback function for progress notifications.
    :param task: the task which sends the notification
    :param notif_type: the type of the notification
    :param value: the task's progress or fraction to update
    :param tracking_url: url to the tracking system
    :param job_id: ID of the remote job to send the notification to
    :returns: None, raises an HTTPError otherwise
    """
    job = fetch_remote_job(tracking_url, job_id)

    shared_meta_data = task.__class__.shared_meta_data

    if job["status"] == "in progress":
        task_meta_data = shared_meta_data.setdefault(task.task_id, {})
        if notif_type == "set_fraction":
            task_meta_data['progress_fraction'] = value
        if notif_type == "set_percentage":
            task_meta_data['progress_percentage'] = value

            # forward message to the luigi scheduler
            task.set_progress_percentage(value)

        # update shared object
        # @see https://docs.python.org/2/library/multiprocessing.html#managers
        # modifications to mutable values or items in dict and list
        # proxies will NOT be propagated through the manager.
        # to modify such an item, you can re-assign the modified object to the
        # container proxy
        shared_meta_data[task.task_id] = task_meta_data

        total_fraction = sum(
            [
                task['progress_fraction']
                for task_id, task in shared_meta_data.items()
                if task_id.startswith('gliberal.Parallel')  # our work packages
            ]
        )
        if total_fraction == 0:
            total_progress = 0.0
        else:
            total_progress = sum([
                (
                    task.get('progress_fraction', 0.0) / total_fraction
                ) * task.get('progress_percentage', 0.0)
                for task_id, task in shared_meta_data.items()
            ])
        job["progress"] = total_progress

    update_remote_job(tracking_url, job)


def on_log_notification(
    task, level, message, tracking_url, job_id, warning_counter, error_counter
):
    """
    Luigi callback function for log notifications.
    :param task: the task which sends the notification
    :param level: the log level
    :param message: the log message
    :param tracking_url: url to the tracking system
    :param job_id: ID of the remote job to send the notification to
    :param warning_counter: counter to track warnings
    :param error_counter: counter to track errors
    :returns: None, raises an HTTPError otherwise
    """
    if level == logging.WARNING:
        logging.getLogger('luigi-interface').warning(message)
        warning_counter.increment()
    if level == logging.ERROR:
        logging.getLogger('luigi-interface').error(message)
        error_counter.increment()

    log_entry = {
        "level": level,
        "message": message,
        "date_created": datetime.datetime.now().isoformat()
    }
    add_remote_log_entry(tracking_url, job_id, log_entry)


# helper classes
class Counter(object):
    """
    Helper class for a counter which can be shared between processes.
    """
    def __init__(self, init_val=0):
        self.val = multiprocessing.RawValue('i', init_val)
        self.lock = multiprocessing.Lock()

    def increment(self):
        with self.lock:
            self.val.value += 1

    @property
    def value(self):
        return self.val.value


class Status(object):
    """
    Helper class for a status which can be shared between processes.
    """
    def __init__(self, status=DISABLED):
        self.status_id = multiprocessing.RawValue(
            'i', self._convert_status(status)
        )
        self.lock = multiprocessing.Lock()

    def _convert_status(self, status):
        return [
            PENDING, FAILED, DONE, RUNNING, BATCH_RUNNING, SUSPENDED, UNKNOWN,
            DISABLED
        ].index(status)

    def _convert_status_id(self, status_id):
        return [
            PENDING, FAILED, DONE, RUNNING, BATCH_RUNNING, SUSPENDED, UNKNOWN,
            DISABLED
        ][status_id]

    def set(self, status):
        with self.lock:
            self.status_id.value = self._convert_status(status)

    def get(self):
        return self._convert_status_id(self.status_id.value)


class IntValue(object):
    """
    Helper class for an integer which can be shared between processes.
    """
    def __init__(self, init_val=0):
        self.val = multiprocessing.RawValue('i', init_val)
        self.lock = multiprocessing.Lock()

    def set(self, new_val):
        with self.lock:
            self.val.value = new_val

    def get(self):
        return self.val.value


class WorkerSchedulerFactory(object):
    """
    Helper class for initializing Luigi schedulers and workers.
    """
    def create_local_scheduler(self):
        return luigi.scheduler.Scheduler(
            prune_on_get_work=True, record_task_history=False,
            send_messages=True
        )

    def create_remote_scheduler(self, url):
        # parameters for the remote scheduler (luigid) can be set via the
        # default configuration in /etc/luigi/luigi.cfg.
        # E.g.
        # [scheduler]
        # send_messages = True
        # NOTE: send_messages is 'True' by default.
        return luigi.rpc.RemoteScheduler(url)

    def create_worker(
        self, scheduler, worker_id=None, worker_processes=1, assistant=False
    ):
        return luigi.worker.Worker(
            scheduler=scheduler, worker_id=worker_id,
            worker_processes=worker_processes, assistant=assistant
        )


# main classes
class LogListenerProcess(multiprocessing.Process):
    """
    Log listener which is run in a separate process.
    """
    log_queue = None

    status = None

    def __init__(self, config, log_queue):
        multiprocessing.Process.__init__(self)
        # the daemon flag has no effect as Python will send a SIGTERM to all
        # daemon processes during the sys.exit() call, which is however ignored
        # by the LogListenerProcess (@see LogListenerProcess.configure())
        self.daemon = True

        self.configure = functools.partial(self.configure, config=config)
        self.log_queue = log_queue

        self.status = Status(PENDING)

    def configure(self, config):
        # NOTE: configurations set by the parent process are also visible after
        # forking, e.g. signal handling or logger configurations.
        # NOTE: process related configurations should be set after forking -
        # otherwise they are also applies to the parent process.

        # NOTE: signals such as SIGINT and SIGTERM which are sent to the
        # main process also affects all child processes within the same process
        # group as the main process.
        signal.signal(signal.SIGINT, signal.SIG_IGN)
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        # signal.signal(signal.SIGUSR2, self.pause)

        lgrunnerd_logger = logging.getLogger('lgrunnerd')
        # we do not propagate log messages to the root logger which can be set
        # via luigi_logging.conf and would result in dupplicating log messages
        lgrunnerd_logger.propagate = False
        lgrunnerd_logger.setLevel(logging.INFO)

        formatstr = (
            '%(asctime)s %(name)s[%(process)s] %(levelname)s: %(message)s'
        )
        formatter = logging.Formatter(formatstr)

        fh = logging.FileHandler(config["lgrunnerd"]["log_file"])
        fh.setLevel(logging.INFO)
        fh.setFormatter(formatter)
        lgrunnerd_logger.addHandler(fh)

        if config.getboolean('lgrunnerd', 'background') is not True:
            sh = logging.StreamHandler(sys.stdout)
            sh.setLevel(logging.INFO)
            sh.setFormatter(formatter)
            lgrunnerd_logger.addHandler(sh)

        luigi.setup_logging.InterfaceLogging.setup(
            core_config(
                logging_conf_file=config.get('core', 'logging_conf_file', '')
            )
        )

    def run(self):
        self.status.set(RUNNING)

        # not to affect configurations from the parent process, we ran
        # LogListenerProcess.configure() after the process was forked
        self.configure()

        while self.status.get() == RUNNING:
            try:
                time.sleep(1)

                # flush all messages - even if an interrupt was sent
                # during time.sleep(1)
                while not self.log_queue.empty():
                    record = self.log_queue.get()
                    logger = logging.getLogger(record.name)
                    logger.handle(record)
            except Exception as ex:
                logger = logging.getLogger('lgrunnerd')
                logger.error(ex, exc_info=True)

    def pause(self, signum=signal.SIGTERM, stack=None):
        raise Exception('Not yet implemented!')

    def stop(self, signum=signal.SIGTERM, stack=None):
        self.status.set(SUSPENDED)


class WorkflowProcess(multiprocessing.Process):
    """
    Luigi workflow which is run in a separate process.
    This class effectively runs the Luigi framework and loggs the worker's
    summary. Furthermore, the stop() method tries to gracefully stop running
    Luigi tasks instead of killing them.
    """
    scheduler = None
    worker_id = None
    worker = None

    status = None

    logger = None

    def __init__(self, scheduler, worker_id, worker, config, tasks):
        multiprocessing.Process.__init__(self)

        self.scheduler = scheduler
        self.worker_id = worker_id
        self.worker = worker

        self.status = Status(PENDING)

        success = True
        for task in tasks:
            success &= self.worker.add(
                task,
                config.getboolean("core", "parallel_scheduling"),
                config.getint("core", "parallel_scheduling_processes")
            )

        if not success:
            self.status = Status(FAILED)
            raise Exception('Scheduling of tasks failed!')

    def configure(self):
        # NOTE: configurations set by the parent process are also visible after
        # forking, e.g. signal handling or logger configurations.
        # NOTE: process related configurations should be set after forking -
        # otherwise they are also applies to the parent process.

        # NOTE: signals such as SIGINT and SIGTERM which are sent to the
        # main process also affects all child processes within the same process
        # group as the main process.
        # NOTE2: signal handlers set by a parent process are inherited by the
        # child processes.
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        # signal.signal(signal.SIGUSR2, self.pause)

        # as we inherit the luigi-interface logger, incl. a QueueHandler, from
        # the parent process, we do not have to add any additional handler here
        self.logger = logging.getLogger('luigi-interface')
        self.logger.setLevel(logging.INFO)

    def run(self):
        self.status.set(RUNNING)

        # not to affect configurations from the parent process, we ran
        # WorkflowProcess.configure() after the process was forked
        self.configure()

        with self.worker:
            exit_code = 0 if self.worker.run() == True else 1
            # the summary is only available within the workers context manager.
            # the same code within the luigi process would not report any
            # completed tasks but only pending ones...
            self.logger.info(luigi.execution_summary.summary(self.worker))
            
            self.status.set(DONE if exit_code == 0 else FAILED)
            sys.exit(exit_code)

    def pause(self, signum=signal.SIGTERM, stack=None):
        raise Exception('Not yet implemented!')

    def stop(self, signum=signal.SIGTERM, stack=None):
        if self.status.get() != RUNNING:
            # only react on interrupt/terminal signals once
            # ignore further signals
            return
        
        # we ignore any signum here but send a SIGUSR1 to the process itself.
        # luigi workers which obtain a SIGUSR1 signal will stop requesting
        # new work and finish running pending tasks.
        os.kill(os.getpid(), signal.SIGUSR1)

        task_names = [
            task_name for task_name, task in self.scheduler.task_list().items()
            if self.worker_id in task['workers']
        ]
        for task_name in task_names:
            result = self.scheduler.send_scheduler_message(
                self.worker_id, task_name, "SIGTERM"
            )
            message_id = result["message_id"]

        # NOTE: we cannot kill the WorkflowProcess here as this would NOT
        # kill the spawned task processes - which would remain working
        
        self.status.set(SUSPENDED)


class LuigiProcess(multiprocessing.Process):
    """
    Luigi workflow manager which is run in a separate process.
    The class initializes and manages a WorkflowProcess.
    Notifications from the WorkflowProcess are forwarded to the
    remote tracking system while status updates on the corresponding remote job
    are monitored. Status updates such as pause/stop will perform the
    corresponding actions on WorkflowProcess.
    In addition, an error counter tracks the number of errors raised by the
    Luigi framework and may force the WorkflowProcess to stop if the
    errors exceed a given threshold.
    """
    scheduler = None
    worker_id = None
    worker = None
    job = None

    config = None

    error_counter = None
    warning_counter = None

    status = None
    execution_time = None

    logger = None

    def __init__(self, scheduler, worker_scheduler_factory, config, job):
        multiprocessing.Process.__init__(self)

        self.scheduler = scheduler
        self.worker_id = 'worker_%s' % job["id"]
        self.worker = worker_scheduler_factory.create_worker(
            scheduler=scheduler, worker_id=self.worker_id,
            worker_processes=config.getint("core", "workers"),
            assistant=config.getboolean("core", "assistant")
        )
        self.job = job
        self.config = config

        self.error_counter = Counter()
        self.warning_counter = Counter()

        self.status = Status(PENDING)
        self.execution_time = IntValue(-1)

    @contextmanager
    def configure(self):
        # NOTE: configurations set by the parent process are also visible after
        # forking, e.g. signal handling or logger configurations.
        # NOTE: process related configurations should be set after forking -
        # otherwise they are also applies to the parent process.

        # Note: signals such as SIGINT and SIGTERM which are sent to the
        # main process also affects all child processes within the same process
        # group as the main process.
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        # signal.signal(signal.SIGUSR2, self.pause)

        # as we inherit the luigi-interface logger, incl. a QueueHandler, from
        # the parent process, we do not have to add any additional handler here
        self.logger = logging.getLogger('luigi-interface')
        self.logger.setLevel(logging.INFO)

        tracking_url = "http://%s:%s" % (
            self.config["tracking"]["default-tracking-host"],
            self.config["tracking"]["default-tracking-port"]
        )

        # load modules with luigi workflows and tasks
        # NOTE: each luigi process loads the modules individualy which
        # allows e.g. the usage of shared data structures or random initialized
        # class attributes. if we would load the modules e.g. within the
        # ServiceRunner we would shared the same data structures and class
        # attributes among all workflow executions (!)
        if self.config["lgrunnerd"]["modules"] == '':
            pass  # no modules to import
        else:
            for module in self.config["lgrunnerd"]["modules"].split(','):
                importlib.import_module(module, package=None)

        # register event handlers
        # NOTE: all handlers are registered on the luigi.Task class
        # and are hence applied to all Luigi tasks
        luigi.Task.event_handler(luigi.Event.BROKEN_TASK)(
            functools.partial(
                on_broken_task,
                tracking_url=tracking_url, job_id=self.job['id']
            )
        )
        luigi.Task.event_handler(luigi.Event.FAILURE)(
            functools.partial(
                on_failure,
                tracking_url=tracking_url, job_id=self.job['id']
            )
        )
        luigi.Task.event_handler("event.lgrunner.status.notification")(
            functools.partial(
                on_status_notification,
                tracking_url=tracking_url, job_id=self.job['id']
            )
        )
        luigi.Task.event_handler("event.lgrunner.progress.notification")(
            functools.partial(
                on_progress_notification,
                tracking_url=tracking_url, job_id=self.job['id']
            )
        )
        luigi.Task.event_handler("event.lgrunner.log.notification")(
            functools.partial(
                on_log_notification,
                tracking_url=tracking_url, job_id=self.job['id'],
                warning_counter=self.warning_counter,
                error_counter=self.error_counter
            )
        )

        yield

        # cleanup
        pass

    def run(self):
        self.status.set(RUNNING)

        # not to affect configurations from the parent process, we ran
        # LuigiProcess.configure() after the process was forked
        with self.configure():
            tracking_url = "http://%s:%s" % (
                self.config["tracking"]["default-tracking-host"],
                self.config["tracking"]["default-tracking-port"]
            )

            try:
                # update job status
                try:
                    self.job["status"] = "in progress"
                    self.job["progress"] = 0
                    update_remote_job(
                        tracking_url, self.job,
                        headers={'If-Match': self.job.get('etag', None)}
                    )
                except Exception as ex:
                    if ex.response.status_code == 412:
                        self.logger.info(
                            (
                                "Job '%s' at '%s' was already modified by another "
                                "process!\nSkip execution."
                            ) % (
                                self.job["id"], ex.request.url
                            )
                        )
                        self.status.set(DONE)
                        sys.exit(0)
                    else:
                        self.status.set(FAILED)
                        raise
                
                workflow_name = '%s.%s' % (
                    self.job["namespace"],
                    self.job["name"]
                )
                workflow_params = {}
                for param in self.job["parameters"]:
                    if param["type"] == 0:  # integer
                        workflow_params[param["name"]] = int(param["value"])
                    if param["type"] == 1:  # float
                        workflow_params[param["name"]] = float(param["value"])
                    if param["type"] == 2:  # string
                        workflow_params[param["name"]] = param["value"]
                    if param["type"] == 3:  # boolean
                        workflow_params[param["name"]] = (
                            param["value"].lower() == "true"
                        )
                    if param["type"] == 4:  # datetime
                        workflow_params[param["name"]] = param["value"]

                self.logger.info(
                    "submit workflow '%s'." % workflow_name
                )
                self.logger.info("    parameters: %s" % workflow_params)

                workflow_class = luigi.task_register.Register.get_task_cls(
                    workflow_name
                )

                start_time = time.time()

                try:
                    workflow_process = WorkflowProcess(
                        self.scheduler, self.worker_id, self.worker, self.config,
                        [workflow_class(**workflow_params)]
                    )
                    self.logger.info('done scheduling tasks!')
                except Exception as ex:
                    self.logger.error(ex, exc_info=True)
                    self.status.set(FAILED)
                    sys.exit(-1)

                workflow_process.start()
                time.sleep(1)  # wait until all tasks are submitted

                # monitor the workflow process
                while workflow_process.exitcode is None:
                    if self.status.get() == RUNNING:
                        # check for stop criteria
                        if self.error_counter.value > 100:
                            self.stop()

                        if (
                            fetch_remote_job(
                                tracking_url, self.job['id']
                            )['status'] == 'stopping'
                        ):
                            self.stop()

                        time.sleep(1)

                    if self.status.get() == SUSPENDED:
                        workflow_process.stop()
                        # wait for termination of all luigi tasks
                        workflow_process.join()

                end_time = time.time()
                self.execution_time.set(int(end_time - start_time))

                # update job status
                status = (
                    "completed" if workflow_process.exitcode == 0 else "failed"
                )
                if (
                    self.error_counter.value > 0 or self.warning_counter.value > 0
                ):
                    status += " [%s]" % (','.join([
                        "%s=%s" % (event, occurrences)
                        for event, occurrences in (
                            ("Errors", self.error_counter.value),
                            ("Warnings", self.warning_counter.value)
                        )
                        if occurrences > 0
                    ]))

                self.job["status"] = status
                self.job["progress"] = 100
                # job["duration"] = end_time - start_time
                # job["exit_code"] = exit_code
                update_remote_job(
                    tracking_url, self.job
                )
                
                self.status.set(DONE if workflow_process.exitcode == 0 else FAILED)
                sys.exit(workflow_process.exitcode)

            except BaseException as ex:
                if isinstance(ex, SystemExit):
                    raise  # ignore exception raised by sys.exit()
                if isinstance(ex, requests.exceptions.RequestException):
                    if isinstance(ex, requests.exceptions.ConnectionError):
                        self.logger.error(
                            "Could not connect to url '%s'!" % ex.request.url
                        )
                    elif isinstance(ex, requests.exceptions.HTTPError):
                        self.logger.error(
                            "Unexpected response for url '%s': [%s] %s!" % (
                                ex.request.url, ex.response.status_code,
                                ex.response.reason
                            )
                        )
                    else:
                        self.logger.error(
                            "Unexpected error while calling url '%s'!" % (
                                ex.request.url
                            )
                        )
                else:
                    self.logger.error(ex, exc_info=True)

                self.status.set(FAILED)
                sys.exit(-1)

    def pause(self, signum=signal.SIGTERM, stack=None):
        raise Exception('Not yet implemented!')

    def stop(self, signum=signal.SIGTERM, stack=None):
        self.status.set(SUSPENDED)


class ServiceRunner(object):
    """
    Service runner which is run in the main process.
    The class shares the same structure as a multiprocess.Process but does NOT
    inherit from it.
    The class searches for new remote jobs and initializes a new LuigiProcess
    if one was found.
    """
    scheduler = None
    worker_scheduler_factory = None

    config = None

    status = None

    logger = None

    def __init__(self, config):
        self.config = config

        self.status = Status(PENDING)

    def configure(self):
        # sets the signal handling for the main process
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)
        # signal.signal(signal.SIGUSR2, self.pause)

        # initialize logger
        # NOTE: the log level is passed on to the children processes
        self.logger = logging.getLogger('lgrunnerd')
        self.logger.setLevel(logging.INFO)

        self.logger.info("lgrunnerd: read configurations...")

        # set default configurations for resources
        if self.config.getint('resources', 'memory', 0) == 0:
            # use all available memory (in MB)
            self.config.set(
                'resources', 'memory',
                str(int(psutil.virtual_memory().available / 1024 / 1024))
            )

        self.worker_scheduler_factory = WorkerSchedulerFactory()

        if self.config.getboolean('core', 'local_scheduler', True) is True:
            self.scheduler = (
                self.worker_scheduler_factory.create_local_scheduler()
            )
        else:
            scheduler_url = 'http://{host}:{port:d}/'.format(
                host=self.config.get(
                    'core', 'default-scheduler-host', 'localhost'
                ),
                port=self.config.getint(
                    'core', 'default-scheduler-port', 8082
                ),
            )
            self.scheduler = (
                self.worker_scheduler_factory.create_remote_scheduler(
                    url=scheduler_url
                )
            )

        # set resources
        for resource in self.config['resources']:
            self.scheduler.update_resource(
                resource, int(self.config['resources'][resource])
            )

        # summarize configurations
        self.logger.info("----------------------")
        self.logger.info("configurations:")
        for section in self.config.sections():
            self.logger.info("    %s" % section.upper())
            self.logger.info("    %s" % dict(self.config[section]))
        self.logger.info("----------------------")
        self.logger.info("lgrunnerd: initialization completed!")

    def run(self):
        self.status.set(RUNNING)

        self.configure()

        tracking_url = "http://%s:%s" % (
            self.config["tracking"]["default-tracking-host"],
            self.config["tracking"]["default-tracking-port"]
        )
        running_luigi_processes = []

        while self.status.get() == RUNNING:
            # clean-up terminated luigi processes
            for luigi_process_idx in reversed(
                range(len(running_luigi_processes))
            ):
                luigi_process = running_luigi_processes[luigi_process_idx]
                if luigi_process.exitcode is not None:
                    self.logger.info(
                        "    %s after %s seconds",
                        (
                            "succeeded" if luigi_process.exitcode == 0
                            else "failed"
                        ),
                        luigi_process.execution_time.get()
                    )
                    del running_luigi_processes[luigi_process_idx]

            if (
                len(running_luigi_processes) ==
                self.config.getint('lgrunnerd', 'luigi_processes')
            ):
                # we already run the maximum number of parallel luigi processes
                time.sleep(10)
                continue

            try:
                # request unprocessed luigi-workflow jobs
                # TODO: add limitation to ask only one task a time
                job_queue = fetch_remote_jobs(tracking_url)

                if len(job_queue) == 0:
                    # nothing to do...
                    time.sleep(10)
                    continue

                for job in job_queue[
                    0:(
                        self.config.getint('lgrunnerd', 'luigi_processes') -
                        len(running_luigi_processes)
                    )
                ]:
                    self.logger.info(
                        "run luigi process for workflow '%s'..." % '%s.%s' % (
                            job["namespace"],
                            job["name"]
                        )
                    )
                    self.logger.info("    parameters: %s" % {
                            parameter['name']: parameter['value']
                            for parameter in job["parameters"]
                        }
                    )

                    luigi_process = LuigiProcess(
                        self.scheduler, self.worker_scheduler_factory,
                        self.config, job
                    )
                    luigi_process.start()
                    running_luigi_processes.append(luigi_process)

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
                            "unexpected exception while calling url '%s'!" % (
                                ex.request.url
                            )
                        )

                    time.sleep(10)
                    continue

                self.logger.error(ex, exc_info=True)
                self.stop()

        # cleanup
        self.logger.info("lgrunnerd: stopping")
        self.logger.info(
            "%s luigi processes to stop..." % len(running_luigi_processes)
        )

        if len(running_luigi_processes) > 0:
            for luigi_process in running_luigi_processes:
                luigi_process.stop()

            for luigi_process in running_luigi_processes:
                luigi_process.join()

            # alternative:

            # time.sleep(5)  # allow a gracefully termination within 5 seconds
            # for luigi_process in running_luigi_processes:
            #     if luigi_process.is_alive():
            #         os.kill(luigi_process.pid, signal.SIGKILL)

        self.status.set(DONE)
        self.logger.info("lgrunnerd: terminated")

    def start(self):
        self.run()

    def pause(self, signum=signal.SIGTERM, stack=None):
        raise Exception('Not yet implemented!')

    def stop(self, signum=signal.SIGTERM, stack=None):
        self.status.set(SUSPENDED)


def configure_logging(log_queue):
    """
    Function which initializes all loggers with a multiprocessing.Queue.
    :param log_queue: the queue for the QueueHandler
    :returns: None
    """
    logger = logging.getLogger('lgrunnerd')
    logger.setLevel(logging.INFO)
    qh = logging.handlers.QueueHandler(log_queue)
    qh.setLevel(logging.INFO)
    logger.addHandler(qh)

    logger = logging.getLogger('luigi-interface')
    logger.setLevel(logging.INFO)
    qh = logging.handlers.QueueHandler(log_queue)
    qh.setLevel(logging.INFO)
    logger.addHandler(qh)


def main(config):
    """
    Main function which runs a LogListenerProcess as well as the ServiceRunner.
    :param config: the lgrunner configurations
    :returns: None
    """
    # we ignore SIGINT and SIGTERM in the main process and let the
    # ServiceRunner handle them.
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

    # initialize a global log queue and log listener
    log_queue = multiprocessing.Queue(-1)  # inf size
    log_listener_process = LogListenerProcess(
        config, log_queue
    )
    log_listener_process.start()
    time.sleep(1)  # wait until the LogListenerProcess is running

    configure_logging(log_queue)

    service_runner = ServiceRunner(
        config
    )
    service_runner.start()

    # we have to manually stop the log listerne - even it's a daemon process.
    # multiprocessing registers a cleanup handler in os.exit() which sends
    # a SIGTERM to all daemon processes - which, however, is ignored by the
    # LogListenerProcess!
    log_listener_process.stop()
    log_listener_process.join()


if __name__ == "__main__":
    luigi.configuration.add_config_path('/etc/lgrunner.d/lgrunnerd.conf')
    config = luigi.configuration.get_config()

    if "--background" in sys.argv:
        config.set('lgrunnerd', 'background', "True")

    if config.getboolean('lgrunnerd', 'background') is True:
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
