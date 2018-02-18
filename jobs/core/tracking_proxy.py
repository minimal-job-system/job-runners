#!/usr/bin/env python3

import logging
from threading import Thread
import multiprocessing
import requests
import Queue


class TrackingProxy(Thread):
    logger = logging.getLogger('luigi-interface')
    queue = multiprocessing.Queue(100)
    
    is_running = False
    
    def __init__(self, *args, **kwargs):
        super(TrackingProxy, self).__init__(*args, **kwargs)
    
    def run(self):
        while self.is_running:
            try:
                message = self.queue.get(block=True, timeout=1)
                
                response = requests.get(
                    "%s/api/jobs/?id=%s" % (
                        message["tracking_url"], message["tracking_id"]
                    ),
                    headers={'content-type': 'application/json'}
                )
                response.raise_for_status()

                jobs = response.json()
                if len(jobs) == 0:
                    raise Exception(
                        "No job found for id '%s'!" %
                        message["tracking_id"]
                    )
                elif len(jobs) == 1:
                    job = jobs[0]
                    
                    if message["command_id"] == "set_status":
                        job["status"] = message["message"]
                    if message["command_id"] == "set_progress":
                        job["progress"] = message["progress"]
                    if message["command_id"] == "add_progress":
                        job["progress"] += message["progress"]
                    if message["command_id"] == "sub_progress":
                        job["progress"] -= message["progress"]
                    
                    requests.put(
                        "%s/api/jobs/%s/" % (
                            message["tracking_url"], message["tracking_id"]
                        ),
                        json=job
                    )
                else:
                    raise Exception(
                        "Multiple jobs found for id '%s'!" %
                        message["tracking_id"]
                    )
            except Queue.Empty:
                pass
            except Exception as ex:
                self.logger.warning(str(ex))
                pass
    
    def start(self):
        self.is_running = True
        super(TrackingProxy, self).start()
    
    def stop(self):
        self.is_running = False
    
    def set_status(self, tracking_url, tracking_id, message):
        self.queue.put(
            {
                "tracking_url": tracking_url, "tracking_id": tracking_id,
                "command_id": "set_status", "message": message
            }
        )
    
    def set_progress(self, tracking_url, tracking_id, percentage):
        self.queue.put(
            {
                "tracking_url": tracking_url, "tracking_id": tracking_id,
                "command_id": "set_progress", "progress": percentage
            }
        )
    
    def add_progress(self, tracking_url, tracking_id, percentage):
        self.queue.put(
            {
                "tracking_url": tracking_url, "tracking_id": tracking_id,
                "command_id": "add_progress", "progress": percentage
            }
        )
    
    def sub_progress(self, tracking_url, tracking_id, percentage):
        self.queue.put(
            {
                "tracking_url": tracking_url, "tracking_id": tracking_id,
                "command_id": "sub_progress", "progress": percentage
            }
        )
