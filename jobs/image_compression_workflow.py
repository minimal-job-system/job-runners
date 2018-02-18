#!/usr/bin/env python3

"""
image_compression_workflow.py:

Demo workflow for compressing image data located within a source folder.
The workflow itself is given as follows:

                       /> ImageCompressionTask \>
    ImageCollectonTask -> ImageCompressionTask -> ImageCompressionWorkflow
                       \> ...                  />

Output folders:
  out: intermediate results
  res: compressed images
"""

import logging
import luigi

from core.global_params import GlobalLuigiParams
from core.base_tasks import JobSystemWorkflow, TrackableTask

from tasks.image_compression_task import ImageCompressionTask


# Event Handler
@luigi.Task.event_handler(luigi.Event.FAILURE)
def on_failure(task, exception):
    """Will be called directly after a failed execution
       of `run` on any Task subclass (i.e. all luigi Tasks)
    """
    TrackableTask().set_status("failed")


# Workflow Definition
class ImageCompressionWorkflow(JobSystemWorkflow, TrackableTask):
    """
    Workflow for compressing images.
    """
    task_namespace = 'demo'

    source_path = luigi.Parameter(default="")
    """
    source path containing all images to compress.
    """
    recursive = luigi.BoolParameter(default=False)
    """
    indicates wherever to search recursively for image files.
    """
    
    def requires(self):
        for worker_id in range(GlobalLuigiParams().workers):
            yield ImageCompressionTask(
                source_path=self.source_path, recursive=self.recursive,
                worker_id=worker_id,
                progress_fraction=100.0/GlobalLuigiParams().workers
            )
