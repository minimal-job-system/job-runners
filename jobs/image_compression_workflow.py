#!/usr/bin/env python3

"""
image_compression_workflow.py:

Demo workflow for compressing image data located within a source folder.
The workflow itself is given as follows:

                       /> ImageCompressionTask \>
    ImageCollectionTask -> ImageCompressionTask -> ImageCompressionWorkflow
                       \> ...                  />

Output folders:
  out: intermediate results
  res: compressed images
"""

import logging
import luigi
import os
import pandas as pd

from core.base_tasks import JobSystemWorkflow

from tasks.image_collection_task import ImageCollectionTask
from tasks.image_compression_task import ImageCompressionTask


# Workflow Definition
class ImageCompressionWorkflow(JobSystemWorkflow):
    """
    Workflow for compressing images.
    """
    task_namespace = 'demo'

    source_path = luigi.Parameter()
    """
    source path containing all images to compress.
    """
    recursive = luigi.BoolParameter(default=False)
    """
    indicates wherever to search recursively for image files.
    """

    def requires(self):
        pass

    def run(self):
        image_collection_target = yield ImageCollectionTask(
            source_path=self.source_path,
            recursive=self.recursive
        )

        with image_collection_target.open('r') as fp:
            image_frame = pd.read_csv(fp, sep='\t', encoding='utf-8')
        group_ids= image_frame["group_id"].unique()

        image_compression_task_list = []
        for group_id in group_ids:
            image_compression_task_list.append(
                ImageCompressionTask(
                    source_path=self.source_path,
                    recursive=self.recursive,
                    group_id=group_id,
                    progress_fraction=100.0 / len(group_ids)
                )
            )
        yield image_compression_task_list
