#!/usr/bin/env python3

import logging
import luigi
import os
import pandas as pd
import re

from core.base_tasks import JobSystemTask


class ImageCollectionTask(JobSystemTask):
    task_namespace = 'demo'

    source_path = luigi.Parameter(default="")
    """
    source path containing all images to index.
    """
    recursive = luigi.BoolParameter(default=False)
    """
    indicates wherever to search recursively for image files.
    """
    regex = luigi.Parameter(default=".*\.(tif{1,2}|png|gif|bmp|jpg|jpeg)$")
    """
    regex to identify image files to index.
    """

    def requires(self):
        """
        Method which returns a list of tasks which have to exist before
        running ImageCollectonTask.

        :return: the list of required tasks
        """
        return None

    def run(self):
        """
        Main method.
        """
        image_frame = pd.DataFrame(columns=["file_path", "group_id"])
        for root, dirs, files in os.walk(self.source_path, topdown=True):
            if not self.recursive:
                dirs = []  # stop recursion

            for idx, file_name in enumerate(files):
                file_path = os.path.join(root, file_name)
                if re.match(self.regex, file_path):
                    image_frame = image_frame.append(
                        pd.DataFrame(
                            [(file_path, idx%4)],
                            columns=["file_path", "group_id"]
                        ),
                        ignore_index=True
                    )

        with self.output().open('w') as fp:
            image_frame.to_csv(fp, sep='\t', encoding='utf-8')

    def output(self):
        """
        Method which returns the task's target.

        :return: local target to file list
        """
        out_path = os.path.join(self.source_path, "out")
        if not os.path.exists(out_path):
            os.mkdir(out_path)

        return luigi.LocalTarget(os.path.join(out_path, "image_frame.csv"))
