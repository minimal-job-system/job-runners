#!/usr/bin/env python3

import logging
import luigi
import os
import pandas as pd
from PIL import Image

from core.global_params import GlobalLuigiParams
from core.base_tasks import TrackableTask
from core.helper import ParallelHelperClass

from image_collection_task import ImageCollectonTask


class ImageCompressionTask(TrackableTask, ParallelHelperClass):
    task_namespace = 'demo'

    source_path = luigi.Parameter(default="")
    """
    source path containing all images to compress.
    """
    recursive = luigi.BoolParameter(default=False)
    """
    indicates wherever to search recursively for image files.
    """
    worker_id = luigi.IntParameter(default=0)
    """
    ID of the worker which process the current task junk.
    """
    
    def requires(self):
        """
        Method which returns a list of tasks which have to exist before
        running ImageCompressionTask.

        :return: the list of required tasks
        """
        return ImageCollectonTask(
            source_path=self.source_path, recursive=self.recursive
        )

    def run(self):
        """
        Main method.
        """
        res_path = os.path.join(self.source_path, "res")
        if not os.path.exists(res_path):
            os.makedirs(res_path)
        
        # gather data
        with self.input().open('r') as fp:
            image_frame = pd.read_csv(fp, sep='\t', encoding='utf-8')
        image_id_partitions = self.split_seq(
            image_frame.index, GlobalLuigiParams().workers
        )
        worker_image_ids = list(image_id_partitions)[self.worker_id]
        
        processed_files = 0
        total_files = len(worker_image_ids)
        progress_fraction_per_file = (
            float(self.progress_fraction)/total_files if total_files > 0 else 0
        )
        
        compr_image_frame = pd.DataFrame(columns=["file_path"])
        for image_id in worker_image_ids:
            file_path = image_frame.loc[image_id]["file_path"]
            file_name = os.path.basename(file_path)
            if os.path.exists(
                os.path.join(res_path, file_name)
            ):
                continue

            try:
                image = Image.open(file_path)
                image.save(
                    os.path.join(res_path, file_name),
                    compression="tiff_lzw"
                )
                compr_image_frame = compr_image_frame.append(
                    pd.DataFrame(
                        [(os.path.join(res_path, file_name),)],
                        columns=["file_path"]
                    ),
                    ignore_index=True
                )
            except Exception as ex:
                logging.getLogger('luigi-interface').error(ex)
                continue
        
            processed_files += 1
            if processed_files % 100 == 0:
                self.add_progress(progress_fraction_per_file * 100)
        
        if total_files > 0:
            self.add_progress(
                progress_fraction_per_file * (processed_files % 100)
            )
        else:
            self.add_progress(self.progress_fraction)
        
        with self.output().open('w') as fp:
            compr_image_frame.to_csv(fp, sep='\t', encoding='utf-8')

    def output(self):
        """
        Method which returns the task's target.

        :return: local target to file list with compressed images
        """
        out_path = os.path.join(self.source_path, "out")
        if not os.path.exists(out_path):
            os.mkdir(out_path)
        
        return luigi.LocalTarget(
            os.path.join(
                out_path, "compr_image_frame_by_worker_%s.csv" % self.worker_id
            )
        )
