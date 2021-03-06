import luigi
import os
import re


class CollectionHelperClass(object):
    """
    Helper class with functions for collecting data.
    """

    def data_path(self, search_path):
        """
        Searches for the data directory containing all relevant raw data to
        process. The name of the data path is expected to start with
        '<creation_date><owner><round_id><plate_id>'.
        Note: A single folder for 2D/3D scanns and two folders for an SF scan
              are expected. SF produces two folders, one [A] during the (fast)
              pre-scanning phase which detects ROIs and one [B] during the
              high-resolution scanning of ROIs. We expect as both, the creation
              date of A as well as its size is smaller then the one of B.

        :param search_path: the path to search in
        :returns: the folder path containing the raw data to process
        :raises RuntimeError: raises an exception if an unexpected file
                              structure was found.
        """
        data_paths = []
        for folder_name in os.listdir(search_path):
            folder_path = os.path.join(search_path, folder_name)
            if os.path.isdir(folder_path):
                if re.compile(r"^\d{6}\w{2}\w{3}").match(folder_name):
                    data_paths.append(folder_path)

        data_path = None
        if len(data_paths) == 0:
            raise RuntimeError(
                "No data folder found in directory '%s'!"
                % search_path
            )
        elif len(data_paths) == 1:  # found directory for 2D/3D data
            data_path = data_paths[0]
        elif len(data_paths) == 2:  # found directory for SF data
            if data_paths[0].split('_')[0] != data_paths[1].split('_')[0]:
                raise RuntimeError(
                    "Multiple data folders found in directory '%s'!"
                    % search_path
                )

            sf_roi_data_path = None
            sf_raw_data_path = None
            if (
                os.path.getmtime(data_paths[0]) >
                os.path.getmtime(data_paths[1])
            ):
                sf_roi_data_path = data_paths[1]
                sf_raw_data_path = data_paths[0]
            else:
                sf_roi_data_path = data_paths[0]
                sf_raw_data_path = data_paths[1]

            sf_roi_data_size = sum(
                os.path.getsize(os.path.join(sf_roi_data_path, f))
                for f in os.listdir(sf_roi_data_path)
                if os.path.isfile(os.path.join(sf_roi_data_path, f))
            )
            sf_raw_data_size = sum(
                os.path.getsize(os.path.join(sf_raw_data_path, f))
                for f in os.listdir(sf_raw_data_path)
                if os.path.isfile(os.path.join(sf_raw_data_path, f))
            )

            if sf_raw_data_size < sf_roi_data_size:
                raise RuntimeError(
                    "Found unexpected SF folder structure in '%s'!"
                    % search_path
                )
            data_path = sf_raw_data_path
        else:
            raise RuntimeError(
                "Multiple non-SF data folders found for directory '%s'!"
                % search_path
            )

        return data_path
