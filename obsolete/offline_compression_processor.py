import argparse
import os
from configparser import ConfigParser
from processor.core.generic_observation_processor import GenericObservationProcessor
from processor.utils.mwa_fits import is_fits_compressed, fits_compress
from processor.utils.mwa_metadata import MWAFileTypeFlags, MWADataQualityFlags, MWAModeFlags, get_obs_gpubox_filenames
from processor.utils.ngas_metadata import get_ngas_file_path_and_address_for_filename_list
from processor.utils.database import run_sql_get_many_rows


class OfflineCompressProcessor(GenericObservationProcessor):
    def __init__(self, processor_name: str, config: ConfigParser, execute: bool):
        super().__init__(processor_name, config, execute)
        self.last_uvcompress_obsid = 1095108744  # this is the last obsid which requires uvcompress
        self.last_uncompressed_obsid = 1178606168  # this (I think) is the last obsid which requires any compression

    def get_observation_list(self) -> list:
        self.logger.info(f"Getting list of observations...")

        observation_list = []

        sql = f"""SELECT s.starttime As obs_id 
                  FROM mwa_setting s
                  WHERE
                      mode = %s  
                  AND starttime > %s
                  AND starttime < %s
                  AND dataquality IN (%s, %s)
                  AND EXISTS (SELECT 1 
                              FROM data_files d 
                              WHERE d.observation_num = s.starttime
                              AND   d.deleted = False
                              AND   d.remote_archived = True
                              AND   d.filetype = %s) 
                  ORDER BY s.starttime LIMIT 2"""

        # Execute query
        params = (MWAModeFlags.HW_LFILES.value,
                  self.last_uvcompress_obsid,
                  self.last_uncompressed_obsid,
                  MWADataQualityFlags.GOOD.value,
                  MWADataQualityFlags.SOME_ISSUES.value,
                  MWAFileTypeFlags.GPUBOX_FILE.value, )
        results = run_sql_get_many_rows(self.mro_metadata_db_pool, sql, params)

        if results:
            observation_list = [r['obs_id'] for r in results]

        self.logger.info(f"{len(observation_list)} observations to process.")
        return observation_list

    def process_one_observation(self, obs_id) -> bool:
        self.log_info(obs_id, f"Starting processing ({self.observation_queue.qsize()} remaining in queue)")
        return True

    def get_observation_staging_file_list(self, obs_id) -> list:
        self.log_info(obs_id, f"Getting list of files...")

        # Get MWA file list
        try:
            mwa_file_list = get_obs_gpubox_filenames(self.mro_metadata_db_pool, obs_id)
        except Exception:
            self.log_exception(obs_id, "Could not get gpubox files for obs_id.")
            return []

        if len(mwa_file_list) == 0:
            self.log_error(obs_id, f"No gpubox files found in mwa database")
            return []

        # Get NGAS file list, based on the mwa metadata list
        try:
            ngas_file_list = get_ngas_file_path_and_address_for_filename_list(self.ngas_db_pool,
                                                                              mwa_file_list)
        except Exception:
            self.log_exception(obs_id, "Could not get ngas files for obs_id.")
            return []

        if len(ngas_file_list) == 0:
            self.log_error(obs_id, f"No gpubox files found in ngas database")
            return []

        if len(mwa_file_list) != len(ngas_file_list):
            self.log_error(obs_id, f"MWA gpubox file count {len(mwa_file_list)} does not match "
                                   f"ngas file count {len(ngas_file_list)}")
            return []

        return ngas_file_list

    def get_observation_item_list(self, obs_id: int) -> list:
        # Just return what we staged
        return self.get_observation_staging_file_list(obs_id)

    def process_one_item(self, obs_id, item) -> bool:
        # item is a full file path. Split it so we get the filename only
        filename = os.path.split(item)[1]

        self.log_info(obs_id, f"{filename}: Starting... ({self.observation_item_queue.qsize()} remaining in queue)")

        # Determine output filename
        output_filename = self.get_working_filename_and_path(obs_id, filename)

        # Compress
        if not is_fits_compressed(item):
            fits_compress(item, output_filename)

        # Archive

        # Update Metadata

        self.log_info(obs_id, f"{filename}: Complete.")
        return True

    def end_of_observation(self, obs_id) -> bool:
        # This file was successfully processed
        self.log_info(obs_id, f"Finalising observation starting...")

        self.log_info(obs_id, f"Finalising observation complete.")
        return True


def run():
    print("Starting offline compression processor...")

    # Get command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--cfg', type=str, action='store')
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()

    cfg_file = None

    if args.cfg:
        cfg_file = args.cfg

        if not os.path.exists(cfg_file):
            print("Error: argument --cfg must point to a configuration file. Exiting.")
            exit(-1)
    else:
        print("Error: argument --cfg is required. Exiting.")
        exit(-2)

    # Read config file
    config = ConfigParser()
    config.read(cfg_file)

    # Create class instance
    processor = OfflineCompressProcessor("offline_compress_processor", config, args.execute)

    # Initialise
    processor.start()
