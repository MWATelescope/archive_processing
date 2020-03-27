import argparse
from configparser import ConfigParser
from core.generic_observation_processor import GenericObservationProcessor
import core.mwa_metadata
import os
import base64
import time


class DeleteProcessor(GenericObservationProcessor):
    def __init__(self, processor_name: str, config: ConfigParser, execute: bool):
        super().__init__(processor_name, config, execute)

        # Read delete processor specific config
        self.ngas_username = config.get("archive", "ngas_user")
        self.ngas_password = base64.b64decode(config.get("archive", "ngas_pass")).decode("UTF-8")

    def stage_files(self, obs_id, file_list, retry_attempts, backoff_seconds) -> bool:
        # We don't want to stage anything
        return True

    def get_observation_list(self) -> list:
        self.logger.info(f"Getting list of observations...")

        observation_list = []

        sql = f"""SELECT obs.starttime As obs_id 
                  FROM mwa_setting As obs 
                  WHERE 
                   obs.dataquality = %s 
                   AND obs.mode IN ('HW_LFILES', 'VOLTAGE_START', 'VOLTAGE_BUFFER')    
                   AND obs.dataqualitycomment IS NOT NULL
                    AND obs.starttime >= 1076867192
                  ORDER BY obs.starttime ASC limit 2"""

        # Execute query
        params = (core.mwa_metadata.MWADataQualityFlags.MARKED_FOR_DELETE.value,)
        results = core.mwa_metadata.run_sql_get_many_rows(self.mro_metadata_db_pool, sql, params)

        if results:
            observation_list = [r['obs_id'] for r in results]
        else:
            return observation_list

        self.logger.info(f"{len(observation_list)} observations to process.")
        return observation_list

    def process_one_observation(self, obs_id) -> bool:
        self.log_info(obs_id, f"Starting... ({self.observation_queue.qsize()} remaining in queue)")

        self.log_info(obs_id, f"Complete.")
        return True

    def get_observation_staging_file_list(self, obs_id) -> list:
        return []

    def get_observation_item_list(self, obs_id, staging_file_list) -> list:
        self.log_info(obs_id, f"Getting list of files...")

        # Get MWA file list
        try:
            mwa_file_list = core.mwa_metadata.get_obs_data_file_filenames(self.mro_metadata_db_pool,
                                                                          obs_id,
                                                                          deleted=False,
                                                                          remote_archived=True)
        except Exception:
            self.log_exception(obs_id, "Could not get data files for obs_id.")
            return []

        if len(mwa_file_list) == 0:
            self.log_error(obs_id, f"No data files found in mwa database")
            return []

        # Get NGAS file list, based on the mwa metadata list
        try:
            ngas_file_list = core.mwa_metadata.get_ngas_file_path_and_address_for_filename_list(self.ngas_db_pool,
                                                                                                mwa_file_list)
        except Exception:
            self.log_exception(obs_id, "Could not get ngas files for obs_id.")
            return []

        if len(ngas_file_list) == 0:
            self.log_error(obs_id, f"No data files found in ngas database")
            return []

        if len(mwa_file_list) != len(ngas_file_list):
            self.log_error(obs_id, f"MWA file count {len(mwa_file_list)} does not match "
                                   f"ngas file count {len(ngas_file_list)}")
            return []

        return ngas_file_list

    def process_one_item(self, obs_id, item) -> bool:
        if self.execute:
            # rm the file
            pass
        else:
            self.log_info(obs_id, f"{item}: Would have rm from filesystem. "
                                  f"({self.observation_item_queue.qsize()} remaining in queue)")
        return True

    def end_of_observation(self, obs_id) -> bool:
        # This file was successfully processed
        if self.num_items_to_process == self.num_items_processed_successfully:
            self.log_info(obs_id, f"All items for this observation processed successfully. "
                                  f"Deleting ngas_files, updating data_files & mwa_setting...")

            if len(self.successful_observation_items) != self.num_items_processed_successfully:
                self.log_error(obs_id, f"Mismatch between successful count "
                                       f"({self.num_items_processed_successfully}) and "
                                       f"list ({len(self.successful_observation_items)})")
                return False

            metadata_filenames = []

            for f in self.successful_observation_items:
                # f will be a filly qualified path
                # split it to just get the filename
                filename = os.path.split(f)[1]

                # NGAS may have multiple versions of a file, but
                # for this we only want the unique filenames which we will match
                # in the mwa metadata database
                if filename not in metadata_filenames:
                    metadata_filenames.append(filename)

            # Delete removed files from NGAS database
            if self.execute:
                pass
            else:
                self.log_info(obs_id, f"Would have deleted {len(self.successful_observation_items)} "
                                      f"rows from ngas_files table.")

            # Update metadata database to set deleted=True for all files deleted
            if self.execute:
                core.mwa_metadata.set_mwa_data_files_deleted_flag(self.mro_metadata_db_pool,
                                                                  metadata_filenames,
                                                                  obs_id)
            else:
                self.log_info(obs_id, f"Would have updated {len(metadata_filenames)} "
                                      f"data_files rows, setting deleted flag "
                                      f"to True.")

            # Update metadata database to set data quality to DELETED
            if self.execute:
                pass
            else:
                self.log_info(obs_id, f"Would have updated data quality of observation "
                                      f"to {core.mwa_metadata.MWADataQualityFlags.DELETED.name} "
                                      f"({core.mwa_metadata.MWADataQualityFlags.DELETED.value}).")
        else:
            self.log_warning(obs_id, f"Not all items for this observation processed successfully. "
                                     f"({self.num_items_to_process - self.num_items_processed_successfully} of "
                                     f"{self.num_items_to_process} failed)")
        return True

    def check_root_working_directory_exists(self):
        # Pass this check, we don't need to check the path since we don't use it
        return True

    def prepare_observation_working_directory(self, obs_id: int):
        # we don't use a working directory
        pass

    def cleanup_observation_working_directory(self, obs_id: int):
        # we don't use a working directory
        pass

    def cleanup_root_working_directory(self):
        pass


def main():
    print("Starting DeleteProcessor...")

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
    processor = DeleteProcessor("DeleteProcessor", config, args.execute)

    # Initialise
    processor.start()


if __name__ == "__main__":
    main()
