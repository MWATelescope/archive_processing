import argparse
import os
from configparser import ConfigParser
from processor.core.generic_observation_processor import GenericObservationProcessor
from processor.core.observation import Observation
from processor.utils.mwa_metadata import MWADataQualityFlags, get_observations_marked_for_delete, get_obs_data_files_filenames_except_ppds, set_mwa_data_files_deleted_flag, update_mwa_setting_dataquality
from processor.utils.ngas_metadata import get_all_ngas_file_path_and_address_for_filename_list, delete_ngas_files


class DeleteProcessor(GenericObservationProcessor):
    def __init__(self, processor_name: str, config: ConfigParser, execute: bool):
        super().__init__(processor_name, config, execute)

        # Lists to keep track of files
        self.mwa_file_list = []
        self.ngas_file_list = []

    def get_observation_list(self) -> list:
        self.logger.info(f"Getting list of observations...")

        observation_list = get_observations_marked_for_delete(self.mro_metadata_db_pool)

        self.logger.info(f"{len(observation_list)} observations to process.")
        return observation_list

    def process_one_observation(self, observation: Observation) -> bool:
        self.log_info(observation.obs_id, f"Starting... ({self.observation_queue.qsize()} remaining in queue)")
        return True

    def get_observation_item_list(self, observation: Observation) -> list:
        self.log_info(observation.obs_id, f"Getting list of files...")

        self.mwa_file_list = []
        self.ngas_file_list = []

        # Get MWA file list
        try:
            self.mwa_file_list = get_obs_data_files_filenames_except_ppds(self.mro_metadata_db_pool,
                                                                          observation.obs_id,
                                                                          deleted=False,
                                                                          remote_archived=True)
        except Exception:
            self.log_exception(observation.obs_id, "Could not get mwa data files for obs_id.")
            return []

        if len(self.mwa_file_list) == 0:
            self.log_info(observation.obs_id, f"No data files found in mwa database")
            return []

        # Get NGAS file list, based on the mwa metadata list
        try:
            self.ngas_file_list = get_all_ngas_file_path_and_address_for_filename_list(self.ngas_db_pool,
                                                                                       self.mwa_file_list)
        except Exception:
            self.log_exception(observation.obs_id, "Could not get ngas files for obs_id.")
            return []

        if len(self.ngas_file_list) == 0:
            self.log_info(observation.obs_id, f"No data files found in ngas database for this obs_id.")
            return []

        return self.ngas_file_list

    def process_one_item(self, observation: Observation, item) -> bool:
        # items in this case are the NGAS files

        if self.execute:
            # rm the file- if it's already gone, we are ok with that.
            try:
                os.remove(item)
                self.log_info(observation.obs_id, f"{item}: deleted from filesystem. "
                                      f"({observation.observation_item_queue.qsize()} remaining in queue)")
            except FileNotFoundError:
                self.log_info(observation.obs_id, f"{item}: does not exist, continuing. "
                                      f"({observation.observation_item_queue.qsize()} remaining in queue)")
        else:
            self.log_info(observation.obs_id, f"{item}: Would have rm from filesystem. "
                                  f"({observation.observation_item_queue.qsize()} remaining in queue)")
        return True

    def end_of_observation(self, observation: Observation) -> bool:
        # This file was successfully processed
        if observation.num_items_to_process == observation.num_items_processed_successfully:
            self.log_info(observation.obs_id, f"All items for this observation processed successfully. "
                                  f"Deleting ngas_files, updating data_files & mwa_setting...")

            if len(observation.successful_observation_items) != observation.num_items_processed_successfully:
                self.log_error(observation.obs_id, f"Mismatch between successful count "
                                       f"({observation.num_items_processed_successfully}) and "
                                       f"list ({len(observation.successful_observation_items)})")
                return False

            # Delete removed files from NGAS database db
            try:
                # Postgres limits us to 32K and it would make sense to do smaller batches here anyway
                # Especially for VCS observations with >100K files to delete
                file_to_delete_count = len(self.ngas_file_list)
                file_index = 0
                batch_size = 2500

                # Keep processing batches until we have nothing left to process
                while file_to_delete_count > 0:
                    batch_ngas_file_list = []

                    # The last batch is unlikely to fit evenly into the batch size, so
                    # this caters for that last iteration were items to process is < batch size
                    if file_to_delete_count < batch_size:
                        batch_size = file_to_delete_count

                    # Go and create a new batch
                    for batch_index in range(batch_size):
                        batch_ngas_file_list.append(os.path.basename(self.ngas_file_list[file_index]))
                        file_index += 1

                    try:
                        if self.execute:
                            delete_ngas_files(self.ngas_db_pool, batch_ngas_file_list)
                            self.log_info(observation.obs_id, f"Deleted {len(batch_ngas_file_list)} "
                                                              f"rows from ngas_files table.")
                        else:
                            self.log_info(observation.obs_id, f"Would have deleted {len(batch_ngas_file_list)} "
                                                              f"rows from ngas_files table. {batch_ngas_file_list}")

                        file_to_delete_count -= batch_size
                    except:
                        self.log_exception(observation.obs_id, f"Error deleting ngas_files")
                        return False
            except:
                self.log_exception(observation.obs_id, f"Error deleting files from ngas database")
                return False

            # Update metadata database to set deleted=True for all files deleted
            try:
                # Postgres limits us to 32K and it would make sense to do smaller batches here anyway
                # Especially for VCS observations with >100K files to update
                file_to_update_count = len(self.mwa_file_list)
                file_index = 0
                batch_size = 2500

                # Keep processing batches until we have nothing left to process
                while file_to_update_count > 0:
                    batch_mwa_file_list = []

                    # The last batch is unlikely to fit evenly into the batch size, so
                    # this caters for that last iteration were items to process is < batch size
                    if file_to_update_count < batch_size:
                        batch_size = file_to_update_count

                    # Go and create a new batch
                    for batch_index in range(batch_size):
                        batch_mwa_file_list.append(self.mwa_file_list[file_index])
                        file_index += 1

                    try:
                        if self.execute:
                            set_mwa_data_files_deleted_flag(self.mro_metadata_db_pool,
                                                            batch_mwa_file_list,
                                                            observation.obs_id)
                            self.log_info(observation.obs_id, f"Updated {len(batch_mwa_file_list)} "
                                                              f"rows in data_files table, setting deleted flag "
                                                              f"to True.")
                        else:
                            self.log_info(observation.obs_id, f"Would have updated {len(batch_mwa_file_list)} "
                                                              f"data_files rows, setting deleted flag "
                                                              f"to True. {batch_mwa_file_list}")

                        file_to_update_count -= batch_size
                    except:
                        self.log_exception(observation.obs_id, f"Error updating data_files to be set to deleted")
                        return False
            except:
                self.log_exception(observation.obs_id, f"Error deleting files from ngas database")
                return False

            # Update metadata database to set data quality to DELETED
            try:
                if self.execute:
                    update_mwa_setting_dataquality(self.mro_metadata_db_pool, observation.obs_id,
                                                   MWADataQualityFlags.DELETED)

                    self.log_info(observation.obs_id, f"Data quality of observation updated "
                                                      f"to {MWADataQualityFlags.DELETED.name} "
                                                      f"({MWADataQualityFlags.DELETED.value})")
                else:
                    self.log_info(observation.obs_id, f"Would have updated data quality of observation "
                                                      f"to {MWADataQualityFlags.DELETED.name} "
                                                      f"({MWADataQualityFlags.DELETED.value}).")
            except:
                self.log_exception(observation.obs_id, f"Error updating data quality of observation")
                return False
        else:
            self.log_warning(observation.obs_id, f"Not all items for this observation processed successfully. "
                                                 f"Skipping further operations."
                                                 f"({observation.num_items_to_process - observation.num_items_processed_successfully} of "
                                                 f"{observation.num_items_to_process} failed)")
            return False

        return True


def run():
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

