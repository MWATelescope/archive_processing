import argparse
import os
from configparser import ConfigParser
from archive_processing.core.generic_observation_processor import (
    GenericObservationProcessor,
)
from archive_processing.core.observation import Observation
from archive_processing.utils.mwa_metadata import (
    MWADataQualityFlags,
    get_observations_marked_for_delete,
    get_obs_data_files_filenames_except_ppds,
    set_mwa_data_files_deleted_timestamp,
    update_mwa_setting_dataquality,
)


class DeleteProcessor(GenericObservationProcessor):
    def __init__(
        self, processor_name: str, config: ConfigParser, execute: bool
    ):
        super().__init__(processor_name, config, execute)

        # Lists to keep track of files
        self.mwa_file_list = []

    def get_observation_list(self) -> list:
        self.logger.info("Getting list of observations...")

        observation_list = get_observations_marked_for_delete(
            self.mro_metadata_db_pool
        )

        self.logger.info(f"{len(observation_list)} observations to process.")
        return observation_list

    def process_one_observation(self, observation: Observation) -> bool:
        self.log_info(
            observation.obs_id,
            f"Starting... ({self.observation_queue.qsize()} remaining in"
            " queue)",
        )
        return True

    def get_observation_item_list(self, observation: Observation) -> list:
        self.log_info(observation.obs_id, "Getting list of files...")

        self.mwa_file_list = []

        # Get MWA file list
        try:
            self.mwa_file_list = get_obs_data_files_filenames_except_ppds(
                self.mro_metadata_db_pool,
                observation.obs_id,
                deleted=False,
                remote_archived=True,
            )
        except Exception:
            self.log_exception(
                observation.obs_id, "Could not get mwa data files for obs_id."
            )
            return []

        if len(self.mwa_file_list) == 0:
            self.log_info(
                observation.obs_id, "No data files found in mwa database"
            )
            return []

        return self.mwa_file_list

    def process_one_item(self, observation: Observation, item) -> bool:
        # items in this case are the acacia or banksia files

        if self.execute:
            # rm the file- if it's already gone, we are ok with that.
            try:
                os.remove(item)
                self.log_info(
                    observation.obs_id,
                    f"{item}: deleted from filesystem."
                    f" ({observation.observation_item_queue.qsize()} remaining"
                    " in queue)",
                )
            except FileNotFoundError:
                self.log_info(
                    observation.obs_id,
                    f"{item}: does not exist, continuing."
                    f" ({observation.observation_item_queue.qsize()} remaining"
                    " in queue)",
                )
        else:
            self.log_info(
                observation.obs_id,
                f"{item}: Would have rm from filesystem."
                f" ({observation.observation_item_queue.qsize()} remaining in"
                " queue)",
            )
        return True

    def end_of_observation(self, observation: Observation) -> bool:
        # This file was successfully processed
        if (
            observation.num_items_to_process
            == observation.num_items_processed_successfully
        ):
            self.log_info(
                observation.obs_id,
                "All items for this observation processed successfully. "
                "Updating data_files & mwa_setting...",
            )

            if (
                len(observation.successful_observation_items)
                != observation.num_items_processed_successfully
            ):
                self.log_error(
                    observation.obs_id,
                    "Mismatch between successful count "
                    f"({observation.num_items_processed_successfully}) and "
                    f"list ({len(observation.successful_observation_items)})",
                )
                return False

            # Update metadata database to set deletedtimestamp for all files
            # deleted
            try:
                # Postgres limits us to 32K and it would make sense to do
                # smaller batches here anyway
                # Especially for VCS observations with >100K files to update
                file_to_update_count = len(self.mwa_file_list)
                file_index = 0
                batch_size = 2500

                # Keep processing batches until we have nothing left to process
                while file_to_update_count > 0:
                    batch_mwa_file_list = []

                    # The last batch is unlikely to fit evenly into the batch
                    # size, so this caters for that last iteration were items
                    # to process is < batch size
                    if file_to_update_count < batch_size:
                        batch_size = file_to_update_count

                    # Go and create a new batch
                    for batch_index in range(batch_size):
                        batch_mwa_file_list.append(
                            self.mwa_file_list[file_index]
                        )
                        file_index += 1

                    try:
                        if self.execute:
                            set_mwa_data_files_deleted_flag(
                                self.mro_metadata_db_pool,
                                batch_mwa_file_list,
                                observation.obs_id,
                            )
                            self.log_info(
                                observation.obs_id,
                                f"Updated {len(batch_mwa_file_list)} rows in"
                                " data_files table, setting deleted flag to"
                                " True.",
                            )
                        else:
                            self.log_info(
                                observation.obs_id,
                                "Would have updated"
                                f" {len(batch_mwa_file_list)} data_files rows,"
                                " setting deleted flag to True."
                                f" {batch_mwa_file_list}",
                            )

                        file_to_update_count -= batch_size
                    except Exception as e:
                        self.log_exception(
                            observation.obs_id,
                            "Error updating data_files to be set to deleted"
                            f" ({e})",
                        )
                        return False
            except Exception as ee:
                self.log_exception(
                    observation.obs_id,
                    f"Error deleting files from ngas database ({ee})",
                )
                return False

            # Update metadata database to set data quality to DELETED
            try:
                if self.execute:
                    update_mwa_setting_dataquality(
                        self.mro_metadata_db_pool,
                        observation.obs_id,
                        MWADataQualityFlags.DELETED,
                    )

                    self.log_info(
                        observation.obs_id,
                        "Data quality of observation updated "
                        f"to {MWADataQualityFlags.DELETED.name} "
                        f"({MWADataQualityFlags.DELETED.value})",
                    )
                else:
                    self.log_info(
                        observation.obs_id,
                        "Would have updated data quality of observation "
                        f"to {MWADataQualityFlags.DELETED.name} "
                        f"({MWADataQualityFlags.DELETED.value}).",
                    )
            except Exception as e:
                self.log_exception(
                    observation.obs_id,
                    f"Error updating data quality of observation ({e})",
                )
                return False
        else:
            failed = (
                observation.num_items_to_process
                - observation.num_items_processed_successfully
            )
            self.log_warning(
                observation.obs_id,
                "Not all items for this observation processed successfully."
                " Skipping further"
                f" operations.({failed} of"
                f" {observation.num_items_to_process} failed)",
            )
            return False

        return True


def run():
    print("Starting DeleteProcessor...")

    # Get command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--cfg", type=str, action="store")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()

    cfg_file = None

    if args.cfg:
        cfg_file = args.cfg

        if not os.path.exists(cfg_file):
            print(
                "Error: argument --cfg must point to a configuration file."
                " Exiting."
            )
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
