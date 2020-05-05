import argparse
import os
import random
import time
from configparser import ConfigParser
from processor.core.generic_observation_processor import GenericObservationProcessor
from processor.utils.mwa_metadata import MWAModeFlags, MWADataQualityFlags
from processor.utils.ngas_metadata import get_ngas_all_file_paths
from processor.utils.database import run_sql_get_many_rows


class TestProcessor(GenericObservationProcessor):
    def __init__(self, processor_name: str, config: ConfigParser, execute: bool):
        super().__init__(processor_name, config, execute)
        self.observation_item_list = []

    def get_observation_list(self) -> list:
        self.logger.info(f"Getting list of observations...")
        observation_list = []

        # Run SQL
        sql = f"""SELECT s.starttime As obs_id 
                          FROM mwa_setting s
                          WHERE
                              mode = %s
                          AND dataquality = %s
                          AND starttime < %s 
                          ORDER BY starttime DESC LIMIT 2"""

        params = (MWAModeFlags.HW_LFILES.value, MWADataQualityFlags.GOOD.value, 1270000000)

        results = run_sql_get_many_rows(self.mro_metadata_db_pool, sql, params)

        if results:
            observation_list = [r['obs_id'] for r in results]

        self.logger.info(f"{len(observation_list)} observations to process.")

        return observation_list

    def long_running_task(self, obs_id: int, item: str = None):
        a, b = 0, 1

        if item:
            self.log_info(obs_id, f"{item}: Running long running task ({self.observation_item_queue.qsize()} "
                                  f"remaining in queue)")
        else:
            self.log_info(obs_id, "Running long running task")

        time.sleep(random.randint(1, 4))

        if item:
            self.log_info(obs_id, f"{item}: Long running task complete.")
        else:
            self.log_info(obs_id, "Long running task complete.")

    def process_one_observation(self, obs_id: int) -> bool:
        self.log_info(obs_id, f"Starting... ({self.observation_queue.qsize()} remaining in queue)")

        self.log_info(obs_id, f"Getting list of files to stage...")

        staging_file_list = get_ngas_all_file_paths(self.ngas_db_pool, obs_id)

        self.log_info(obs_id, f"{len(staging_file_list)} files to stage.")

        if self.stage_files(obs_id, staging_file_list):
            self.log_info(obs_id, f"Staging complete.")

            # We need to store the items for later, when get_observation_item_list() is called
            self.observation_item_list = staging_file_list

            if self.implements_per_item_processing == 0:
                # Run a long running task
                self.long_running_task(obs_id)
                return True
            else:
                return True
        else:
            self.log_info(obs_id, f"Cannot stage files. Skipping this observation.")
            return False

    def get_observation_item_list(self, obs_id: int) -> list:
        self.log_info(obs_id, f"Getting list of items...")
        self.log_info(obs_id, f"{len(self.observation_item_list)} items to process.")
        return self.observation_item_list

    def process_one_item(self, obs_id: int, item: str) -> bool:
        self.long_running_task(obs_id, item)
        return True

    def end_of_observation(self, obs_id: int) -> bool:
        # This file was successfully processed
        self.log_info(obs_id, f"Finalising observation starting...")
        time.sleep(random.randint(1, 3))
        self.log_info(obs_id, f"Finalising observation complete.")
        return True


def run():
    print("Starting TestProcessor...")

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
    processor = TestProcessor("TestProcessor", config, args.execute)

    # Initialise
    processor.start()
