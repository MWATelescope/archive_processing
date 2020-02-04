from core.generic_observation_processor import GenericObservationProcessor

import argparse
import asyncio
from configparser import ConfigParser
import os


class TestProcessor(GenericObservationProcessor):
    def __init__(self, processor_name, config):
        super().__init__(processor_name, config)

    async def get_observation_list(self):
        self.logger.info(f"Getting list of observations...")
        return ["1234567890", "1234567891", "1234567892", "1234567893", "1234567894", ]

    async def process_one_observation(self, obs_id, task_id):
        self.log_info(obs_id, task_id, f"Processing observation..")
        return True

    async def get_observation_file_list(self, obs_id, task_id):
        self.log_info(obs_id, task_id, f"Getting list of files...")
        file_list = ["testfile", ]
        self.log_info(obs_id, task_id, f"{len(file_list)} files to process.")
        return file_list

    async def process_one_file(self, obs_id, task_id, filename):
        self.log_info(obs_id, task_id, f"{filename}: processing starting.")
        await asyncio.sleep(5)
        self.log_info(obs_id, task_id, f"{filename}: processing complete.")
        return True

    async def end_of_observation(self, obs_id, task_id):
        # This file was successfully processed
        self.log_info(obs_id, task_id, f"Observation complete.")
        return True


def main():
    print("Starting TestProcessor...")

    # Get command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--cfg', type=str, action='store')
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
    processor = TestProcessor("TestProcessor", config)

    # Initialise
    processor.start()


if __name__ == "__main__":
    main()
