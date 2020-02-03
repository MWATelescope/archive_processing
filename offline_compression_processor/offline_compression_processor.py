from generic_observation_processor import GenericObservationProcessor

import asyncio
import asyncpg
from configparser import ConfigParser
from mantaray.mantaray.sql import ngas

import argparse
import os


class OfflineCompressProcessor(GenericObservationProcessor):
    async def get_observations_to_process(self, pool):
        self.logger.info("Getting list of observations...")
        return []

    async def process_one_observation(self, obs_id):
        # Get file list
        file_list = await self.retrieive_file_list(pool, obs_id)

        if file_list is None:
            return False

        # Stage files
        if not await self.stage_observation(obs_id, file_list):
            return False

        # Compress files
        if not await self.compress_files(obs_id, file_list):
            return False

        # This observation was successfully processed
        return True

    async def compress_files(self, obs_id, file_list):
        self.log_info(obs_id, f"Compressing {len(file_list)} files...")

        try:
            for filename in file_list:
                pass

        except Exception as compress_exception:
            self.log_exception(obs_id, "Exception in compress_file()", compress_exception)
            return False


def main():
    print("Starting offline compression processor...")

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
    processor = OfflineCompressProcessor("offline_compress_processor")

    # Initialise
    processor.start()


if __name__ == "__main__":
    main()
