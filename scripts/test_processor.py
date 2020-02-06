import argparse
from configparser import ConfigParser
from core.generic_observation_processor import GenericObservationProcessor
import os
import random
import time


class TestProcessor(GenericObservationProcessor):
    def __init__(self, processor_name, config):
        super().__init__(processor_name, config)

    def get_observation_list(self) -> list:
        self.logger.info(f"Getting list of observations...")
        observation_list = ["1234567890", "1234567891", "1234567892", "1234567893", "1234567894", ]
        self.logger.info(f"{len(observation_list)} observations to process.")
        return observation_list

    def long_running_task(self):
        a, b = 0, 1

        for i in range(random.randint(500000, 1000000)):
            nth = a + b
            a = b
            b = nth

    def process_one_observation(self, obs_id) -> bool:
        self.log_info(obs_id, f"Starting... ({self.observation_queue.qsize()} remaining in queue)")

        self.long_running_task()

        self.log_info(obs_id, f"Complete.")
        return True

    def get_observation_item_list(self, obs_id) -> list:
        self.log_info(obs_id, f"Getting list of files...")
        file_list = ["testfile1", "testfile2", "testfile3", "testfile4", "testfile5", "testfile6", "testfile7", ]
        self.log_info(obs_id, f"{len(file_list)} files to process.")
        return file_list

    def process_one_item(self, obs_id, item) -> bool:
        self.log_info(obs_id, f"{item}: Starting... ({self.observation_item_queue.qsize()} remaining in queue)")

        time.sleep(random.randint(2, 7))

        self.log_info(obs_id, f"{item}: Complete.")
        return True

    def end_of_observation(self, obs_id) -> bool:
        # This file was successfully processed
        self.log_info(obs_id, f"Finalising observation starting...")
        time.sleep(random.randint(1, 3))
        self.log_info(obs_id, f"Finalising observation complete.")
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
