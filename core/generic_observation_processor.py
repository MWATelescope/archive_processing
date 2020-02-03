import logging.handlers
import asyncio
import queue
from mwa_archiving import pawsey_stage_files


class GenericObservationProcessor:
    def __init__(self, processor_name):
        self.name = processor_name

        # Setup logging
        self.logger = logging.getLogger('processor')
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

        # Setup log file logging
        self.log_filename = f"{self.name}"
        log_file_handler = logging.FileHandler(self.log_filename)
        log_file_handler.setLevel(logging.DEBUG)
        log_file_handler.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
        self.logger.addHandler(log_file_handler)

        # Setup console logging
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter('%(asctime)s, %(levelname)s, %(message)s'))
        self.logger.addHandler(console_handler)

        # Internal variables
        self.observation_list = []
        self.obs_queue = queue.Queue()

    def log_info(self, obs_id, message):
        self.logger.info(f"{obs_id}: {message}")

    def log_error(self, obs_id, message):
        self.logger.error(f"{obs_id}: {message}")

    def log_exception(self, obs_id, message, exception):
        self.logger.error(f"{obs_id}: {message}. {str(exception)}")

    async def retrieive_file_list(self, pool, obs_id):
        self.log_info(obs_id, f"Retrieving file list...")

        try:
            return await ngas.get_mwa_obs_files(pool, obs_id)
        except Exception as get_mwa_obs_files_exception:
            self.log_exception(obs_id, "Exception in ngas.get_mwa_obs_files()", get_mwa_obs_files_exception)
            return None

    async def stage_observation(self, obs_id, file_list, staging_host, staging_port):
        self.log_info(obs_id, f"Staging {len(file_list)} files...")

        try:
            return_code = await pawsey_stage_files(file_list, staging_host, staging_port)

            if return_code == 0:
                self.log_info(obs_id, "Staging successful")
                return True
            else:
                self.log_error(obs_id, f"Staging failed: return code {return_code}")
                return False

        except Exception as staging_exception:
            self.log_exception(obs_id, "Exception in stage.pawsey_stage_files()", staging_exception)
            return False

    def get_observation_list(self):
        # This is to be supplied by the inheritor. Using SQL or whatever to get a list of obs_ids
        raise NotImplementedError("get_observation_list() is not implemented by class.")

    def process_one_observation(self, obs_id):
        # This is to be supplied by the inheritor.
        raise NotImplementedError("process_one_observation() is not implemented by class.")

    def main_loop(self):
        while self.obs_queue.not_empty:
            # Get next item
            item = self.obs_queue.get()

            self.log_info(item, "processing...")

            self.process_one_observation()

            self.log_info(item, "complete.")

    def start(self):
        self.logger.info(f"{self.name} started")

        # Setup database connections
        self.logger.info("Setting up database pools...")

        # Setup main loop
        loop = asyncio.get_event_loop()
        app = loop.run_until_complete(main_loop())

        self.logger.info("Offline Compression Processor stopped")

