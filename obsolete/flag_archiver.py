import argparse
import glob
import os
import queue
from threading import Event
import time
from psycopg2.errors import UniqueViolation
from configparser import ConfigParser
from processor.core.generic_processor import GenericProcessor
from processor.utils.mwa_metadata import invalidate_metadata_cache, MWAFileTypeFlags
from processor.utils.mwa_archiving import archive_file


class FlagArchiver(GenericProcessor):
    def __init__(self, processor_name: str, config: ConfigParser, execute: bool):
        super().__init__(processor_name, config, execute)
        # This event allows us to wait for long intervals between cycles, but also alows it be cancelled
        # if the user kills the process, interrupts us
        self.exit = Event()

        # Set status
        self.current_status = "Initialising"

        # Where do look for files to archive
        self.flag_file_path = config.get("processing", "flag_file_path")

        # Ensure we have a working dir
        if self.flag_file_path is None:
            self.logger.error("flag_file_path must be supplied in the config file.")
            exit(-3)
        else:
            if not os.path.isdir(self.flag_file_path):
                self.logger.error(f"flag_file_path '{self.flag_file_path}' must be a valid directory.")
                exit(-4)

        # Ensure we have archive info
        if self.ngas_host is None or self.ngas_port is None or self.ngas_user is None or self.ngas_pass is None:
            self.logger.error(f"ngas details in the [archive] section of the config file are missing.")
            exit(-5)

        # Setup queue
        self.q = queue.Queue()

        # Current item we are processing
        self.stats_observations_processed_successfully = 0
        self.stats_observations_failed_with_errors = 0

    def get_file_list(self, watch_dir, watch_ext):
        pattern = os.path.join(watch_dir, "*" + watch_ext)
        files = glob.glob(pattern)
        return sorted(files)

    def scan_directory(self, watch_dir, watch_ext):
        self.current_status = f"Scanning {watch_dir} for *.{watch_ext} files"

        # Just loop through all files and add them to the queue
        self.logger.info(f"Scanning {watch_dir} for files matching {'*' + watch_ext}...")

        files = self.get_file_list(watch_dir, watch_ext)

        self.logger.info(f"Found {len(files)} files")

        for file in files:
            self.q.put(file)
            self.logger.info(f'Added {file} to queue')

    def process_queue(self) -> bool:
        success = False
        item = self.q.get(block=False, timeout=1)

        # Get the filename and take the first 10 chars as the obsid
        obs_id = os.path.split(item)[1][0:10]
        if not obs_id.isdigit():
            # Invalid filename!
            self.log_error(obs_id, f"{item} is not a valid MWA flag zip file. Should be obsid_flags.zip.")
            # Something is very wrong to have other files in here, so abort completely- we don't want to
            # archive non flag zips and we don't want to delete anything either!
            exit(-10)

        self.current_status = f"Processing {item}"
        self.log_info(obs_id, f"processing {item}...")

        start_time = time.time()

        # Check file exists (maybe someone deleted it?)
        if os.path.exists(item):
            # Archive the file
            self.log_info(obs_id, f"Archiving file {item}...")

            if self.execute:
                try:
                    archive_file(self.mro_metadata_db_pool,
                                 self.ngas_host, self.ngas_port, self.ngas_user, self.ngas_pass,
                                 obs_id, item, MWAFileTypeFlags.FLAG_FILE)

                    success = True

                except UniqueViolation as unique_exception:
                    self.log_warning(obs_id, f"Ignoring... Flags already archived for this obs_id. Full error: {str(unique_exception)}")
                    success = True

                except Exception as archive_exception:
                    self.log_exception(obs_id, str(archive_exception))
                    success = False
            else:
                self.log_info(obs_id, f"Would have executed SQL: insert into data_files...")
                self.log_info(obs_id, f"Would have executed NGAS QARCHIVE: {self.ngas_host}:{self.ngas_port}")
                success = True

            # Dequeue the item, but requeue if it was not successful
            self.q.task_done()

            if success:
                # remove file
                if self.execute:
                    # rm the file- if it's already gone, we are ok with that.
                    try:
                        os.remove(item)
                        self.log_info(obs_id, f"deleted {item} from filesystem.")
                    except FileNotFoundError:
                        self.log_warning(obs_id, f"{item} does not exist, continuing.")
                else:
                    self.log_info(obs_id, f"Would have deleted {item} from filesystem.")

                if self.execute:
                    try:
                        invalidate_metadata_cache(obs_id)
                    except Exception as e:
                        self.log_warning(obs_id, f"{e}")
                else:
                    self.log_info(obs_id, f"Would have invalidated metadata cache.")

                self.stats_observations_processed_successfully += 1
            else:
                success = False
                self.stats_observations_failed_with_errors += 1
                self.q.put(item)
        else:
            # Dequeue the item
            self.q.task_done()
            self.log_warning(obs_id, f"file {item} was moved or deleted. Queue size: {self.q.qsize()}")

        elapsed = time.time() - start_time
        self.log_info(obs_id, f"Complete. Queue size: {self.q.qsize()} Elapsed: {elapsed:.2f} sec")
        return success

    # main loop
    def main_loop(self):
        sleep_minutes = 10
        backoff_minutes = 60

        fail_count = 0

        # Process the queue
        while not self.exit.is_set() and not self.terminate:
            # Get all files in flag_file_path for archiving
            # And add them to a queue
            self.scan_directory(self.flag_file_path, ".zip")

            while not self.exit.is_set() and self.q.qsize() > 0 and not self.terminate:
                success = self.process_queue()

                if success:
                    fail_count = 0
                else:
                    fail_count += 1

                    if fail_count > 4:
                        self.logger.warning(f"Too many failures in a row, backing off for {backoff_minutes} minutes")
                        self.exit.wait(backoff_minutes * 60)
                        fail_count = 0

            # Sleep for 10 minutes
            self.current_status = f"Queue processed. Sleeping for {sleep_minutes} minutes."
            self.logger.info(f"Queue processed. Going to sleep for {sleep_minutes} minutes.")
            self.exit.wait(sleep_minutes * 60)

        self.logger.info("Main loop done!")

    def stop(self):
        self.exit.set()
        self.terminate = True

        if not self.terminated:
            self.current_status = "Stopping"
            if self.logger:
                self.logger.info("Stopping processor...")
            else:
                print("Stopping processor...")

            self.logger.debug("Clearing observation queue")
            while self.q.qsize() > 0:
                self.q.get_nowait()
                self.q.task_done()

            # Close pools
            self.logger.debug(f"Closing database pools")
            if self.mro_metadata_db_pool:
                try:
                    self.mro_metadata_db_pool.closeall()
                except Exception as mro_db_execption:
                    self.logger.warning(f"Non clean closing of MRO db pool {mro_db_execption}")

            # End the web server
            self.logger.info("Webserver stopping...")
            self.web_server.shutdown()
            self.logger.debug("Webserver stopped. ")

            self.logger.info(
                f"Observations attempted             : {self.stats_observations_processed_successfully + self.stats_observations_failed_with_errors}")
            self.logger.info(f"Observations processed successfully: {self.stats_observations_processed_successfully}")
            self.logger.info(f"Observations failed with errors    : {self.stats_observations_failed_with_errors}")
            self.terminated = True

    def get_status(self) -> str:
        status_text = None

        if self.current_status is None:
            status_text = "Unknown status"
        else:
            status_text = self.current_status
        return f"{status_text}. Successful: {self.stats_observations_processed_successfully} Error: {self.stats_observations_failed_with_errors}"


def run():
    print("Starting FlagArchiver...")

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
    processor = FlagArchiver("FlagArchiver", config, args.execute)

    # Initialise
    processor.start()
