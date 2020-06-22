import glob
import logging.handlers
import os
import psycopg2.pool
import queue
import shutil
import signal
import threading
import time
from configparser import ConfigParser
from processor.core.processor_webservice import ProcessorHTTPServer, ProcessorHTTPGetHandler
from processor.utils.mwa_archiving import pawsey_stage_files


class GenericObservationProcessor:
    def __init__(self, processor_name: str, config: ConfigParser, execute: bool):
        print(f"Initialising {processor_name}...")
        self.name = processor_name
        self.execute = execute
        self.terminate = False
        self.terminated = False

        # Setup signal handler
        signal.signal(signal.SIGINT, self.signal_handler)

        # Setup logging
        self.logger = logging.getLogger("processor")
        self.logger.setLevel(logging.DEBUG)
        self.logger.propagate = False

        # Setup log file logging
        if not os.path.isdir("logs"):
            print("No ./logs directory found. Creating directory.")
            os.mkdir("logs")

        if self.execute:
            self.log_filename = f"logs/{self.name}.log"
        else:
            self.log_filename = f"logs/{self.name}_dry_run.log"

        log_file_handler = logging.FileHandler(self.log_filename)
        log_file_handler.setLevel(logging.DEBUG)
        log_file_handler.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, [%(threadName)s], %(message)s"))
        self.logger.addHandler(log_file_handler)

        # Setup console logging
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, [%(threadName)s], %(message)s"))
        self.logger.addHandler(console_handler)

        self.logger.info("Reading configuration...")

        if config.get("processing", "log_level") == "DEBUG":
            self.logger.setLevel(logging.DEBUG)
        elif config.get("processing", "log_level") == "INFO":
            self.logger.setLevel(logging.INFO)
        else:
            print("Error: log_level in config file must be DEBUG or INFO. Exiting.")
            exit(-1)

        # Database connection config
        self.mro_metadata_db_host = config.get("mro_metadata_db", "host")
        self.mro_metadata_db_port = config.getint("mro_metadata_db", "port")
        self.mro_metadata_db_name = config.get("mro_metadata_db", "db")
        self.mro_metadata_db_user = config.get("mro_metadata_db", "user")
        self.mro_metadata_db_pass = config.get("mro_metadata_db", "pass")

        self.ngas_db_host = config.get("ngas_db", "host")
        self.ngas_db_port = config.getint("ngas_db", "port")
        self.ngas_db_name = config.get("ngas_db", "db")
        self.ngas_db_user = config.get("ngas_db", "user")
        self.ngas_db_pass = config.get("ngas_db", "pass")

        # Staging config
        self.staging_host = config.get("staging", "dmget_host")
        self.staging_port = config.getint("staging", "dmget_port")
        self.staging_retry_attempts = config.getint("staging", "retry_attempts")
        self.staging_backoff_seconds = config.getint("staging", "backoff_seconds")

        # Web server config
        self.web_service_port = config.get("web_service", "port")

        # Get generic processor options
        self.implements_per_item_processing = config.getint("processing", "implements_per_item_processing")
        self.concurrent_processes = config.getint("processing", "concurrent_processes")

        if config.has_option("processing", "working_path"):
            self.working_path = config.get("processing", "working_path")

            # Check working path
            self.check_root_working_directory_exists()

            # Clean working path
            self.cleanup_root_working_directory()
        else:
            self.working_path = None

        # Initialise stuff
        self.logger.info("Initialising...")

        if self.execute:
            self.logger.warning("** EXECUTE is true. Will make changes to data! **")
        else:
            self.logger.info("DRY RUN. No data will be changed")

        # Queues
        self.observation_queue = queue.Queue()
        self.observation_item_queue = queue.Queue()

        # Stats / info
        self.stats_observations_to_process = 0
        self.stats_observations_processed_successfully = 0
        self.stats_observations_failed_with_errors = 0

        self.current_observations = []
        self.current_observation_items = []
        self.successful_observation_items = []

        self.num_items_to_process = 0
        self.num_items_processed_successfully = 0

        # Threads
        self.consumer_threads = []

        # Web service
        self.web_server = None
        self.web_server_thread = None

        # Locks
        # This is used to lock the queue while we refresh the list of observations to process
        self.observation_queue_lock = threading.Lock()

        # Create pools
        self.logger.info("Setting up database pools...")
        self.mro_metadata_db_pool = psycopg2.pool.ThreadedConnectionPool(0, 10,
                                                                         user=self.mro_metadata_db_user,
                                                                         password=self.mro_metadata_db_pass,
                                                                         host=self.mro_metadata_db_host,
                                                                         port=self.mro_metadata_db_port,
                                                                         database=self.mro_metadata_db_name)
        self.ngas_db_pool = psycopg2.pool.ThreadedConnectionPool(0, 10,
                                                                 user=self.ngas_db_user,
                                                                 password=self.ngas_db_pass,
                                                                 host=self.ngas_db_host,
                                                                 port=self.ngas_db_port,
                                                                 database=self.ngas_db_name)

    def stage_files(self, obs_id: int, file_list: list, retry_attempts: int = 3, backoff_seconds: int = 30) -> bool:
        # The passed in list of files should be Pawsey DMF paths.
        # The dmget daemon just needs filenames so sanitise it first
        filename_list = []

        for file in file_list:
            # Ensure item is a full file path. Split it so we get the filename only
            if os.path.isabs(file):
                filename_list.append(os.path.split(file)[1])
            else:
                self.log_error(obs_id, f"Staging failed: stage_files() requires absolute paths, not just filenames.")

        #
        # Stage the files on the Pawsey filesystem
        #
        attempts = 1

        t1 = time.time()

        # While we have attempts left and we are not shutting down
        while attempts <= retry_attempts and self.terminate is False:
            if attempts == 1:
                self.log_info(obs_id, f"Staging {len(filename_list)} files from tape... ")
            else:
                self.log_info(obs_id, f"Staging {len(filename_list)} files from tape... "
                                         f"(attempt {attempts}/{retry_attempts})")

            try:
                # Staging will return 0 on success or non zero on staging error or raise exception on other failure
                exit_code = pawsey_stage_files(filename_list,
                                               self.staging_host,
                                               self.staging_port)
                t2 = time.time()

                if exit_code == 0:
                    self.log_info(obs_id, f"Staging {len(filename_list)} files from tape complete "
                                          f"({t2 - t1:.2f} seconds).")
                    return True
                else:
                    self.log_error(obs_id, f"Staging failed with return code {exit_code}.")
            except Exception as staging_error:
                self.log_error(obs_id, f"Staging failed with other exception condition {str(staging_error)}.")

            self.log_debug(obs_id, f"Backing off {backoff_seconds} seconds trying to stage data.")
            time.sleep(backoff_seconds)
            attempts += 1

        # If we get here, we give up
        if not self.terminate:
            t2 = time.time()
            self.log_error(obs_id, f"Staging failed too many times. Gave up after trying {retry_attempts} "
                                   f"times and {t2 - t1:.2f} seconds.")
        return False

    def check_root_working_directory_exists(self) -> bool:
        if self.working_path:
            self.logger.info(f"Checking and cleaning working path {self.working_path}...")
            if not os.path.exists(self.working_path):
                self.logger.error(f"Working path specified in configuration {self.working_path} is not valid.")
                exit(-1)
            else:
                return True
        else:
            raise Exception("check_root_working_directory_exists(): working_path is not defined in the config "
                            "file.")

    def get_working_filename_and_path(self, obs_id: int, filename: str) -> str:
        # will return working_path/obs_id/filename
        if self.working_path:
            return os.path.join(self.working_path, str(obs_id), filename)
        else:
            raise Exception("get_working_filename_and_path(): working_path is not defined in the config "
                            "file.")

    def cleanup_root_working_directory(self):
        if self.working_path:
            path_to_clean = os.path.join(self.working_path, "*")

            if os.path.exists(self.working_path):
                self.logger.debug(f"Removing contents of {self.working_path}...")

                # Remove any files/folders in there
                files = glob.glob(path_to_clean)

                for f in files:
                    if os.path.isfile(f):
                        self.logger.debug(f"Deleting file {f}")
                        os.remove(f)
                    else:
                        self.logger.debug(f"Deleting folder {f}")
                        shutil.rmtree(f)
            else:
                # Nothing to do
                pass
        else:
            raise Exception("cleanup_root_working_directory(): working_path is not defined in the config "
                            "file.")

    def prepare_observation_working_directory(self, obs_id: int):
        if self.working_path:
            obs_id_str = str(obs_id)

            path_to_create = os.path.join(self.working_path, obs_id_str)

            if os.path.exists(path_to_create):
                # The folder exists, lets delete it and it's contents just in case
                self.cleanup_observation_working_directory(obs_id)

            # Now Create directory
            os.mkdir(path_to_create, int(0o755))
            self.log_debug(obs_id, f"Created directory {path_to_create}...")
        else:
            raise Exception("prepare_observation_working_directory(): working_path is not defined in the config "
                            "file.")

    def cleanup_observation_working_directory(self, obs_id: int):
        if self.working_path:
            obs_id_str = str(obs_id)
            path_to_clean = os.path.join(self.working_path, obs_id_str)

            if os.path.exists(path_to_clean):
                self.log_debug(obs_id, f"Removing directory and contents of {path_to_clean}...")

                # Remove any files in there
                shutil.rmtree(path_to_clean)
            else:
                # Nothing to do
                pass
        else:
            raise Exception("cleanup_observation_working_directory(): working_path is not defined in the config "
                            "file.")

    def log_info(self, obs_id: int, message: str):
        self.logger.info(f"{obs_id}: {message}")

    def log_warning(self, obs_id: int, message: str):
        self.logger.warning(f"{obs_id}: {message}")

    def log_debug(self, obs_id: int, message: str):
        self.logger.debug(f"{obs_id}: {message}")

    def log_error(self, obs_id: int, message: str):
        self.logger.error(f"{obs_id}: {message}")

    def log_exception(self, obs_id: int, message: str):
        self.logger.exception(f"{obs_id}: {message}")

    #
    # Virtual methods
    #
    def get_observation_list(self) -> list:
        # This is to be supplied by the inheritor. Using SQL or whatever to get a list of obs_ids
        raise NotImplementedError()

    def process_one_observation(self, obs_id: int) -> bool:
        # This is to be supplied by the inheritor.
        # Any staging (for small obs) should be done by the inheritor
        raise NotImplementedError()

    def get_observation_item_list(self, obs_id: int) -> list:
        # This is to be supplied by the inheritor. We pass in the list of staged files for the current obs_id
        # If implements_per_item_processing == 0 then just 'pass'
        raise NotImplementedError()

    def process_one_item(self, obs_id, item: str) -> bool:
        # This is to be supplied by the inheritor.
        # If implements_per_item_processing == 0 then just 'pass'
        # For large (e.g. VCS) observations, inheritor should stage files here
        raise NotImplementedError()

    def end_of_observation(self, obs_id: int) -> bool:
        # This is to be supplied by the inheritor.
        # If implements_per_item_processing == 0 then just 'pass'
        raise NotImplementedError()

    def web_server_loop(self, web_server):
        web_server.serve_forever()

    def get_status(self) -> str:
        status = f"Running {self.concurrent_processes} tasks. " \
                 f"Observations remaining: {self.observation_queue.qsize()}/{self.stats_observations_to_process}"

        if self.implements_per_item_processing:
            status = f"{status} Processing: {self.current_observations} > " \
                     f"{[os.path.split(c)[1] if len(os.path.split(c)) == 2 else c for c in self.current_observation_items]}\n"
        else:
            status = f"{status} Processing: {self.current_observations}\n"

        return status

    def refresh_obs_list(self):
        self.logger.info(f"Refreshing observation list...")

        # Acquire lock
        self.observation_queue_lock.acquire(timeout=30)

        # Get a new list
        new_list = self.get_observation_list()

        new_item_count = 0

        if len(new_list) > 0:
            # Enqueue any missing items
            for new_item in new_list:
                if new_item not in self.observation_queue.queue:
                    if new_item not in self.current_observations:
                        self.observation_queue.put(new_item)
                        new_item_count += 1
        else:
            # Nothing to do
            pass

        # Release lock
        self.observation_queue_lock.release()

        self.logger.info(f"Refreshing observation list complete. {new_item_count} new observations.")

    # main loop
    def main_loop(self):
        # Create and start web server
        self.logger.info(f"Starting http server on port {self.web_service_port}...")
        self.web_server = ProcessorHTTPServer(('', int(self.web_service_port)), ProcessorHTTPGetHandler)
        self.web_server.context = self
        self.web_server_thread = threading.Thread(name='webserver',
                                                  target=self.web_server_loop,
                                                  args=(self.web_server,))
        self.web_server_thread.setDaemon(True)
        self.web_server_thread.start()

        # Get observations to process
        observation_list = []

        try:
            observation_list = self.get_observation_list()
        except Exception:
            self.logger.exception("Exception in get_observation_list()")

        self.stats_observations_to_process = len(observation_list)

        # Do we have any observations to process?
        if self.stats_observations_to_process > 0:
            # Enqueue them
            for observation in observation_list:
                self.observation_queue.put(observation)

            # Do processing
            if self.implements_per_item_processing:
                self.logger.info(f"A single observation and {self.concurrent_processes} concurrent items "
                                 f"will be processed.")

                # Loop through obs_id's one by one
                # execute process_one_observation
                # then kick off x number of consumers of files simultaneously
                try:
                    while self.terminate is False:
                        # Get next item, but only if the queue is not locked
                        self.observation_queue_lock.acquire(timeout=30)

                        # Now get next item
                        obs_id = self.observation_queue.get_nowait()

                        # Release lock
                        self.observation_queue_lock.release()

                        # keep track of what we are working on
                        self.current_observations.append(obs_id)

                        self.log_info(obs_id, "Started")

                        # Prepare a working area for this observation
                        if self.working_path:
                            self.prepare_observation_working_directory(obs_id)

                        observation_result = False

                        # give the caller a chance to do something on the observation itself
                        try:
                            observation_result = self.process_one_observation(obs_id)
                        except Exception:
                            self.log_exception(obs_id, "Exception in process_one_observation()")

                        if observation_result:
                            # now get the list of items to process
                            observation_item_list = self.get_observation_item_list(obs_id)
                            self.num_items_to_process = len(observation_item_list)
                            self.num_items_processed_successfully = 0
                            self.successful_observation_items = []

                            # Enqueue items into the queue
                            for item in observation_item_list:
                                self.observation_item_queue.put(item)

                            # process each file
                            if self.observation_item_queue.qsize() > 0:
                                for thread_id in range(self.concurrent_processes):
                                    new_thread = threading.Thread(name=f"t{thread_id+1}",
                                                                  target=self.observation_item_consumer,
                                                                  args=(obs_id,))
                                    self.consumer_threads.append(new_thread)

                                # Start all threads
                                for thread in self.consumer_threads:
                                    thread.start()

                                self.observation_item_queue.join()

                            self.logger.debug("Item Queue empty. Cleaning up...")

                            # Cancel the consumers- these will be idle now anyway
                            for thread in self.consumer_threads:
                                thread.join()

                            # Reset consumer theads list for next loop
                            self.consumer_threads = []

                            # Now finalise the observation, if need be
                            if self.end_of_observation(obs_id):
                                self.stats_observations_processed_successfully = \
                                    self.stats_observations_processed_successfully + 1
                            else:
                                self.stats_observations_failed_with_errors = self.stats_observations_failed_with_errors + 1

                        # Tell queue that job is done
                        self.observation_queue.task_done()

                        # Cleanup any temp files in working area
                        if self.working_path:
                            self.cleanup_observation_working_directory(obs_id)

                        # Update stats
                        self.current_observations.remove(obs_id)

                        self.log_info(obs_id, "Complete")
                except queue.Empty:
                    self.logger.debug(f"Queue empty")

                    # Release lock
                    self.observation_queue_lock.release()

            else:
                self.logger.info(f"{self.concurrent_processes} concurrent observations will be processed.")

                # Kick off x number of consumers of observations simultaneously
                for thread_id in range(self.concurrent_processes):
                    new_thread = threading.Thread(name=f"t{thread_id+1}", target=self.observation_consumer)
                    self.consumer_threads.append(new_thread)

                # Start all threads
                for thread in self.consumer_threads:
                    thread.start()

                # Wait for all consumers to finish
                self.observation_queue.join()

                self.logger.debug("Queue empty. Cleaning up...")

                # Cancel the consumers- these will be idle now anyway
                for thread in self.consumer_threads:
                    thread.join()

        # Stopped
        self.logger.debug("main() stopped. ")

        # Uncomment this if there are loose threads!
        for thread in threading.enumerate():
           if thread.name != "MainThread":
               self.logger.debug(f"Thread: {thread.name} is still running, but it shouldn't be.")

    def observation_consumer(self) -> bool:
        try:
            while self.terminate is False:
                # Get next item, but only if the queue is not locked
                self.observation_queue_lock.acquire(timeout=30)

                # Get next item, now that the lock is released
                obs_id = self.observation_queue.get_nowait()

                # Release lock
                self.observation_queue_lock.release()

                # Keep track of what we are working on
                self.current_observations.append(obs_id)

                self.log_info(obs_id, "Started")

                # Prepare a working area
                if self.working_path:
                    self.prepare_observation_working_directory(obs_id)

                # process the observation
                try:
                    if self.process_one_observation(obs_id):
                        self.stats_observations_processed_successfully = self.stats_observations_processed_successfully + 1
                    else:
                        self.stats_observations_failed_with_errors = self.stats_observations_failed_with_errors + 1

                except Exception:
                    self.log_exception(obs_id, "Exception in process_one_observation()")

                # Tell queue that job is done
                self.observation_queue.task_done()

                # Cleanup any temp files in working area
                if self.working_path:
                    self.cleanup_observation_working_directory(obs_id)

                # Update stat on what we're working on
                self.current_observations.remove(obs_id)

                self.log_info(obs_id, "Complete")

        except queue.Empty:
            self.logger.debug(f"Queue empty")

            # Release lock
            self.observation_queue_lock.release()

        return True

    def observation_item_consumer(self, obs_id: int) -> bool:
        try:
            self.log_debug(obs_id, f"Task Started")

            while self.terminate is False:
                # Get next item
                item = self.observation_item_queue.get_nowait()

                # Update stats
                self.current_observation_items.append(item)

                try:
                    if self.process_one_item(obs_id, item):
                        self.num_items_processed_successfully += 1
                        self.successful_observation_items.append(item)

                except Exception:
                    self.log_exception(obs_id, f"{item}: Exception in process_one_item()")
                finally:
                    # Tell queue that job is done
                    self.observation_item_queue.task_done()

                    # Update stats
                    self.current_observation_items.remove(item)

        except queue.Empty:
            self.log_debug(obs_id, "Queue empty")

        # Mark this task as done
        self.log_debug(obs_id, "Task Complete")
        return True

    def start(self):
        self.logger.info(f"{self.name} started")

        # Setup main loop
        self.main_loop()

        # Stop everything
        self.stop()

        self.logger.info(f"{self.name} stopped")

    def stop(self):
        self.terminate = True

        if not self.terminated:
            if self.logger:
                self.logger.info("Stopping processor...")
            else:
                print("Stopping processor...")

            # Wait until current tasks are done- then clear the queue so we get past queue.join
            if len(self.consumer_threads) > 0:
                self.logger.debug(f"Waiting for {len(self.consumer_threads)} tasks to complete...")

            while len(self.consumer_threads) > 0:
                for c in self.consumer_threads:
                    if not c.is_alive():
                        self.logger.debug(f"{c.name} has stopped.")
                        self.consumer_threads.remove(c)
                    time.sleep(0.1)

            # Now they are all stopped, we can dequeue the remaining items and cleanup
            self.logger.debug("Clearing queues")
            while self.observation_item_queue.qsize() > 0:
                self.observation_item_queue.get_nowait()
                self.observation_item_queue.task_done()

            while self.observation_queue.qsize() > 0:
                self.observation_queue.get_nowait()
                self.observation_queue.task_done()

            # Close pools
            self.logger.debug(f"Closing database pools")
            if self.mro_metadata_db_pool:
                self.mro_metadata_db_pool.closeall()

            if self.ngas_db_pool:
                self.ngas_db_pool.closeall()

            # End the web server
            self.logger.info("Webserver stopping...")
            self.web_server.shutdown()
            self.logger.debug("Webserver stopped. ")

            self.logger.info(f"Total Observations to process      : {self.stats_observations_to_process}")
            self.logger.info(f"Observations attempted             : {self.stats_observations_processed_successfully + self.stats_observations_failed_with_errors}")
            self.logger.info(f"Observations processed successfully: {self.stats_observations_processed_successfully}")
            self.logger.info(f"Observations failed with errors    : {self.stats_observations_failed_with_errors}")
            self.terminated = True

    def signal_handler(self, sig, frame):
        if self.logger:
            self.logger.info("Interrupted!")
        else:
            print("Interrupted!")

        self.stop()
