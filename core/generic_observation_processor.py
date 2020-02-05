import logging.handlers
import os
import psycopg2.pool
import queue
import signal
import threading
import time


class GenericObservationProcessor:
    def __init__(self, processor_name, config):
        print(f"Initialising {processor_name}...")
        self.name = processor_name
        self.terminate = False

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

        self.log_filename = f"logs/{self.name}.log"
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

        # Stagin config
        self.staging_host = config.get("staging", "dmget_host")
        self.staging_port = config.getint("staging", "dmget_port")

        # Get generic processor options
        self.implements_per_file_processing = config.getint("processing", "implements_per_file_processing")
        self.concurrent_processes = config.getint("processing", "concurrent_processes")

        # Queues
        self.obs_queue = queue.Queue()
        self.file_queue = queue.Queue()
        self.files_to_process = 0
        self.files_processed_successfully = 0

        self.consumer_threads = []

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

    def log_info(self, obs_id, message):
        self.logger.info(f"{obs_id}: {message}")

    def log_debug(self, obs_id, message):
        self.logger.debug(f"{obs_id}: {message}")

    def log_error(self, obs_id, message):
        self.logger.error(f"{obs_id}: {message}")

    def log_exception(self, obs_id, message, exception):
        self.logger.error(f"{obs_id}: {message}. {str(exception)}")

    #
    # Virtual methods
    #
    def get_observation_list(self):
        # This is to be supplied by the inheritor. Using SQL or whatever to get a list of obs_ids
        raise NotImplementedError()

    def process_one_observation(self, obs_id):
        # This is to be supplied by the inheritor.
        raise NotImplementedError()

    def get_observation_file_list(self, obs_id):
        # This is to be supplied by the inheritor. Using SQL or whatever to get a list of files for the current obs_id
        # If implements_per_file_processing == 0 then just 'pass'
        raise NotImplementedError()

    def process_one_file(self, obs_id, filename):
        # This is to be supplied by the inheritor.
        # If implements_per_file_processing == 0 then just 'pass'
        raise NotImplementedError()

    def end_of_observation(self, obs_id):
        # This is to be supplied by the inheritor.
        # If implements_per_file_processing == 0 then just 'pass'
        raise NotImplementedError()

    # main loop
    def main_loop(self):
        # Get observations to process
        observation_list = self.get_observation_list()

        # Enqueue them
        for observation in observation_list:
            self.obs_queue.put(observation)

        # Do processing
        if self.implements_per_file_processing:
            self.logger.info(f"{self.concurrent_processes} concurrent files will be processed.")

            # Loop through obs_id's one by one then kick off x number of consumers of files simultaneously
            try:
                self.logger.debug(f"Task Started")

                while self.terminate is False:
                    # Get next item
                    obs_id = self.obs_queue.get_nowait()

                    # process the observation- get the list of files
                    observation_file_list = self.get_observation_file_list(obs_id)
                    self.files_to_process = len(observation_file_list)
                    self.files_processed_successfully = 0

                    # Enqueue items into the queue
                    for filename in observation_file_list:
                        self.file_queue.put(filename)

                    # process each file
                    for thread_id in range(self.concurrent_processes):
                        new_thread = threading.Thread(name=f"t{thread_id+1}", target=self.observation_file_consumer,
                                                      args=(obs_id,))
                        self.consumer_threads.append(new_thread)

                    # Start all threads
                    for thread in self.consumer_threads:
                        thread.start()

                    self.file_queue.join()

                    self.logger.debug("File Queue empty. Cleaning up...")

                    # Cancel the consumers- these will be idle now anyway
                    for thread in self.consumer_threads:
                        thread.join()

                    # Reset consumer theads list for next loop
                    self.consumer_threads = []

                    # Now finalise the observation, if need be (and only if all were processed successfully)
                    if self.files_processed_successfully == self.files_to_process:
                        self.end_of_observation(obs_id)

                    # Tell queue that job is done
                    self.obs_queue.task_done()
            except queue.Empty:
                self.logger.debug(f"Queue empty")

            self.logger.debug(f"Task Complete")
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
            self.obs_queue.join()

            self.logger.debug("Queue empty. Cleaning up...")

            # Cancel the consumers- these will be idle now anyway
            for thread in self.consumer_threads:
                thread.join()

        # Close pools
        if self.mro_metadata_db_pool:
            self.mro_metadata_db_pool.closeall()

        if self.ngas_db_pool:
            self.ngas_db_pool.closeall()

    def observation_consumer(self):
        try:
            self.logger.debug(f"Task Started")

            while self.terminate is False:
                # Get next item
                obs_id = self.obs_queue.get_nowait()

                # process the observation
                self.process_one_observation(obs_id)

                # Tell queue that job is done
                self.obs_queue.task_done()
        except queue.Empty:
            self.logger.debug(f"Queue empty")

        # Mark this task as done
        self.logger.debug(f"Task Complete")
        return True

    def observation_file_consumer(self, obs_id):
        try:
            self.log_debug(obs_id, f"Task Started")

            while self.terminate is False:
                # Get next item
                filename = self.file_queue.get_nowait()

                if self.process_one_file(obs_id, filename):
                    self.files_processed_successfully += 1

                # Tell queue that job is done
                self.file_queue.task_done()
        except queue.Empty:
            self.log_debug(obs_id, "Queue empty")

        # Mark this task as done
        self.log_debug(obs_id, "Task Complete")
        return True

    def start(self):
        self.logger.info(f"{self.name} started")

        # Setup main loop
        self.main_loop()

        self.logger.info(f"{self.name} stopped")

    def stop(self):
        self.terminate = True

        # Wait until current tasks are done- then clear the queue so we get past queue.join
        self.logger.debug(f"Waiting for {len(self.consumer_threads)} tasks to complete...")
        while len(self.consumer_threads) > 0:
            for c in self.consumer_threads:
                if not c.is_alive():
                    self.consumer_threads.remove(c)
                time.sleep(0.1)

        # Now they are all stopped, we can dequeue the remaining items
        self.logger.debug("Clearing queues")
        while self.file_queue.qsize() > 0:
            self.file_queue.get_nowait()
            self.file_queue.task_done()

        while self.obs_queue.qsize() > 0:
            self.obs_queue.get_nowait()
            self.obs_queue.task_done()

    def signal_handler(self, sig, frame):
        if self.logger:
            self.logger.info("Interrupted! Closing down...")
        else:
            print("Interrupted! Closing down...")

        self.stop()
