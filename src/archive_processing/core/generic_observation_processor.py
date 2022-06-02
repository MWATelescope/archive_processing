import os
import queue
import shutil
import threading
import time
from configparser import ConfigParser
from archive_processing.core.generic_processor import GenericProcessor
from archive_processing.core.observation import Observation
from archive_processing.utils.mwa_archiving import pawsey_stage_files


class GenericObservationProcessor(GenericProcessor):
    def __init__(
        self, processor_name: str, config: ConfigParser, execute: bool
    ):
        super().__init__(processor_name, config, execute)

        # Get generic processor options
        self.implements_per_item_processing = config.getint(
            "processing", "implements_per_item_processing"
        )
        self.concurrent_observations = config.getint(
            "processing", "concurrent_observations"
        )
        self.concurrent_items = config.getint("processing", "concurrent_items")

        # Queues
        self.observation_queue = queue.Queue()

        # Stats / info
        self.stats_obs_to_process = 0
        self.stats_obs_processed_good = 0
        self.stats_obs_failed_errors = 0

        self.current_observations = []

        # Threads
        self.consumer_threads = []

        # Locks
        # This is used to lock the queue while we refresh the list of
        # observations to process
        self.observation_queue_lock = threading.Lock()

    def stage_files(
        self,
        obs_id: int,
        file_list: list,
        retry_attempts: int = 3,
        backoff_seconds: int = 30,
    ) -> bool:
        # The passed in list of files should be Pawsey DMF paths.
        # The dmget daemon just needs filenames so sanitise it first
        filename_list = []

        for file in file_list:
            # Ensure item is a full file path. Split it so we get the filename
            # only
            if os.path.isabs(file):
                filename_list.append(os.path.split(file)[1])
            else:
                self.log_error(
                    obs_id,
                    "Staging failed: stage_files() requires absolute paths,"
                    f" not just filenames ({file}).",
                )

        #
        # Stage the files on the Pawsey filesystem
        #
        attempts = 1

        t1 = time.time()

        # While we have attempts left and we are not shutting down
        while attempts <= retry_attempts and self.terminate is False:
            if attempts == 1:
                self.log_info(
                    obs_id, f"Staging {len(filename_list)} files from tape... "
                )
            else:
                self.log_info(
                    obs_id,
                    f"Staging {len(filename_list)} files from tape... "
                    f"(attempt {attempts}/{retry_attempts})",
                )

            try:
                # Staging will return 0 on success or non zero on staging
                # error or raise exception on other failure
                exit_code = pawsey_stage_files(
                    filename_list, self.staging_host, self.staging_port
                )
                t2 = time.time()

                if exit_code == 0:
                    self.log_info(
                        obs_id,
                        f"Staging {len(filename_list)} files from tape"
                        f" complete ({t2 - t1:.2f} seconds).",
                    )
                    return True
                else:
                    self.log_error(
                        obs_id, f"Staging failed with return code {exit_code}."
                    )
            except Exception as staging_error:
                self.log_error(
                    obs_id,
                    "Staging failed with other exception condition"
                    f" {str(staging_error)}.",
                )

            self.log_debug(
                obs_id,
                f"Backing off {backoff_seconds} seconds trying to stage data.",
            )
            time.sleep(backoff_seconds)
            attempts += 1

        # If we get here, we give up
        if not self.terminate:
            t2 = time.time()
            self.log_error(
                obs_id,
                "Staging failed too many times. Gave up after trying"
                f" {retry_attempts} times and {t2 - t1:.2f} seconds.",
            )
        return False

    def get_working_filename_and_path(self, obs_id: int, filename: str) -> str:
        # will return working_path/obs_id/filename
        if self.working_path:
            return os.path.join(self.working_path, str(obs_id), filename)
        else:
            raise Exception(
                "get_working_filename_and_path(): working_path is not defined"
                " in the config file."
            )

    def prepare_observation_working_directory(self, obs_id: int):
        if self.working_path:
            obs_id_str = str(obs_id)

            path_to_create = os.path.join(self.working_path, obs_id_str)

            if os.path.exists(path_to_create):
                # The folder exists, lets delete it and it's contents
                # just in case
                self.cleanup_observation_working_directory(obs_id)

            # Now Create directory
            os.mkdir(path_to_create, int(0o755))
            self.log_debug(obs_id, f"Created directory {path_to_create}...")
        else:
            raise Exception(
                "prepare_observation_working_directory(): working_path is not"
                " defined in the config file."
            )

    def cleanup_observation_working_directory(self, obs_id: int):
        if self.working_path:
            obs_id_str = str(obs_id)
            path_to_clean = os.path.join(self.working_path, obs_id_str)

            if os.path.exists(path_to_clean):
                self.log_debug(
                    obs_id,
                    f"Removing directory and contents of {path_to_clean}...",
                )

                # Remove any files in there
                shutil.rmtree(path_to_clean)
            else:
                # Nothing to do
                pass
        else:
            raise Exception(
                "cleanup_observation_working_directory(): working_path is not"
                " defined in the config file."
            )

    #
    # Virtual methods
    #
    def get_observation_list(self) -> list:
        # This is to be supplied by the inheritor. Using SQL or whatever
        # to get a list of obs_ids
        raise NotImplementedError()

    def process_one_observation(self, observation: Observation) -> bool:
        # This is to be supplied by the inheritor.
        # Any staging (for small obs) should be done by the inheritor
        raise NotImplementedError()

    def get_observation_item_list(self, observation: Observation) -> list:
        # This is to be supplied by the inheritor. We pass in the list
        # of staged files for the current obs_id
        # If implements_per_item_processing == 0 then just 'pass'
        raise NotImplementedError()

    def process_one_item(self, observation: Observation, item: str) -> bool:
        # This is to be supplied by the inheritor.
        # If implements_per_item_processing == 0 then just 'pass'
        # For large (e.g. VCS) observations, inheritor should stage files here
        raise NotImplementedError()

    def end_of_observation(self, observation: Observation) -> bool:
        # This is to be supplied by the inheritor.
        # If implements_per_item_processing == 0 then just 'pass'
        raise NotImplementedError()

    def get_status(self) -> str:
        status = (
            f"Running {self.concurrent_observations} observations. "
            "Observations remaining:"
            f" {self.observation_queue.qsize()}/"
            f"{self.stats_obs_to_process}"
        )

        if self.implements_per_item_processing:
            status = f"{status} Processing: {self.current_observations} > "

            for child in self.current_observations:
                status = (
                    status
                    + f"{[os.path.split(c)[1] if len(os.path.split(c)) == 2 else c for c in child.current_observation_items]}\n"  # noqa: E501
                )
        else:
            status = f"{status} Processing: {self.current_observations}\n"

        return status

    def refresh_obs_list(self):
        self.logger.info("Refreshing observation list...")

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

        self.logger.info(
            f"Refreshing observation list complete. {new_item_count} new"
            " observations."
        )

    # main loop
    def main_loop(self):
        # Get observations to process
        observation_list = []

        try:
            observation_list = self.get_observation_list()
        except Exception:
            self.logger.exception("Exception in get_observation_list()")

        self.stats_obs_to_process = len(observation_list)

        # Do we have any observations to process?
        if self.stats_obs_to_process > 0:
            # Enqueue them
            for observation_id in observation_list:
                new_obs = Observation(self, observation_id)
                self.observation_queue.put(new_obs)

            # Do processing
            self.logger.info(
                f"{self.concurrent_observations} concurrent observations will"
                " be processed."
            )

            # Kick off x number of consumers of observations simultaneously
            for thread_id in range(self.concurrent_observations):
                new_thread = threading.Thread(
                    name=f"o{thread_id+1}", target=self.observation_consumer
                )
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
                self.logger.debug(
                    f"Thread: {thread.name} is still running, but it"
                    " shouldn't be."
                )

    def observation_consumer(self) -> bool:
        try:
            while self.terminate is False:
                # Get next item, but only if the queue is not locked
                self.observation_queue_lock.acquire(timeout=30)

                # Get next item, now that the lock is released
                observation = self.observation_queue.get_nowait()

                # Release lock
                self.observation_queue_lock.release()

                # Keep track of what we are working on
                self.current_observations.append(observation.obs_id)

                self.log_info(observation.obs_id, "Started")

                # Prepare a working area
                if self.working_path:
                    self.prepare_observation_working_directory(
                        observation.obs_id
                    )

                # process the observation
                try:
                    if self.process_one_observation(observation):
                        if self.implements_per_item_processing:
                            if observation.process():
                                # Now finalise the observation, if need be
                                if self.end_of_observation(observation):
                                    self.stats_obs_processed_good = (
                                        self.stats_obs_processed_good + 1
                                    )
                                else:
                                    self.stats_obs_failed_errors = (
                                        self.stats_obs_failed_errors + 1
                                    )
                        else:
                            self.stats_obs_processed_good = (
                                self.stats_obs_processed_good + 1
                            )
                    else:
                        self.stats_obs_failed_errors = (
                            self.stats_obs_failed_errors + 1
                        )

                except Exception:
                    self.log_exception(
                        observation.obs_id,
                        "Exception in process_one_observation()",
                    )

                # Tell queue that job is done
                self.observation_queue.task_done()

                # Cleanup any temp files in working area
                if self.working_path:
                    self.cleanup_observation_working_directory(
                        observation.obs_id
                    )

                # Update stat on what we're working on
                self.current_observations.remove(observation.obs_id)

                self.log_info(observation.obs_id, "Complete")

        except queue.Empty:
            self.logger.debug("Queue empty")

            # Release lock
            self.observation_queue_lock.release()

        return True

    def stop(self):
        self.terminate = True

        if not self.terminated:
            if self.logger:
                self.logger.info("Stopping processor...")
            else:
                print("Stopping processor...")

            # Wait until current tasks are done- then clear the
            # queue so we get past queue.join
            if len(self.consumer_threads) > 0:
                self.logger.debug(
                    f"Waiting for {len(self.consumer_threads)} tasks to"
                    " complete..."
                )

            while len(self.consumer_threads) > 0:
                for c in self.consumer_threads:
                    if not c.is_alive():
                        self.logger.debug(f"{c.name} has stopped.")
                        self.consumer_threads.remove(c)
                    time.sleep(0.1)

            # Now they are all stopped, we can dequeue the
            # remaining items and cleanup
            self.logger.debug("Clearing observation queue")
            while self.observation_queue.qsize() > 0:
                self.observation_queue.get_nowait()
                self.observation_queue.task_done()

            # Close pools
            self.logger.debug("Closing database pools")
            if self.mro_metadata_db_pool:
                self.mro_metadata_db_pool.closeall()

            # End the web server
            self.logger.info("Webserver stopping...")
            self.web_server.shutdown()
            self.logger.debug("Webserver stopped. ")

            self.logger.info(
                "Total Observations to process      :"
                f" {self.stats_obs_to_process}"
            )
            attempted = (
                self.stats_obs_processed_good + self.stats_obs_failed_errors
            )
            self.logger.info(
                f"Observations attempted             : {attempted}"
            )
            self.logger.info(
                "Observations processed successfully:"
                f" {self.stats_obs_processed_good}"
            )
            self.logger.info(
                "Observations failed with errors    :"
                f" {self.stats_obs_failed_errors}"
            )
            self.terminated = True
