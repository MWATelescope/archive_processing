import argparse
import os
import queue
import threading
import time
from configparser import ConfigParser
from archive_processing.core.observation import Observation
from archive_processing.core.generic_processor import GenericProcessor
from archive_processing.utils.mwa_metadata import (
    get_delete_requests,
    get_delete_request_observations,
)


class DeleteRequestProcessor(GenericProcessor):
    # This class will gather all Delete_Requests from the mwa metadata db.
    # It will loop through each one (one at a time).
    # Each delete request will then spawn N threads to process an
    # observation each.
    #
    # Each observation will
    #
    def __init__(
        self, processor_name: str, config: ConfigParser, execute: bool
    ):
        super().__init__(processor_name, config, execute)

        # Settings
        self.s3_delete_batch_size = 2

        # Queues
        self.observation_queue = queue.Queue()

        # Stats / info
        self.stats_delete_requests_to_process = 0
        self.stats_delete_request_attempted = 0
        self.stats_delete_requests_successful = 0
        self.stats_delete_requests_failed = 0

        self.stats_obs_to_process = 0
        self.stats_obs_attempted = 0
        self.stats_obs_successful = 0
        self.stats_obs_failed = 0

        self.current_delete_request = None

        # Threads
        self.consumer_threads = []
        self.concurrent_observations = 3
        self.concurrent_files_per_observation = 24

        # Locks
        # This is used to lock the queue while we refresh the list of
        # observations to process
        self.observation_queue_lock = threading.Lock()
        self.observation_stats_lock = threading.Lock()

    def get_delete_request_list(self) -> list:
        return get_delete_requests(self.mro_metadata_db_pool)

    def get_observation_list(self, delete_request_id: int) -> list:
        return get_delete_request_observations(
            self.mro_metadata_db_pool, delete_request_id
        )

    def get_status(self) -> str:
        status = (
            "Delete requests: "
            f"{self.stats_delete_requests_to_process} "
            f"Attempted: {self.stats_delete_request_attempted} "
            f"Success: {self.stats_delete_requests_successful} "
            f"Failed: {self.stats_delete_requests_failed}; "
            "Observations: "
            f"{self.stats_obs_to_process} "
            f"Attempted: {self.stats_obs_attempted} "
            f"Success: {self.stats_obs_successful} "
            f"Failed: {self.stats_obs_failed}"
        )

        return status

    # main loop
    def main_loop(self):
        delete_request_list = []

        # Get delete requests to process
        try:
            delete_request_list = self.get_delete_request_list()
        except Exception:
            self.logger.exception("Exception in get_delete_request_list()")

        delete_request_list.append(9)
        delete_request_list.append(9)

        self.stats_delete_requests_to_process = len(delete_request_list)

        # Do we have any delete requests to process?
        for delete_request_id in delete_request_list:
            self.stats_delete_request_attempted += 1

            self.logger.info(
                f"Delete Request {self.stats_delete_request_attempted} of"
                f" {self.stats_delete_requests_to_process}: Starting..."
            )

            # Get observations in delete request
            try:
                observation_list = self.get_observation_list(delete_request_id)
            except Exception:
                self.logger.exception(
                    f"Exception in get_observation_list({delete_request_id})"
                )

            observation_list.append(2000000064)
            observation_list.append(2000000128)
            observation_list.append(2000000192)
            observation_list.append(2000000256)
            observation_list.append(2000000320)
            observation_list.append(2000000384)

            obs_to_process = len(observation_list)

            self.observation_stats_lock.acquire(timeout=30)
            self.stats_obs_to_process += obs_to_process
            self.observation_stats_lock.release()

            self.logger.info(
                f"Delete Request {self.stats_delete_request_attempted} of"
                f" {self.stats_delete_requests_to_process}:"
                f" {obs_to_process} observations to"
                " process."
            )

            # Enqueue the observations
            for obs_id in observation_list:
                new_observation: Observation = Observation(
                    obs_id,
                    self.logger,
                    self.mro_metadata_db_pool,
                    self.s3_delete_batch_size,
                    self.execute,
                )
                self.observation_queue.put(new_observation)

            # Do processing
            # Kick off x number of consumers of observations
            # simultaneously
            for thread_id in range(self.concurrent_observations):
                new_thread = threading.Thread(
                    name=f"o{thread_id+1}",
                    target=self.observation_consumer,
                )
                self.consumer_threads.append(new_thread)

            # Start all threads
            for thread in self.consumer_threads:
                thread.start()

            # Wait for all consumers to finish
            self.observation_queue.join()

            # clear consumer threads
            self.consumer_threads = []

            self.logger.debug(
                f"Delete Request {self.stats_delete_request_attempted} of"
                f" {self.stats_delete_requests_to_process}: Complete."
            )

            if (
                self.stats_obs_failed == 0
                or self.stats_obs_to_process == self.stats_obs_attempted
            ):
                self.stats_delete_requests_successful += 1
            else:
                self.stats_delete_requests_failed += 1

            # Cancel the consumers- these will be idle now anyway
            for thread in self.consumer_threads:
                thread.join()

        # Stopped
        self.logger.debug("main() stopped. ")

        # Summarise!
        self.logger.info(self.get_status())

        # Uncomment this if there are loose threads!
        for thread in threading.enumerate():
            if thread.name != "MainThread":
                self.logger.debug(
                    f"Thread: {thread.name} is still running, but it"
                    " shouldn't be."
                )

    def observation_consumer(self) -> bool:
        #
        # This is executed per thread and peforms processing on
        # one observation at a time
        #
        try:
            while self.terminate is False:
                # Get next item, but only if the queue is not locked
                self.observation_queue_lock.acquire(timeout=30)

                # Get next item, now that the lock is released
                observation: Observation = self.observation_queue.get_nowait()

                # Release lock
                self.observation_queue_lock.release()

                self.observation_stats_lock.acquire(timeout=30)
                self.stats_obs_attempted += 1
                self.observation_stats_lock.release()

                self.log_info(observation.obs_id, "Started")

                # process the observation
                if observation.delete():
                    self.observation_stats_lock.acquire(timeout=30)
                    self.stats_obs_successful += 1
                    self.observation_stats_lock.release()
                else:
                    self.observation_stats_lock.acquire(timeout=30)
                    self.stats_obs_failed += 1
                    self.observation_stats_lock.release()

                # Report status
                observation.log_status()

                self.log_info(observation.obs_id, "Complete")

                # Tell queue that job is done
                self.observation_queue.task_done()

        except queue.Empty:
            self.logger.debug("Queue empty")

            # Release lock
            self.observation_queue_lock.release()
            return True

        except Exception as e:
            self.log_exception(observation.obs_id, e)
            # Tell queue that job is done
            self.observation_queue.task_done()
            return False

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
                try:
                    self.observation_queue.get_nowait()
                    self.observation_queue.task_done()
                except queue.Empty:
                    pass

            # Close pools
            self.logger.debug("Closing database pools")
            if self.mro_metadata_db_pool:
                self.mro_metadata_db_pool.close()

            # End the web server
            self.logger.info("Webserver stopping...")
            self.web_server.shutdown()
            self.logger.debug("Webserver stopped. ")

            self.terminated = True


def run():
    print("Starting DeleteRequestProcessor...")

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
    processor = DeleteRequestProcessor(
        "DeleteRequestProcessor", config, args.execute
    )

    # Initialise
    processor.start()


if __name__ == "__main__":
    run()
