import queue
import threading


class Observation:
    def __init__(self, processor, obs_id: int):
        self.obs_id = obs_id
        self.processor = processor
        self.observation_item_queue = queue.Queue()
        self.observation_item_list = []
        self.num_items_to_process = 0
        self.num_items_processed_successfully = 0
        self.current_observation_items = []
        self.successful_observation_items = []
        self.consumer_threads = []

    def process(self):
        # now get the list of items to process
        self.observation_item_list = self.processor.get_observation_item_list(
            self
        )
        self.num_items_to_process = len(self.observation_item_list)

        # Enqueue items into the queue
        for item in self.observation_item_list:
            self.observation_item_queue.put(item)

        # process each item
        self.processor.logger.info(
            f"{self.processor.concurrent_items} concurrent items will be"
            " processed."
        )

        if self.observation_item_queue.qsize() > 0:
            parent_name = threading.current_thread().name

            for thread_id in range(self.processor.concurrent_items):
                new_thread = threading.Thread(
                    name=f"{parent_name}_i{thread_id + 1}",
                    target=self.observation_item_consumer,
                    args=(self.obs_id,),
                )
                self.consumer_threads.append(new_thread)

            # Start all threads
            for thread in self.consumer_threads:
                thread.start()

            self.observation_item_queue.join()

        self.processor.logger.debug("Item Queue empty. Cleaning up...")

        # Cancel the consumers- these will be idle now anyway
        for thread in self.consumer_threads:
            thread.join()

        # Reset consumer theads list for next loop
        self.consumer_threads = []

        return True

    def observation_item_consumer(self, obs_id: int) -> bool:
        try:
            self.processor.log_debug(obs_id, "Task Started")

            while self.processor.terminate is False:
                # Get next item
                item = self.observation_item_queue.get_nowait()

                # Update stats
                self.current_observation_items.append(item)

                try:
                    if self.processor.process_one_item(self, item):
                        self.num_items_processed_successfully += 1
                        self.successful_observation_items.append(item)

                except Exception:
                    self.processor.log_exception(
                        obs_id, f"{item}: Exception in process_one_item()"
                    )
                finally:
                    # Tell queue that job is done
                    self.observation_item_queue.task_done()

                    # Update stats
                    self.current_observation_items.remove(item)

            self.processor.log_debug(self.obs_id, "Clearing item queues")
            while self.observation_item_queue.qsize() > 0:
                self.observation_item_queue.get_nowait()
                self.observation_item_queue.task_done()

        except queue.Empty:
            self.processor.log_debug(obs_id, "Queue empty")

        # Mark this task as done
        self.processor.log_debug(obs_id, "Task Complete")
        return True
