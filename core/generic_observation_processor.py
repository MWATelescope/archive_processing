import asyncio
import asyncio.tasks
import asyncpg
import logging.handlers
import os
import signal


class GenericObservationProcessor:
    def __init__(self, processor_name, config):
        print(f"Initialising {processor_name}...")
        self.name = processor_name
        self.terminate = False

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
        log_file_handler.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(message)s"))
        self.logger.addHandler(log_file_handler)

        # Setup console logging
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG)
        console_handler.setFormatter(logging.Formatter("%(asctime)s, %(levelname)s, %(message)s"))
        self.logger.addHandler(console_handler)

        self.logger.info("Reading configuration...")
        # Database connection config
        self.mro_metadata_db_host = config.get("mro_metadata_db", "host")
        self.mro_metadata_db_port = config.getint("mro_metadata_db", "port")
        self.mro_metadata_db_name = config.get("mro_metadata_db", "db")
        self.mro_metadata_db_user = config.get("mro_metadata_db", "user")
        self.mro_metadata_db_pass = config.get("mro_metadata_db", "pass")
        self.mro_metadata_db_dsn = f"postgres://{self.mro_metadata_db_user}:" \
                                   f"{self.mro_metadata_db_pass}@" \
                                   f"{self.mro_metadata_db_host}:" \
                                   f"{self.mro_metadata_db_port}/" \
                                   f"{self.mro_metadata_db_name}"

        self.ngas_db_host = config.get("ngas_db", "host")
        self.ngas_db_port = config.getint("ngas_db", "port")
        self.ngas_db_name = config.get("ngas_db", "db")
        self.ngas_db_user = config.get("ngas_db", "user")
        self.ngas_db_pass = config.get("ngas_db", "pass")
        self.ngas_db_dsn = f"postgres://{self.ngas_db_user}:" \
                           f"{self.ngas_db_pass}@" \
                           f"{self.ngas_db_host}:" \
                           f"{self.ngas_db_port}/" \
                           f"{self.ngas_db_name}"

        # Stagin config
        self.staging_host = config.get("staging", "dmget_host")
        self.staging_port = config.getint("staging", "dmget_port")

        # Get generic processor options
        self.implements_per_file_processing = config.getint("processing", "implements_per_file_processing")
        self.concurrent_observations = config.getint("processing", "concurrent_observation_processes")

        # Queues
        self.obs_queue = asyncio.Queue()
        self.file_queue = asyncio.Queue()
        self.consumers = None

        # Create pools
        self.logger.info("Setting up database pools...")
        self.mro_metadata_db_pool = asyncpg.create_pool(dsn=self.mro_metadata_db_dsn)
        self.ngas_db_pool = asyncpg.create_pool(dsn=self.ngas_db_dsn)

    def log_info(self, obs_id, task_id, message):
        self.logger.info(f"{task_id}: {obs_id}: {message}")

    def log_error(self, obs_id, task_id, message):
        self.logger.error(f"{task_id}: {obs_id}: {message}")

    def log_exception(self, obs_id, task_id, message, exception):
        self.logger.error(f"{task_id}: {obs_id}: {message}. {str(exception)}")

    #
    # Virtual methods
    #
    async def get_observation_list(self):
        # This is to be supplied by the inheritor. Using SQL or whatever to get a list of obs_ids
        raise NotImplementedError()

    async def process_one_observation(self, obs_id, task_id):
        # This is to be supplied by the inheritor.
        raise NotImplementedError()

    async def get_observation_file_list(self, obs_id, task_id):
        # This is to be supplied by the inheritor. Using SQL or whatever to get a list of files for the current obs_id
        # If implements_per_file_processing == 0 then just 'pass'
        raise NotImplementedError()

    async def process_one_file(self, obs_id, task_id, filename):
        # This is to be supplied by the inheritor.
        # If implements_per_file_processing == 0 then just 'pass'
        raise NotImplementedError()

    async def end_of_observation(self, obs_id, task_id):
        # This is to be supplied by the inheritor.
        # If implements_per_file_processing == 0 then just 'pass'
        raise NotImplementedError()

    # main loop
    async def main_loop(self):
        # Get observations to process
        observation_list = await self.get_observation_list()

        # Enqueue them
        for observation in observation_list:
            await self.obs_queue.put(observation)

        # Kick off x number of consumers
        loop = asyncio.get_event_loop()
        self.consumers = [loop.create_task(self.observation_consumer(x)) for x in range(self.concurrent_observations)]

        # Wait for all consumers to finish
        await self.obs_queue.join()

        self.logger.info("Queue empty. Cleaning up...")

        # Cancel the consumers- these will be idle now anyway
        for c in self.consumers:
            c.cancel()

        # Close pools
        try:
            await asyncio.wait_for(self.mro_metadata_db_pool.close(), timeout=30)
        except asyncio.TimeoutError:
            self.logger.error("Timeout waiting to gracefully close mro_metadata_db_pool.")
        except asyncpg.exceptions.InterfaceError:
            self.logger.debug("Cannot close mro_metadata_db_pool - pool not initialised.")

        try:
            await asyncio.wait_for(self.ngas_db_pool.close(), timeout=30)
        except asyncio.TimeoutError:
            self.logger.error("Timeout waiting to gracefully close ngas_db_pool.")
        except asyncpg.exceptions.InterfaceError:
            self.logger.debug("Cannot close ngas_db_pool - pool not initialised.")

    async def observation_consumer(self, task_id):
        try:
            self.logger.info(f"{task_id}: Task Started")

            while self.terminate is False:
                # Get next item
                obs_id = self.obs_queue.get_nowait()

                # Get file list
                file_list = await self.get_observation_file_list(obs_id, task_id)

                # Process files
                for filename in file_list:
                    await self.process_one_file(obs_id, task_id, filename)

                # Call end of observation
                await self.end_of_observation(obs_id, task_id)

                # Tell queue that job is done
                self.obs_queue.task_done()
        except asyncio.QueueEmpty:
            self.logger.info(f"{task_id}: Queue empty")

        # Mark this task as done
        self.logger.info(f"{task_id}: Task Complete")
        return True

    def start(self):
        self.logger.info(f"{self.name} started")

        # Setup main loop
        loop = asyncio.get_event_loop()

        signals = (signal.SIGHUP, signal.SIGTERM, signal.SIGINT)
        for s in signals:
            loop.add_signal_handler(
                s, lambda s=s: loop.create_task(self.signal_handler(s, loop)))

        loop.run_until_complete(self.main_loop())
        loop.close()

        self.logger.info(f"{self.name} stopped")

    async def signal_handler(self, sig, frame):
        if self.logger:
            self.logger.info("Interrupted! Closing down...")
        else:
            print("Interrupted! Closing down...")
        self.terminate = True

        # Wait until current tasks are done- then clear the queue so we get past queue.join
        while len(self.consumers) > 0:
            for c in self.consumers:
                if c.done():
                    self.consumers.remove(c)

            self.logger.info(f"Waiting for {len(self.consumers)} tasks to complete...")
            await asyncio.sleep(1)

        # Now they are all stopped, we can dequeue the remaining items
        self.logger.info("Clearing queue")
        while self.obs_queue.qsize() > 0:
            self.obs_queue.get_nowait()
            self.obs_queue.task_done()
