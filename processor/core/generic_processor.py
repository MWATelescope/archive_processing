import base64
import glob
import logging.handlers
import os
import psycopg2.pool
import shutil
import signal
import threading
from configparser import ConfigParser
from processor.core.processor_webservice import ProcessorHTTPServer, ProcessorHTTPGetHandler


class GenericProcessor:
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
        if config.has_option("processing", "log_dir"):
            log_dir = config.get("processing", "log_dir")
        else:
            log_dir = "logs"

        if not os.path.isdir(log_dir):
            print(f"Log director {log_dir} directory found. Creating directory.")
            os.mkdir(log_dir)

        if self.execute:
            self.log_filename = f"{log_dir}/{self.name}.log"
        else:
            self.log_filename = f"{log_dir}/{self.name}_dry_run.log"

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

        # Initialise stuff
        self.logger.info("Initialising...")

        if self.execute:
            self.logger.warning("** EXECUTE is true. Will make changes to data! **")
        else:
            self.logger.info("DRY RUN. No data will be changed")

        # Web server config
        self.web_service_port = config.get("web_service", "port")

        # Create and start web server
        self.logger.info(f"Starting http server on port {self.web_service_port}...")
        self.web_server = ProcessorHTTPServer(('', int(self.web_service_port)), ProcessorHTTPGetHandler)
        self.web_server.context = self
        self.web_server_thread = threading.Thread(name='webserver',
                                                  target=self.web_server_loop,
                                                  args=(self.web_server,))
        self.web_server_thread.setDaemon(True)
        self.web_server_thread.start()

        if config.has_option("processing", "working_path"):
            self.working_path = config.get("processing", "working_path")

            # Check working path
            self.check_root_working_directory_exists()

            # Clean working path
            self.cleanup_root_working_directory()
        else:
            self.working_path = None

        # Database connection config
        if config.has_section("mro_metadata_db"):
            self.mro_metadata_db_host = config.get("mro_metadata_db", "host")
            self.mro_metadata_db_port = config.getint("mro_metadata_db", "port")
            self.mro_metadata_db_name = config.get("mro_metadata_db", "db")
            self.mro_metadata_db_user = config.get("mro_metadata_db", "user")
            self.mro_metadata_db_pass = config.get("mro_metadata_db", "pass")
        else:
            self.mro_metadata_db_host = None
            self.mro_metadata_db_port = None
            self.mro_metadata_db_name = None
            self.mro_metadata_db_user = None
            self.mro_metadata_db_pass = None

        if config.has_section("ngas_db"):
            self.ngas_db_host = config.get("ngas_db", "host")
            self.ngas_db_port = config.getint("ngas_db", "port")
            self.ngas_db_name = config.get("ngas_db", "db")
            self.ngas_db_user = config.get("ngas_db", "user")
            self.ngas_db_pass = config.get("ngas_db", "pass")
        else:
            self.ngas_db_host = None
            self.ngas_db_port = None
            self.ngas_db_name = None
            self.ngas_db_user = None
            self.ngas_db_pass = None

        # Staging config
        if config.has_section("staging"):
            self.staging_host = config.get("staging", "dmget_host")
            self.staging_port = config.getint("staging", "dmget_port")
            self.staging_retry_attempts = config.getint("staging", "retry_attempts")
            self.staging_backoff_seconds = config.getint("staging", "backoff_seconds")
        else:
            self.staging_host = None
            self.staging_port = None
            self.staging_retry_attempts = None
            self.staging_backoff_seconds = None

        # Archiving config
        if config.has_section("archive"):
            self.ngas_host = config.get("archive", "ngas_host")
            self.ngas_port = config.getint("archive", "ngas_port")
            self.ngas_user = config.get("archive", "ngas_user")
            self.ngas_pass = base64.b64decode(config.get("archive", "ngas_pass")).decode()
        else:
            self.ngas_host = None
            self.ngas_port = None
            self.ngas_user = None
            self.ngas_pass = None

        # Create pools if needed
        if self.mro_metadata_db_host is not None:
            self.logger.info("Setting up database pools...")
            self.mro_metadata_db_pool = psycopg2.pool.ThreadedConnectionPool(2, 10,
                                                                             user=self.mro_metadata_db_user,
                                                                             password=self.mro_metadata_db_pass,
                                                                             host=self.mro_metadata_db_host,
                                                                             port=self.mro_metadata_db_port,
                                                                             database=self.mro_metadata_db_name)
        if self.ngas_db_host is not None:
            self.ngas_db_pool = psycopg2.pool.ThreadedConnectionPool(2, 10,
                                                                     user=self.ngas_db_user,
                                                                     password=self.ngas_db_pass,
                                                                     host=self.ngas_db_host,
                                                                     port=self.ngas_db_port,
                                                                     database=self.ngas_db_name)

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

    def web_server_loop(self, web_server):
        web_server.serve_forever()

    def get_status(self) -> str:
        # Provided by inheritor
        raise NotImplementedError()

    # main loop
    def main_loop(self):
        # inheritor provides
        raise NotImplementedError()

    def start(self):
        self.logger.info(f"{self.name} started")

        # Setup main loop
        self.main_loop()

        # Stop everything
        self.stop()

        self.logger.info(f"{self.name} stopped")

    def stop(self):
        # inheritor provides
        raise NotImplementedError()

    def signal_handler(self, sig, frame):
        if self.logger:
            self.logger.info("Interrupted!")
        else:
            print("Interrupted!")

        self.stop()
