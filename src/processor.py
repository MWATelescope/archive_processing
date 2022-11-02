import logging
import os
import signal
import sys
from abc import ABC, abstractmethod
from argparse import Namespace
from collections import defaultdict
from configparser import ConfigParser

import boto3
from botocore.config import Config
from psycopg import Connection

from repository import DeleteRepository

locations = {
    2: 'acacia',
    3: 'banksia'
}


class Processor(ABC):
    def __init__(self, args: Namespace, connection: Connection | None):
        self.verbose = args.verbose
        self.dry_run = args.dry_run
        self.logger = logging.getLogger()
        self.config = self._read_config(args.cfg)
        self.terminate = False

        if self.dry_run:
            self.logger.info("Dry run enabled.")

        if self.verbose:
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.WARN)

        for sig in [signal.SIGINT]:
            signal.signal(sig, self._signal_handler)

    def _read_config(self, file_name: str) -> ConfigParser:
        """
        Parameters
        ----------
        file_name: str
            Path to a config file.

        Returns
        -------
        ConfigParser
            ConfigParser object with parsed information from file.
        """

        self.logger.info("Parsing config file.")

        config = ConfigParser()

        try:
            with open(file_name) as f:
                config.read_file(f)

            return config
        except (IOError, FileNotFoundError):
            self.logger.error("Could not parse config file.")
            sys.exit(1)

    def _signal_handler(self, sig, frame):
        self.logger.info(f"Interrupted! Received signal {sig}.")
        self.terminate = True

    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError


class DeleteProcessor(Processor):
    def __init__(self, args: ConfigParser, connection: Connection):
        super().__init__(args, connection)

        try:
            self.logger.info("Connecting to database.")
            self.repository = DeleteRepository(self.config, connection, self.dry_run)
        except Exception as e:
            self.logger.error("Could not connect to database.")
            self.logger.error(e)
            sys.exit(1)

    def _bucket_delete_keys(self, bucket, keys: list[str]) -> list[list]:
        """
        For a given bucket and list of keys to be deleted from that bucket,
        make the S3 call to delete the corresponding objects. Then, return a list of filenames which were deleted.

        Parameters
        ----------
        bucket:
            A resource representing the S3 bucket.
        keys: list[str]
            A list of keys inside the bucket to be deleted.

        Returns
        -------
        A list of filenames which were deleted from the bucket.
        """
        response = bucket.delete_objects(
            Delete={
                'Objects': [{'Key': key} for key in keys]
            }
        )

        deleted_objects = response["Deleted"]
        deleted_keys = [deleted_object["Key"] for deleted_object in deleted_objects]
        return [[os.path.split(key)[-1] for key in deleted_keys]]

    def _batch_delete_objects(self, location: int, bucket: str, keys_to_delete: list[str]) -> None:
        """
        For a given file location, bucket within that location, and list of keys - delete those keys from the file.

        Parameters
        ----------
        location: int
            Number representing the location of files, usually either Acacia (2) or Banksia (3)
        bucket: str
            The name of a bucket within location
        keys_to_delete: list[str]
            A list of string keys within bucket which will be deleted
        """

        # If we've received a signal, do not process this batch and exit. Allows in progress batches to complete execution
        if self.terminate:
            self.logger.warn("Exiting.")
            sys.exit(0)

        try:
            config = Config(connect_timeout=5, retries={"mode": "standard"})

            match locations[location]:
                case 'acacia':
                    s3 = boto3.resource(
                        's3',
                        aws_access_key_id=self.config.get('acacia', 'aws_access_key_id'),
                        aws_secret_access_key=self.config.get('acacia', 'aws_secret_access_key'),
                        endpoint_url=self.config.get('acacia', 'endpoint_url'),
                        config=config
                    )
                case 'banksia':
                    s3 = boto3.resource(
                        's3',
                        aws_access_key_id=self.config.get('banksia', 'aws_access_key_id'),
                        aws_secret_access_key=self.config.get('banksia', 'aws_secret_access_key'),
                        endpoint_url=self.config.get('banksia', 'endpoint_url'),
                        config=config
                    )
                case _:
                    raise ValueError(f"Invalid location found {location}.")

            bucket_object = s3.Bucket(bucket)

        except Exception as e:
            self.logger.warning(f"Could not connect to {location}:{bucket}.")
            self.logger.warning(e)
            raise

        if self.dry_run:
            self.logger.info(f"Would have deleted {len(keys_to_delete)} files from {locations[location]}:{bucket}.")
            return
        else:
            self.logger.info(f"Deleting {len(keys_to_delete)} files from {locations[location]}:{bucket}.")
            self.logger.info(keys_to_delete)

        # If this fails, should we exit? 🤔
        self.repository.update_files_to_deleted(self._bucket_delete_keys, bucket_object, keys_to_delete)

    def _generate_data_structures(self) -> dict:
        """
        Algorithm to generate two data structures which will later be processed.
        Foreach delete request:
            Foreach obs_id in delete request:
                Foreach file (location, bucket, key) in obs_id

        Returns
        ----------
        files: dict
            Of the format:
            {
                location_id_2: {
                    bucket1: (path/1.fits, path/2.fits),
                    bucket2: (path/1.fits, path/2.fits)
                },
                location_id_3: {
                    bucket3: (path/1.fits, path/2.fits),
                    bucket4: (path/1.fits, path/2.fits)
                }
            }
        delete_request: dict
            Of the format:
            {
                delete_request_id_1: [obs_id_1, obs_id_2],
                delete_request_id_2: [obs_id_3, obs_id_4]
            }
        """
        delete_requests_ids = self.repository.get_delete_requests()
        num_delete_requests = len(delete_requests_ids)

        self.logger.info(f"Found {num_delete_requests} delete requests.")

        files = defaultdict(lambda: defaultdict(set))
        delete_requests = defaultdict(list)

        for index, delete_request_id in enumerate(delete_requests_ids):
            obs_ids = self.repository.get_obs_ids_for_delete_request(delete_request_id)
            invalid_obs_ids = self.repository.validate_obsids(obs_ids)

            self.logger.info(f"Processing delete request ({index + 1}/{num_delete_requests}).")
            self.logger.info(f"Delete request {delete_request_id} contains {len(obs_ids)} obs_ids.")

            if invalid_obs_ids:
                # Shouldn't happen, but catching the case where an observation was added to a collection after the delete request was created
                self.logger.error(f"Delete request {delete_request_id} contains observations that cannot be deleted. Please check and try again.")
                sys.exit(0)

            for index, obs_id in enumerate(obs_ids):
                obs_files = self.repository.get_not_deleted_obs_data_files_except_ppds(obs_id)
                delete_requests[delete_request_id].append(obs_id)

                self.logger.info(f"Processing obs_id {obs_id} ({index + 1}/{len(obs_ids)}).")
                self.logger.info(f"obs_id {obs_id} contains {len(obs_files)} files.")

                for index, file in enumerate(obs_files):
                    self.logger.info(f"Processing file {index + 1}/{len(obs_files)}")

                    files[file['location']][file['bucket']].add(file['key'])

        self.logger.info(f"Found files to delete in {len(files.keys())} locations.")

        return files, delete_requests

    def _process_file_structure(self, files: dict) -> None:
        """
        Iterates through the files dict (described above), and sends delete requests in batches of 1000

        Parameters
        ----------
        files: dict
            Described in docstring above
        """
        for index, location in enumerate(files.keys()):
            buckets = files[location]

            self.logger.info(f"Location {location} contains {len(buckets)} buckets with deleteable files.")
            self.logger.info(f"Processing location {index + 1}/{len(files.keys())}")

            for bucket in buckets:
                keys = files[location][bucket]
                keys_to_delete = []
                counter = 0

                self.logger.info(f"Bucket {bucket} contains {len(keys)} files to be deleted.")

                for key in keys:
                    keys_to_delete.append(key)
                    counter += 1

                    if counter == self.config.getint('processing', 'batch_size'):
                        # Delete in batches of 1000
                        self._batch_delete_objects(location, bucket, keys_to_delete)
                        keys_to_delete = []
                        counter = 0

                # Delete any that are left!
                if keys_to_delete:
                    self._batch_delete_objects(location, bucket, keys_to_delete)

    def _process_delete_requests(self, delete_requests: dict) -> tuple[int, dict]:
        """
        Iterates through the delete_requests dict (described above).
        Check with obs_id had all of its files deleted, and marks the observation itself as deleted.
        If all files associated with a delete request were deleted, mark the delete request as actioned.

        Parameters
        ----------
        files: dict
            Described in docstring above

        Returns
        ----------
        num_actioned_delete_requests: int
            The number of delete requests that were actioned.
        failed_delete_requests: dict
            Dictionary containing the failed delete request ID as the key, and a list of observations that failed as the value.
        """
        num_actioned_delete_requests = 0
        failed_delete_requests = defaultdict(list)

        for delete_request_id in delete_requests:
            self.logger.info(f"Reviewing delete request {delete_request_id}.")

            delete_request_has_missing_files = False

            for obs_id in delete_requests[delete_request_id]:

                if self.terminate:
                    self.logger.warn("Exiting.")
                    sys.exit(0)

                obs_files = self.repository.get_not_deleted_obs_data_files_except_ppds(obs_id)

                if len(obs_files) > 0:
                    self.logger.info(f"obs_id {obs_id} has {len(obs_files)} files that are not deleted.")

                    delete_request_has_missing_files = True
                    failed_delete_requests[delete_request_id].append(obs_id)
                else:
                    self.logger.info(f"Updating obs_id {obs_id} to deleted.")

                    self.repository.set_obs_id_to_deleted(obs_id)

            if not delete_request_has_missing_files:
                self.logger.info(f"Updating delete request {delete_request_id} to actioned.")

                self.repository.set_delete_request_to_actioned(delete_request_id)
                num_actioned_delete_requests += 1

        return num_actioned_delete_requests, failed_delete_requests

    def _print_summary(self, delete_requests: dict, num_actioned_delete_requests: int, failed_delete_requests: dict) -> None:
        """
        Logs a summary of what was processed.

        Parameters
        ----------
        delete_requests: int
            The delete requests structure described above
        num_actioned_delete_requests: int
            The number of delete requests that were actioned
        failed_delete_requests: int
            Dictionary of which delete request IDs failed (and their corresponding observations)
        """
        self.logger.info(f"Delete requests to process: {len(delete_requests.keys())}.")
        self.logger.info(f"Delete requests actioned:   {num_actioned_delete_requests}.")

        for failed_delete_request in failed_delete_requests.keys():
            self.logger.info(f"Delete request {failed_delete_request} was not actioned. The following observations were not deleted: {failed_delete_requests[failed_delete_request]}.")

    def run(self) -> None:
        """
        Main workflow. Does the processing bit of the processor.
        """
        self.logger.info("Starting delete processor.")

        files, delete_requests = self._generate_data_structures()
        self._process_file_structure(files)
        num_actioned_delete_requests, failed_delete_requests = self._process_delete_requests(delete_requests)
        self._print_summary(delete_requests, num_actioned_delete_requests, failed_delete_requests)


class ProcessorFactory():
    def __init__(self, args: Namespace, connection: Connection | None = None):
        self.args = args
        self.connection = connection

    def get_processor(self) -> Processor:
        subcommand = self.args.subcommand
        match subcommand:
            case 'delete':
                return DeleteProcessor(self.args, self.connection)
            case _:
                raise ValueError(f"Missing or invalid subcommand {subcommand}.")
