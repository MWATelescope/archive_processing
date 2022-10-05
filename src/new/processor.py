import sys
import logging

from abc import abstractmethod, ABC
from collections import defaultdict
from configparser import ConfigParser
from argparse import Namespace

import boto3
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

        if self.verbose:
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.WARN)

        if self.dry_run:
            self.logger.info("Dry run enabled.")

        self.config = self._read_config(args.cfg)


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

    
    @abstractmethod
    def run(self) -> None:
        raise NotImplementedError


class DeleteProcessor(Processor):
    def __init__(self, args: ConfigParser, connection: Connection):
        super().__init__(args, connection)

        try:
            self.logger.info("Connecting to database.")
            self.repository = DeleteRepository(self.config, connection)
        except Exception as e:
            self.logger.error("Could not connect to database.")
            self.logger.error(e)
            sys.exit(1)


    def _bucket_delete_keys(self, bucket, keys: list[str]) -> None:
        """
        For a given bucket and list of keys to be deleted from that bucket, delete objects.

        Parameters
        ----------
        bucket:
            A resource representing the S3 bucket.
        keys: list[str]
            A list of keys inside the bucket to be deleted.  
        """
        bucket.delete_objects(
            Delete={
                'Objects': [{'Key': key} for key in keys]
            }
        )


    def _batch_delete_objects(self, location: int, bucket: str, keys_to_delete: list[str]):
        self.logger.info(f"Deleting {len(keys_to_delete)} files.")

        try:
            match locations[location]:
                case 'acacia':
                    s3 = boto3.resource(
                        's3',
                        aws_access_key_id=self.config.get('acacia', 'aws_access_key_id'),
                        aws_secret_access_key=self.config.get('acacia', 'aws_secret_access_key'),
                        endpoint_url=self.config.get('acacia', 'endpoint_url')
                    )
                case 'banksia':
                    s3 = boto3.resource(
                        's3',
                        aws_access_key_id=self.config.get('banksia', 'aws_access_key_id'),
                        aws_secret_access_key=self.config.get('banksia', 'aws_secret_access_key'),
                        endpoint_url=self.config.get('banksia', 'endpoint_url')
                    )
                case _:
                    self.logger.warning(f"Invalid location found {location}.")
                    return
        except Exception as e:
            self.logger.warning(f"Could not connect to location {location}.")
            raise

        #Add log line indicating files/bucket/location which would have been deleted
        if self.dry_run:
            #TODO implement dry run properly
            pass
            #return

        bucket_object = s3.Bucket(bucket)

        #If this fails, should we exit? 🤔
        self.repository.update_files_to_deleted(self._bucket_delete_keys, bucket_object, keys_to_delete)


    def _generate_data_structures(self) -> dict:
        delete_requests_ids = self.repository.get_delete_requests()
        num_delete_requests = len(delete_requests_ids)

        self.logger.info(f"Found {num_delete_requests} delete requests.")

        files = defaultdict(dict)
        delete_requests = defaultdict(list)

        for index, delete_request_id in enumerate(delete_requests_ids):
            obs_ids = self.repository.get_obs_ids_for_delete_request(delete_request_id)

            self.logger.info(f"Processing delete request ({index + 1}/{num_delete_requests}).")

            self.logger.info(f"Delete request {delete_request_id} contains {len(obs_ids)} obs_ids.")

            for obs_id in obs_ids:
                obs_files = self.repository.get_obs_data_files_filenames_except_ppds(obs_id)

                self.logger.info(f"Processing obs_id {obs_id}.")
                self.logger.info(f"obs_id {obs_id} contains {len(obs_files)} files.")

                delete_requests[delete_request_id].append(obs_id)

                for file in obs_files:
                    if file['bucket'] in files[file['location']]:
                        files[file['location']][file['bucket']].add(file['key'])
                    else:
                       files[file['location']][file['bucket']] = set([file['key']])

        self.logger.info(f"Found files to delete in {len(files.keys())} locations.")

        return files, delete_requests


    def _process_file_structure(self, files: dict):
        for location in files.keys():

            self.logger.info(f"Location {location} contains {len(files[location].keys())} buckets with deleteable files.")

            for bucket in files[location].keys():
                keys = files[location][bucket]
                keys_to_delete = []
                counter = 0

                self.logger.info(f"Bucket {bucket} contains {len(keys)} files to be deleted.")

                for key in keys:
                    keys_to_delete.append(key)
                    counter += 1

                    if counter == 1000:
                        #Delete in batches of 1000
                        self._batch_delete_objects(location, bucket, keys_to_delete)
                        keys_to_delete = []
                
                #Delete any that are left!
                if keys_to_delete:
                    self._batch_delete_objects(location, bucket, keys_to_delete)


    def _process_delete_requests(self, delete_requests):
        for delete_request_id in delete_requests:
            self.logger.info(f"Reviewing delete request {delete_request_id}.")
            
            delete_request_has_missing_files = False

            for obs_id in delete_requests[delete_request_id]:
                obs_undeleted_files = self.repository.get_undeleted_files_from_obs_id(obs_id)

                if len(obs_undeleted_files) > 0:
                    self.logger.info(f"obs_id {obs_id} has {len(obs_undeleted_files)} files that are not deleted.")
                    delete_request_has_missing_files = True
                else:
                    self.logger.info(f"Updating obs_id {obs_id} to deleted.")
                    self.repository.set_obs_id_to_deleted(obs_id)

            if not delete_request_has_missing_files:
                self.logger.info(f"Updating delete request {delete_request_id} to actioned.")
                self.repository.set_delete_request_to_actioned(delete_request_id)


    def run(self):
        self.logger.info("Starting delete processor.")

        files, delete_requests = self._generate_data_structures()

        self._process_file_structure(files)

        self._process_delete_requests(delete_requests)



class ProcessorFactory():
    def __init__(self, args, connection=None):
        self.args = args
        self.connection = connection


    def get_processor(self) -> Processor:
        subcommand = self.args.subcommand
        match subcommand:
            case 'delete':
                return DeleteProcessor(self.args, self.connection)
            case _:
                raise ValueError(f"Missing or invalid subcommand {subcommand}.")