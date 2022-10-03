import sys
import atexit
import logging

from abc import abstractmethod, ABC
from collections import defaultdict
from configparser import ConfigParser

import boto3
from psycopg_pool import ConnectionPool

from sql import (
    get_delete_requests, 
    get_obs_ids_for_delete_request, 
    get_obs_data_files_filenames_except_ppds, 
    get_undeleted_files_from_obs_id,
    set_obs_id_to_deleted,
    set_delete_request_to_actioned
)

locations = {
    2: 'acacia',
    3: 'banksia'
}

class Processor(ABC):
    def __init__(self, args):
        self.verbose = args.verbose
        self.dry_run = args.dry_run
        self.logger = logging.getLogger()

        if self.verbose:
            self.logger.setLevel(logging.INFO)
        else:
            self.logger.setLevel(logging.WARN)

        self.config = self._read_config(args.cfg)

        self.pool = self._setup_pool()

        atexit.register(self._close_pool)
    

    def _read_config(self, file_name):
        self.logger.info("Parsing config file.")

        config = ConfigParser()

        try:
            with open(file_name) as f:
                config.read_file(f)

            return config
        except (IOError, FileNotFoundError):
            self.logger.error("Could not parse config file.")
            sys.exit(1)


    def _setup_pool(self):
        self.logger.info("Setting up database pool.")

        db_config = {
            'host': self.config.get("mro_metadata_db", "host"),
            'port': self.config.get("mro_metadata_db", "port"),
            'name': self.config.get("mro_metadata_db", "db"),
            'user': self.config.get("mro_metadata_db", "user"),
            'pass': self.config.get("mro_metadata_db", "pass"),
        }

        dsn = f"postgresql://{db_config['user']}:{db_config['pass']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"
        
        pool = ConnectionPool(
            min_size=2,
            max_size=10,
            conninfo=dsn
        )

        self.logger.info("Connection succesful.")

        return pool


    def _close_pool(self):
        self.logger.info("Closing database pool.")
        self.pool.close()

    
    @abstractmethod
    def run(self):
        raise NotImplementedError


class DeleteProcessor(Processor):
    def __init__(self, args):
        super().__init__(args)


    #TODO remove all of the DB stuff into its own repository class
    def _get_delete_requests(self):
        return get_delete_requests(self.pool)

    
    def _get_obs_ids_for_delete_request(self, delete_request_id):
        return get_obs_ids_for_delete_request(self.pool, delete_request_id)


    def _get_files_from_obs_id(self, obs_id):
        return get_obs_data_files_filenames_except_ppds(self.pool, obs_id)


    def _get_undeleted_files_from_obs_id(self, obs_id):
        return get_undeleted_files_from_obs_id(self.pool, obs_id)


    def _set_obs_id_to_deleted(self, obs_id):
        return set_obs_id_to_deleted(self.pool, obs_id)


    def _set_delete_request_to_actioned(self, obs_id):
        return set_delete_request_to_actioned(self.pool, obs_id)


    def batch_delete_objects(self, location, bucket, keys_to_delete):
        #TODO make sure that deleted_timestamp is not updated if a file is deleted again
        #Either only query files that have not already been deleted or find some other way

        if self.dry_run:
            return

        confirm = input(f"Are you sure you would like to delete {len(keys_to_delete)} from {locations[location]}:{bucket} (y/n)")

        if confirm != 'y':
            return

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
            return

        if s3 == None:
            return

        bucket_object = s3.Bucket(bucket)

        objects_to_delete = []
        for key in keys_to_delete:
            objects_to_delete.append({'Key': key})

        filenames = [key.split('/')[-1] for key in keys_to_delete]

        if self.pool():
            self.pool.check()

            with self.pool.connection(autocommit=True) as conn:
                cur = conn.cursor()
                with conn.transaction():

                    sql = """
                    UPDATE data_files SET
                    deleted = TRUE, deleted_timestamp = NOW()
                    WHERE filename IN ('%s')
                    """
                
                    cur.execute(sql, ("','".join(filenames)))

                    bucket_object.delete_objects(
                        Delete={
                            'Objects': objects_to_delete
                        }
                    )


    def _generate_data_structures(self) -> dict:
        delete_requests_ids = self._get_delete_requests()
        num_delete_requests = len(delete_requests_ids)

        self.logger.info(f"Found {num_delete_requests} delete requests.")

        files = defaultdict(dict)
        delete_requests = defaultdict(list)

        for index, delete_request_id in enumerate(delete_requests_ids):
            obs_ids = self._get_obs_ids_for_delete_request(delete_request_id)

            self.logger.info(f"Processing delete request ({index + 1}/{num_delete_requests}).")

            self.logger.info(f"Delete request {delete_request_id} contains {len(obs_ids)} obs_ids.")

            for obs_id in obs_ids:
                obs_files = self._get_files_from_obs_id(obs_id)

                self.logger.info(f"Processing obs_id {obs_id}.")
                self.logger.info(f"obs_id {obs_id} contains {len(obs_files)} files.")

                delete_requests[delete_request_id].append(obs_id)

                for file in obs_files:
                    if file['bucket'] in files[file['location']]:
                        files[file['location']]['bucket'].add(file['key'])
                    else:
                       files[file['location']]['bucket'] = set(file['key'])

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
                        self.batch_delete_objects(location, bucket, keys)
                        keys_to_delete = []
                
                #Delete any that are left!
                self.batch_delete_objects(location, bucket, keys)


    def _process_delete_requests(self, delete_requests):
        for delete_request_id in delete_requests:
            dr_missing_files = False

            for obs_id in delete_requests[delete_request_id]:
                obs_undeleted_files = self._get_undeleted_files_from_obs_id(obs_id)

                if len(obs_undeleted_files) > 0:
                    self.logger.info(f"obs_id {obs_id} has {len(obs_undeleted_files)} files that are not deleted.")
                    dr_missing_files = True
                else:
                    self.logger.info(f"Updating obs_id {obs_id} to deleted.")
                    self._set_obs_id_to_deleted(obs_id)

            if not dr_missing_files:
                self.logger.info(f"Updating delete request {delete_request_id} to actioned.")
                self._set_delete_request_to_actioned(delete_request_id)


    def run(self):
        self.logger.info("Starting delete processor.")

        files, delete_requests = self._generate_data_structures()

        self._process_file_structure(files)

        self._process_delete_requests(delete_requests)



class ProcessorFactory():
    def __init__(self, args):
        self.args = args

    def get_processor(self) -> Processor:
        subcommand = self.args.subcommand
        match subcommand:
            case 'delete':
                return DeleteProcessor(self.args)
            case _:
                raise ValueError(f"Missing or invalid subcommand {subcommand}.")