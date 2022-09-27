import sys
import atexit
import logging

from abc import abstractmethod, ABC
from collections import defaultdict
from configparser import ConfigParser

import boto3
from psycopg_pool import ConnectionPool

from sql import get_delete_requests, get_obs_ids_for_delete_request, get_obs_data_files_filenames_except_ppds

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
        pass


class DeleteProcessor(Processor):
    def __init__(self, args):
        super().__init__(args)


    def _get_delete_requests(self):
        return get_delete_requests(self.pool)

    
    def _get_obs_ids_for_delete_request(self, delete_request_id):
        return get_obs_ids_for_delete_request(self.pool, delete_request_id)


    def _get_files_from_obs_id(self, obs_id):
        return get_obs_data_files_filenames_except_ppds(self.pool, obs_id)


    def batch_delete_objects(self, bucket_object, keys_to_delete):

        #TODO maybe add a user prompt here?
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


    def run(self):
        self.logger.info("Starting delete processor.")

        delete_requests = self._get_delete_requests()
        num_delete_requests = len(delete_requests)

        self.logger.info(f"Found {num_delete_requests} delete requests.")

        files = defaultdict(dict)

        for delete_request_id in delete_requests:
            obs_ids = self._get_obs_ids_for_delete_request(delete_request_id)

            self.logger.info(f"Processing delete request ({delete_request_id}/{num_delete_requests}).")

            self.logger.info(f"Delete request {delete_request_id} contains {len(obs_ids)} obs_ids.")

            for obs_id in obs_ids:
                obs_files = self._get_files_from_obs_id(obs_id)

                self.logger.info(f"Processing obs_id {obs_id}.")

                for file in obs_files:
                    if file['bucket'] in files[file['location']]:
                        files[file['location']]['bucket'].append(file['key'])
                    else:
                       files[file['location']]['bucket'] = [file['key']]


        for location in files.keys():
            match location:
                case 2:
                    #Acacia
                    s3 = boto3.resource(
                        's3',
                        aws_access_key_id=self.config.get('acacia', 'aws_access_key_id'),
                        aws_secret_access_key=self.config.get('acacia', 'aws_secret_access_key'),
                        endpoint_url=self.config.get('acacia', 'endpoint_url')
                    )
                case 3:
                    #Banksia
                    s3 = boto3.resource(
                        's3',
                        aws_access_key_id=self.config.get('banksia', 'aws_access_key_id'),
                        aws_secret_access_key=self.config.get('banksia', 'aws_secret_access_key'),
                        endpoint_url=self.config.get('banksia', 'endpoint_url')
                    )
                case _:
                    break


            for bucket in files[location].keys():
                bucket_object = s3.Bucket(bucket)

                keys = files[location][bucket]
                keys_to_delete = []
                counter = 0
                for key in keys:
                    keys_to_delete.append(key)
                    counter += 1

                    if counter == 1000:
                        self.batch_delete_objects(bucket_object, keys_to_delete)
                        keys_to_delete = []
                    
                self.batch_delete_objects(bucket_object, keys_to_delete)

        #TODO check which obs should be marked as deleted


class ProcessorFactory():
    def __init__(self, args):
        self.args = args

    def get_processor(self) -> DeleteProcessor:
        subcommand = self.args.subcommand
        match subcommand:
            case 'delete':
                return DeleteProcessor(self.args)
            case _:
                raise ValueError("Missing or invalid subcommand.")