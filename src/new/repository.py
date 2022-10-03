import sys
import json

from abc import abstractmethod, ABC
from enum import Enum

from psycopg_pool import ConnectionPool
from psycopg.rows import dict_row


class MWAFileTypeFlags(Enum):
    GPUBOX_FILE = 8
    FLAG_FILE = 10
    VOLTAGE_RAW_FILE = 11
    MWA_PPD_FILE = 14
    VOLTAGE_ICS_FILE = 15
    VOLTAGE_RECOMBINED_ARCHIVE_FILE = 16
    MWAX_VOLTAGES = 17
    MWAX_VISIBILITIES = 18



class RepositoryFactory():
    def __init__(self, type, config):
        self.type = type
        self.config = config

    def get_repository(self):
        match self.type:
            case 'postgres':
                return PostgresRepository(self.config)
            case 'json':
                return JsonRepository(self.config)
            case _:
                raise NotImplementedError


class Repository(ABC):
    @abstractmethod
    def get_delete_requests(self) -> list:
        raise NotImplementedError

    @abstractmethod
    def get_obs_ids_for_delete_request(self, delete_request_id: int) -> list:
        raise NotImplementedError

    @abstractmethod
    def get_obs_data_files_filenames_except_ppds(self, obs_id: int) -> list:
        raise NotImplementedError

    @abstractmethod
    def get_undeleted_files_from_obs_id(self, obs_id: int) -> list:
        raise NotImplementedError

    @abstractmethod
    def set_obs_id_to_deleted(self) -> None:
        raise NotImplementedError

    @abstractmethod
    def set_delete_request_to_actioned(self) -> None:
        raise NotImplementedError


class PostgresRepository(Repository):
    def __init__(self):
        self.pool = self._setup_pool()

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

    
    def run_sql_get_one_row(self, sql: str, args: tuple):
        cur = None
        record = None

        try:
            if self.pool:
                self.pool.check()

                with self.pool.connection() as conn:
                    cur = conn.cursor(row_factory=dict_row)

                    if args is None:
                        cur.execute(sql)
                    else:
                        cur.execute(sql, args)

                    record = cur.fetchone()
            else:
                raise Exception("database pool is not initialised")

        except Exception as error:
            raise error

        finally:
            if cur:
                cur.close()

        return record


    def run_sql_get_many_rows(self, sql: str, args) -> list:
        records = []
        cur = None

        try:
            if self.pool:
                self.pool.check()

                with self.pool.connection() as conn:
                    cur = conn.cursor(row_factory=dict_row)

                    if args is None:
                        cur.execute(sql)
                    else:
                        cur.execute(sql, args)

                    records = cur.fetchall()
            else:
                raise Exception("database pool is not initialised")

        except Exception as error:
            raise error

        finally:
            if cur:
                cur.close()
        return records


    def run_sql_update(self, sql: str, args) -> None:
        records = []
        cur = None

        try:
            if self.pool:
                self.pool.check()

                with self.pool.connection() as conn:
                    cur = conn.cursor(row_factory=dict_row)

                    if args is None:
                        cur.execute(sql)
                    else:
                        cur.execute(sql, args)
            else:
                raise Exception("database pool is not initialised")

        except Exception as error:
            raise error

        finally:
            if cur:
                cur.close()


    def get_delete_requests(self):
        #TODO add AND WHERE actioned_datetime is NULL
        sql = """SELECT id
                FROM deletion_requests
                WHERE cancelled_datetime IS NULL
                ORDER BY created_datetime"""

        # Execute query
        params = None
        results = self.run_sql_get_many_rows(self.pool, sql, params)

        if results:
            return [r["id"] for r in results]
        else:
            return []


    def get_obs_ids_for_delete_request(self, delete_request_id: int) -> list:
        sql = """SELECT obs_id
                FROM deletion_request_observation
                WHERE request_id = %s
                ORDER BY obs_id"""

        # Execute query
        params = (delete_request_id,)
        results = self.run_sql_get_many_rows(self.pool, sql, params)

        if results:
            return [r["obs_id"] for r in results]
        else:
            return []


    def get_obs_data_files_filenames_except_ppds(self, obs_id: int) -> list:
        sql = """SELECT location, bucket, CONCAT_WS('', folder, filename) as key
                FROM data_files
                WHERE filetype NOT IN (%s)
                AND observation_num = %s
                AND deleted_timestamp IS NULL
                AND remote_archived = True
                ORDER BY filename"""

        results = self.run_sql_get_many_rows(
            self.pool,
            sql,
            (
                MWAFileTypeFlags.MWA_PPD_FILE.value,
                int(obs_id),
            ),
        )

        if results:
            return [
                {
                    'location': r["location"], 
                    'bucket': r["bucket"], 
                    'key': r["key"]
                } 
                for r in results
            ]
        else:
            return []


    def get_undeleted_files_from_obs_id(self, obs_id: int) -> list:
        sql = """SELECT filename
                FROM data_files
                WHERE observation_num = %s
                AND deleted_timestamp IS NULL"""

        # Execute query
        params = (obs_id,)
        results = self.run_sql_get_many_rows(self.pool, sql, params)

        if results:
            return [r["filename"] for r in results]
        else:
            return []


    def set_obs_id_to_deleted(self, obs_id: int):
        sql = """UPDATE mwa_setting
                SET deleted_timestamp = NOW()
                WHERE starttime = %s"""

        # Execute query
        params = (obs_id,)
        self.run_sql_update(self.pool, sql, params)

    
    def set_delete_request_to_actioned(self, delete_request_id: int):
        sql = """UPDATE delete_requests
                SET actioned_datetime = NOW()
                WHERE id = %s"""

        # Execute query
        params = (delete_request_id,)
        self.run_sql_update(self.pool, sql, params)


class JsonRepository(Repository):
    def __init__(self):
        self.data = self._read_data_from_file(self.config.get('json', 'path'))


    def _read_data_from_file(self, path):
        try:
            with open(path) as f:
                return json.load(f)
        except (IOError, FileNotFoundError):
            self.logger.error("Could not parse data file.")
            sys.exit(1)


    def get_delete_requests(self):
        return self.data['delete_requests'].keys()


    def get_obs_ids_for_delete_request(self, delete_request_id: int) -> list:
        return self.data['delete_requests'][delete_request_id]['obs_ids']


    def get_obs_data_files_filenames_except_ppds(self, obs_id: int) -> list:
        return self.data['obs_ids'][obs_id]['files']


    def get_undeleted_files_from_obs_id(self, obs_id: int) -> list:
        undeleted_files = []

        for file in self.data['obs_ids'][obs_id]['files']:
            if file['deleted'] == False:
                undeleted_files.append(file)

        return undeleted_files


    def set_obs_id_to_deleted(self, obs_id: int) -> None:
        self.data['obs_ids'][obs_id]['delete'] = True


    def set_delete_request_to_actioned(self, delete_request_id: int) -> None:
        self.data['delete_requests'][delete_request_id]['actioned'] = True