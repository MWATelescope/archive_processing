import atexit

from enum import Enum

import psycopg
from psycopg import ClientCursor
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



class Repository():
    def __init__(self, config, connection):
        self.config = config
        self.conn = self._setup_conn(connection)

        if connection is None:
            #Test database will close cleanly, but need to close our connection in production
            atexit.register(self._close)


    def _setup_conn(self, connection):
        #Optionally pass in an existing psycopg3 connection. Otherwise look in the supplied config file and make a new one.
        if connection is not None:
            return connection

        db_config = {
            'host': self.config.get("database", "host"),
            'port': self.config.get("database", "port"),
            'name': self.config.get("database", "db"),
            'user': self.config.get("database", "user"),
            'pass': self.config.get("database", "pass"),
        }

        dsn = f"postgresql://{db_config['user']}:{db_config['pass']}@{db_config['host']}:{db_config['port']}/{db_config['name']}"

        return psycopg.connect(dsn, cursor_factory=dict_row, autocommit=True)


    def _close(self):
        self.conn.close()

    
    def run_sql_get_one_row(self, sql: str, args: tuple):
        record = None

        try:
            with self.conn.cursor(row_factory=dict_row) as cur:
                if args is None:
                    cur.execute(sql)
                else:
                    cur.execute(sql, args)

                record = cur.fetchone()

        except Exception as error:
            raise error

        return record


    def run_sql_get_many_rows(self, sql: str, args) -> list:
        records = []

        try:
            with self.conn.cursor(row_factory=dict_row) as cur:
                if args is None:
                    cur.execute(sql)
                else:
                    cur.execute(sql, args)

                records = cur.fetchall()

        except Exception as error:
            raise error

        return records


    def run_sql_update(self, sql: str, args) -> None:
        try:
            with self.conn.transaction():
                with self.conn.cursor() as cur:
                    if args is None:
                        cur.execute(sql)
                    else:
                        cur.execute(sql, args)

            self.conn.commit()

        except Exception as error:
            raise error


    def run_function_in_transaction(self, sql, params, fun, *args):
        try:
            with self.conn.transaction():
                with ClientCursor(self.conn) as cur:
                    query = cur.mogrify(sql, params)
                    cur.execute(query)

                    fun(*args)

            self.conn.commit()
        except Exception as error:
            raise error


class DeleteRepository(Repository):
    def __init__(self, config, connection):
        super().__init__(config, connection)


    def get_delete_requests(self):
        sql = """SELECT id
                FROM deletion_requests
                WHERE cancelled_datetime IS NULL
                AND actioned_datetime IS NULL
                ORDER BY created_datetime"""

        # Execute query
        params = None
        results = self.run_sql_get_many_rows(sql, params)

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
        results = self.run_sql_get_many_rows(sql, params)

        if results:
            return [r["obs_id"] for r in results]
        else:
            return []


    def get_obs_data_files_filenames_except_ppds(self, obs_id: int) -> list:
        sql = """SELECT location, bucket, CONCAT_WS('', folder, filename) as key, filename
                FROM data_files
                WHERE filetype NOT IN (%s)
                AND observation_num = %s
                AND deleted_timestamp IS NULL
                AND remote_archived = True
                ORDER BY filename"""

        results = self.run_sql_get_many_rows(
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
                    'key': r["key"],
                    'filename': r["filename"]
                } 
                for r in results
            ]
        else:
            return []


    def update_files_to_deleted(self, delete_func, bucket, keys):
        sql = """UPDATE data_files 
                SET deleted = TRUE, 
                deleted_timestamp = NOW() 
                WHERE filename = ANY(%s);"""

        params = [[key.split('/')[-1] for key in keys]]

        self.run_function_in_transaction(sql, params, delete_func, bucket, keys)


    def get_undeleted_files_from_obs_id(self, obs_id: int) -> list:
        sql = """SELECT filename
                FROM data_files
                WHERE observation_num = %s
                AND deleted_timestamp IS NULL"""

        # Execute query
        params = (obs_id,)
        results = self.run_sql_get_many_rows(sql, params)

        if results:
            return [r["filename"] for r in results]
        else:
            return []


    def set_obs_id_to_deleted(self, obs_id: int):
        sql = """UPDATE mwa_setting
                SET deleted_timestamp = NOW(),
                deleted = TRUE
                WHERE starttime = %s"""

        # Execute query
        params = (obs_id,)
        self.run_sql_update(sql, params)

    
    def set_delete_request_to_actioned(self, delete_request_id: int):
        sql = """UPDATE deletion_requests
                SET actioned_datetime = NOW()
                WHERE id = %s"""

        # Execute query
        params = (delete_request_id,)
        self.run_sql_update(sql, params)