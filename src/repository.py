import atexit

from enum import Enum
from typing import Callable
from configparser import ConfigParser

import psycopg
from psycopg import ClientCursor, Connection
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
    def __init__(self, config: ConfigParser, connection: Connection | None):
        self.config = config
        self.conn = self._setup_conn(connection)

        if connection is None:
            # Test database will close cleanly causing this to fail, so need to check if our connection was initially None or not.
            atexit.register(self._close)

    def _setup_conn(self, connection: Connection | None) -> Connection:
        # Optionally pass in an existing psycopg3 connection. Otherwise look in the supplied config file and make a new one.
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

    def _close(self) -> None:
        """
        Function to close postgres connection on shutdown.
        """
        self.conn.close()

    def run_sql_get_one_row(self, sql: str, args: tuple) -> dict:
        """
        Execute the provided sql with args, and return a single result.

        Parameters
        ----------
        sql: str
            String of the query to pass to the server
        args: tuple
            Arguments which will be substituted into the sql string.

        Returns
        -------
        dict
            The result of the query from the server
        """
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

    def run_sql_get_many_rows(self, sql: str, args: tuple) -> list[dict]:
        """
        Execute the provided sql with args, and return a list of results.

        Parameters
        ----------
        sql: str
            String of the query to pass to the server
        args: tuple
            Arguments which will be substituted into the sql string.

        Returns
        -------
        list[dict]
            The result of the query from the server
        """
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

    def run_sql_update(self, sql: str, args: tuple) -> None:
        """
        Execute the provided sql with args. For updates only.

        Parameters
        ----------
        sql: str
            String of the query to pass to the server
        args: tuple
            Arguments which will be substituted into the sql string.
        """
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

    def run_function_in_transaction(self, sql: str, params: list, fun: Callable, *args) -> None:
        """
        Run the provided sql (with its params) in side of a transaction.
        Then, run fun(*args). If this fails, the transaction will be rolled back.

        Parameters
        ----------
        sql: str
            String of the query to pass to the server
        params: list
            Arguments which will be substituted into the sql string.
        fun: Callable
            Function which is to run inside of the transaction
        *args:
            Arguments for the provided fun
        """
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
    def __init__(self, config: ConfigParser, connection: Connection):
        super().__init__(config, connection)

    def get_delete_requests(self) -> list:
        """
        Function to return a list of all not-cancelled unactioned delete request ids.

        Returns
        -------
        list:
            List of delete request ids
        """
        sql = """SELECT id
                FROM deletion_requests
                WHERE cancelled_datetime IS NULL
                AND actioned_datetime IS NULL
                ORDER BY created_datetime"""

        params = None
        results = self.run_sql_get_many_rows(sql, params)

        if results:
            return [r["id"] for r in results]
        else:
            return []

    def get_obs_ids_for_delete_request(self, delete_request_id: int) -> list:
        """
        Function to return a list of obs_ids associated with a given delete request

        Parameters
        ----------
        delete_request_id: int
            The id of the delete request for which to fetch associated obs_ids.

        Returns
        -------
        list:
            List of obs_ids associated with the delete request.
        """
        sql = """SELECT obs_id
                FROM deletion_request_observation
                WHERE request_id = %s
                ORDER BY obs_id"""

        params = (delete_request_id,)
        results = self.run_sql_get_many_rows(sql, params)

        if results:
            return [r["obs_id"] for r in results]
        else:
            return []

    def get_obs_data_files_filenames_except_ppds(self, obs_id: int) -> list:
        """
        Function to return a list of files associated with a given obs_id (excluding PPDs).

        Parameters
        ----------
        obs_id: int
            The obs_id for which to fetch associated files.

        Returns
        -------
        list:
            List of files associated with the given obs_id.
        """
        sql = """SELECT location, bucket, CONCAT_WS('', folder, filename) as key, filename, deleted
                FROM data_files
                WHERE filetype NOT IN (%s)
                AND observation_num = %s
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
                    'filename': r["filename"],
                    'deleted': r["deleted"]
                } 
                for r in results
            ]
        else:
            return []

    def update_files_to_deleted(self, delete_func: Callable, bucket, keys: list) -> None:
        """
        Given a provided function to actually delete files, an S3 bucket, and a list of keys:
        Determine filenames from the list of keys.
        Create the SQL which will cause filenames to be marked as deleted.
        Pass everything to another function which will open a transaction, run the command, run the delete function, and rollback if anything went wrong.

        Parameters
        ----------
        delete_func: Callable
            The function which will actually go and delete the provided keys in bucket
        bucket:
            The bucket which contains the keys to be deleted
        keys: list
            The list of keys in the bucket which will be deleted.
        """
        sql = """UPDATE data_files 
                SET deleted = TRUE, 
                deleted_timestamp = NOW() 
                WHERE filename = ANY(%s);"""

        params = [[key.split('/')[-1] for key in keys]]

        self.run_function_in_transaction(sql, params, delete_func, bucket, keys)

    def set_obs_id_to_deleted(self, obs_id: int) -> None:
        """
        Mark a given obs_id as deleted.

        Parameters
        ----------
        obs_id: int
            The obs_id to mark as deleted.
        """
        sql = """UPDATE mwa_setting
                SET deleted_timestamp = NOW(),
                deleted = TRUE
                WHERE starttime = %s"""

        params = (obs_id,)
        self.run_sql_update(sql, params)

    def set_delete_request_to_actioned(self, delete_request_id: int) -> None:
        """
        Function to mark a delete request as actioned, given its id.

        Parameters
        ----------
        delete_request_id: int
            The id of the delete request which will be marked as actioned.
        """
        sql = """UPDATE deletion_requests
                SET actioned_datetime = NOW()
                WHERE id = %s"""

        # Execute query
        params = (delete_request_id,)
        self.run_sql_update(sql, params)