import sys
import atexit
import logging
from enum import Enum
from typing import Callable

import psycopg
import requests
from psycopg import ClientCursor, Connection
from psycopg.rows import dict_row

logger = logging.getLogger()


class MWAFileTypeFlags(Enum):
    GPUBOX_FILE = 8
    FLAG_FILE = 10
    VOLTAGE_RAW_FILE = 11
    MWA_PPD_FILE = 14
    VOLTAGE_ICS_FILE = 15
    VOLTAGE_RECOMBINED_ARCHIVE_FILE = 16
    MWAX_VOLTAGES = 17
    MWAX_VISIBILITIES = 18


class Repository:
    def __init__(
        self,
        dsn: str | None = None,
        connection: Connection | None = None,
        webservices_url: str | None = None,
        dry_run: bool = False,
    ):
        self.dry_run = dry_run
        self.webservices_url = webservices_url
        self.conn = self._setup_conn(dsn, connection)

        if connection is None:
            # Test database will close cleanly causing this to fail, so need to check if our connection was initially None or not.
            atexit.register(self._close)

    def _setup_conn(self, dsn: str | None, connection: Connection | None) -> Connection:
        # Optionally pass in an existing psycopg3 connection. Otherwise look in the supplied config file and make a new one.
        if connection is not None:
            return connection

        try:
            logger.info("Connecting to database.")
            return psycopg.connect(dsn, row_factory=dict_row, autocommit=True)
        except Exception:
            logger.error("Could not connect to database.")
            sys.exit(1)

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
        if self.dry_run:
            logger.info("Would have ran the below SQL with args:")
            logger.info(sql)
            logger.info(args)
            return

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

    def run_function_in_transaction(self, sql: str, fun: Callable, *args) -> None:
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
        if self.dry_run:
            logger.info("Would have ran the below SQL with args:")
            logger.info(sql)
            logger.info(args)
            return

        try:
            with self.conn.transaction():
                with ClientCursor(self.conn) as cur:
                    params = fun(*args)

                    query = cur.mogrify(sql, params)
                    cur.execute(query)

            self.conn.commit()
        except Exception as error:
            raise error

    def ws_request(self, url: str, data: dict) -> dict:
        """
        Send a post request to webservices.

        Parameters
        ----------
        url: str
            URL relative to the base as defined in the config file. Should be prefixed with a /
        data: dict
            Data dictionary to send with the request

        Returns
        -------
        JSON response from the server
        """
        try:
            response = requests.post(f"{self.webservices_url}{url}", json=data)

            if response.status_code != 200:
                raise Exception(
                    "Webservices request failed. Got status code"
                    f" {response.status_code}."
                )
            else:
                return response.json()
        except Exception:
            raise


class DeleteRepository(Repository):
    def __init__(
        self,
        dsn: str | None = None,
        connection: Connection | None = None,
        webservices_url: str | None = None,
        dry_run: bool = False,
    ):
        super().__init__(
            dsn=dsn,
            connection=connection,
            webservices_url=webservices_url,
            dry_run=dry_run,
        )

    def get_delete_requests(
        self, optional_request_id_list: list[int] | None = None
    ) -> list:
        """
        Function to return a list of all not-cancelled unactioned delete request ids.

        Parameters
        ----------
        optional_request_id_list: list[int]
            An optional list of id's to delete. If None assume ALL will be processed

        Returns
        -------
        list:
            List of delete request ids
        """

        if optional_request_id_list:
            sql = """SELECT id
                    FROM deletion_requests
                    WHERE id ANY(%s)
                    AND cancelled_datetime IS NULL
                    AND actioned_datetime IS NULL
                    AND approved_datetime IS NOT NULL
                    ORDER BY created_datetime"""

            params = (optional_request_id_list,)
        else:
            sql = """SELECT id
                    FROM deletion_requests
                    WHERE cancelled_datetime IS NULL
                    AND actioned_datetime IS NULL
                    AND approved_datetime IS NOT NULL
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

    def validate_obsids(self, obs_ids: list) -> list:
        """
        Function to validate a given list of obs_ids, ensuring that they are eligible to be deleted.
        Anything that is a calibrator, or in an existing collection is invalid.

        Parameters
        ----------
        obs_ids: list
            The list of obs_ids to validate

        Returns
        -------
        list:
            A list of obs_ids which are ineligible to be deleted.
        """

        data = {"obsids": obs_ids, "with_reasons": False}

        return self.ws_request("/validate_obsids", data)

    def get_not_deleted_obs_data_files_except_ppds(self, obs_id: int) -> list:
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
        sql = """SELECT location, bucket, CONCAT_WS('', folder, filename) as key, filename
                FROM data_files
                WHERE filetype NOT IN (%s)
                AND observation_num = %s
                AND remote_archived = True
                AND deleted_timestamp IS NULL
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
                    "location": r["location"],
                    "bucket": r["bucket"],
                    "key": r["key"],
                    "filename": r["filename"],
                }
                for r in results
            ]
        else:
            return []

    def update_files_to_deleted(
        self, delete_func: Callable, bucket, keys: list
    ) -> None:
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
                SET deleted_timestamp = NOW()
                WHERE deleted_timestamp IS NULL
                AND filename = ANY(%s);"""

        self.run_function_in_transaction(sql, delete_func, bucket, keys)

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
                WHERE deleted_timestamp IS NULL
                AND starttime = %s"""

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
                WHERE actioned_datetime IS NULL
                AND id = %s"""

        # Execute query
        params = (delete_request_id,)
        self.run_sql_update(sql, params)
