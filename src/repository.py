import sys
import atexit
import logging
from typing import Callable

import psycopg
import requests
from psycopg import ClientCursor, Connection
from psycopg.rows import dict_row

logger = logging.getLogger()


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
            # Test database will close cleanly causing this to fail, so need to check
            # if our connection was initially None or not.
            atexit.register(self._close)

    def _setup_conn(self, dsn: str | None, connection: Connection | None) -> Connection:
        # Optionally pass in an existing psycopg3 connection.
        # Otherwise look in the supplied config file and make a new one.
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
