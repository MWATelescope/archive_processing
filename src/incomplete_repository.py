import logging
from psycopg import Connection
from repository import Repository

logger = logging.getLogger()


class IncompleteRepository(Repository):
    def __init__(
        self,
        dsn: str | None = None,
        connection: Connection | None = None,
        dry_run: bool = False,
    ):
        super().__init__(
            dsn=dsn,
            connection=connection,
            webservices_url=None,
            dry_run=dry_run,
        )

    def get_data_file_checksum(self, observation_num: int, filename: str) -> str:
        """
        Function to return checksum of archived MWA data file.

        Parameters
        ----------
        filename: str
            Filename of an archived MWA data file.

        Returns
        -------
        str:
            md5 checksum of file from database.
        """

        sql = """SELECT checksum
                FROM data_files
                WHERE
                filename = %s AND
                observation_num = %s"""

        params = (
            filename,
            observation_num,
        )

        results = self.run_sql_get_one_row(sql, params)

        if results:
            return results["checksum"]
        else:
            return []
