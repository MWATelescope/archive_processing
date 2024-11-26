import logging
from psycopg import Connection
from typing import Callable, Optional
from mwa_utils import MWAFileTypeFlags
from repository import Repository

logger = logging.getLogger("archive_processing")


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

    def get_delete_requests(self, optional_request_id_list: list[int] | None = None) -> list:
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
                    WHERE id = ANY(%s)
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

    def get_filetype_id_for_delete_request(self, delete_request_id: int) -> Optional[int]:
        """
        Function to return a specific filetype_id or None for a given delete request.
        None means- get rid of the whole observation, whereas a filetype_id means
        just get rid of this type of file from the observation.

        Parameters
        ----------
        delete_request_id: int
            The id of the delete request for which to fetch filetype_id.

        Returns
        -------
        int | None:
            Filetypeid or None
        """
        sql = """SELECT filetype_id
                FROM deletion_requests
                WHERE id = %s"""

        params = (delete_request_id,)
        result = self.run_sql_get_one_row(sql, params)

        if result:
            if result["filetype_id"]:
                return int(result["filetype_id"])
            else:
                return None
        else:
            raise Exception(f"Delete Request {delete_request_id} not found")

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

    def get_not_deleted_obs_data_files_except_ppds(self, obs_id: int, filetype_id: Optional[int] = None) -> list:
        """
        Function to return a list of files associated with a given obs_id (excluding PPDs).
        If a filetype_id is specified, limit to just that filetype_id

        Parameters
        ----------
        obs_id: int
            The obs_id for which to fetch associated files.

        Returns
        -------
        list:
            List of files associated with the given obs_id.
        """
        if filetype_id:
            # Only this filetype_id
            sql = """SELECT location, bucket, CONCAT_WS('', folder, filename) as key, filename
                    FROM data_files
                    WHERE filetype = %s
                    AND observation_num = %s
                    AND remote_archived = True
                    AND deleted_timestamp IS NULL
                    ORDER BY filename"""
        else:
            # all filetypes except the metafits PPD
            sql = """SELECT location, bucket, CONCAT_WS('', folder, filename) as key, filename
                    FROM data_files
                    WHERE filetype NOT IN (%s)
                    AND observation_num = %s
                    AND remote_archived = True
                    AND deleted_timestamp IS NULL
                    ORDER BY filename"""
            filetype_id = MWAFileTypeFlags.MWA_PPD_FILE.value

        results = self.run_sql_get_many_rows(
            sql,
            (
                filetype_id,
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

    def update_files_to_deleted(self, delete_func: Callable, bucket, keys: list) -> None:
        """
        Given a provided function to actually delete files, an S3 bucket, and a list of keys:
        Determine filenames from the list of keys.
        Create the SQL which will cause filenames to be marked as deleted.
        Pass everything to another function which will open a transaction, run the command,
        run the delete function, and rollback if anything went wrong.

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
                AND filename = ANY(%s)
                AND observation_num = ANY(%s);"""

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
