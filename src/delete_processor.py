import base64
import logging
import hashlib
import json
import os
import sys
import time
import boto3
from typing import Optional
import botocore
from botocore.config import Config
from collections import defaultdict
from configparser import ConfigParser
from mwa_utils import locations
from processor import Processor
from delete_repository import DeleteRepository

logger = logging.getLogger("archive_processing")


class DeleteProcessor(Processor):
    def __init__(
        self, repository: DeleteRepository, dry_run: bool = False, config: ConfigParser = None, force: bool = False
    ):
        super().__init__(dry_run)
        self.config = config
        self.repository = repository
        self.force = force
        self.acacia_sleep_time: int = config.getint("delete_processor", "acacia_sleep_time")

    def _parse_ids(self, ids: str | None) -> list | None:
        """
        Given an --ids parameter from the command line, parse it into a list of integers.

        Parameters
        ----------
        ids:
            A user-supplied, comma separated string of the request_ids which should be processed.

        Returns
        -------
            A list of integers, represeting the delete_request_ids that the user has asked to be processed.
        """
        if ids is None:
            return None

        try:
            ids_list = list(map(int, ids.split(",")))
            return ids_list
        except ValueError:
            raise ValueError("Supplied invalid request ID.")

    def _bucket_delete_keys(self, bucket, keys: list[str]) -> list[list]:
        """
        For a given bucket and list of keys to be deleted from that bucket,
        make the S3 call to delete the corresponding objects. Then, return a list of filenames and obsids
        which were deleted.

        Parameters
        ----------
        bucket:
            A resource representing the S3 bucket.
        keys: list[str]
            A list of keys inside the bucket to be deleted.

        Returns
        -------
        A list of filenames which were deleted from the bucket.
        """
        MAX_ATTEMPTS = 3
        current_attempt = 0

        while current_attempt < MAX_ATTEMPTS:
            current_attempt += 1

            try:
                delete_payload={"Objects": [{"Key": key} for key in keys]}
                #payload_bytes = json.dumps(delete_payload).encode('utf-8')
                #md5_digest = hashlib.md5(payload_bytes).digest()
                #md5_b64 = base64.b64encode(md5_digest).decode('utf-8')

                response = bucket.delete_objects(Delete=delete_payload)
                # on success leave the loop
                break

            except botocore.exceptions.ClientError as client_exception:
                logger.warning(
                    f"Boto3 ClientError while calling delete_objects(): {client_exception.response['Error']['Code']}"
                    f":{client_exception.response['Error']['Message']}. "
                    f"Attempt {current_attempt} of {MAX_ATTEMPTS}"
                )

                if current_attempt == 3:
                    raise

            except Exception as other_exception:
                logger.warning(
                    f"Exception while calling delete_objects(): {other_exception}. "
                    f"Attempt {current_attempt} of {MAX_ATTEMPTS}"
                )

                if current_attempt == 3:
                    raise

        deleted_objects = response["Deleted"]

        deleted_keys = [deleted_object["Key"] for deleted_object in deleted_objects]
        # Get just the filenames (no paths)
        # Acacia keys do not have any "folder" info e.g. xxxxxx.fits
        # But some Banksia keys do have folder info e.g. mwa/mfa/ngas_data_volume/date/1/xxxxx.fits
        deleted_filenames = [os.path.split(key)[-1] for key in deleted_keys]

        # Determine a unique set of obsids from all the filenames deleted
        #
        # Due to data_files having a primary key on observatopm_num AND
        # filename, we should extract the observation_num from the filenames
        # and use that in the query
        #
        # We use a set as it will never store duplicates
        obsids_set = set()

        for filename in deleted_filenames:
            # Add the obsid (which is the first 10 chars of the filename)
            obsids_set.add(filename[0:10])

        # Convert our set to a list
        obsids_list = list(obsids_set)

        return [deleted_filenames, obsids_list]

    def _batch_delete_objects(self, location: int, bucket: str, keys_to_delete: list[str]) -> None:
        """
        For a given file location, bucket within that location, and list of keys - delete those keys from the location.

        Parameters
        ----------
        location: int
            Number representing the location of files, usually either Acacia mwaingest (2) or Banksia (3) or Acacia mwa (4)
        bucket: str
            The name of a bucket within location
        keys_to_delete: list[str]
            A list of string keys within bucket which will be deleted
        """

        # If we've received a signal, do not process this batch and exit.
        # Allows in progress batches to complete execution
        if self.terminate:
            logger.warning("Exiting.")
            sys.exit(0)

        try:
            config = Config(connect_timeout=60, retries={"mode": "standard"})

            match locations[location]:
                case "acacia_mwa":
                    s3 = boto3.resource(
                        "s3",
                        aws_access_key_id=self.config.get("acacia_mwa", "aws_access_key_id"),
                        aws_secret_access_key=self.config.get("acacia_mwa", "aws_secret_access_key"),
                        endpoint_url=self.config.get("acacia_mwa", "endpoint_url"),
                        config=config,
                    )
                case "banksia":
                    s3 = boto3.resource(
                        "s3",
                        aws_access_key_id=self.config.get("banksia", "aws_access_key_id"),
                        aws_secret_access_key=self.config.get("banksia", "aws_secret_access_key"),
                        endpoint_url=self.config.get("banksia", "endpoint_url"),
                        config=config,
                    )
                case _:
                    raise ValueError(f"Invalid location found {location}.")

            bucket_object = s3.Bucket(bucket)

        except Exception as e:
            logger.warning(f"Could not connect to {location}:{bucket}.")
            logger.warning(e)
            raise

        if self.dry_run:
            logger.info(f"Would have deleted {len(keys_to_delete)} files from" f" {locations[location]}:{bucket}.")
            return
        else:
            logger.info(f"Deleting {len(keys_to_delete)} files from" f" {locations[location]}:{bucket}.")
            logger.info(keys_to_delete)

        # If this fails, should we exit? 🤔
        self.repository.update_files_to_deleted(self._bucket_delete_keys, bucket_object, keys_to_delete)

    def _generate_data_structures(self, delete_request_ids: list[int] | None = None) -> dict:
        """
        Algorithm to generate two data structures which will later be processed.
        Foreach delete request:
            Foreach obs_id in delete request:
                Foreach file (location, bucket, key) in obs_id

        Parameters
        ----------
        delete_request_ids: list[int]
            An optional list of delete request IDs

        Returns
        ----------
        files: dict
            Of the format:
            {
                location_id_2: {
                    bucket1: (path/1.fits, path/2.fits),
                    bucket2: (path/1.fits, path/2.fits)
                },
                location_id_3: {
                    bucket3: (path/1.fits, path/2.fits),
                    bucket4: (path/1.fits, path/2.fits)
                }
            }
        delete_request: dict
            Of the format:
            {
                delete_request_id_1: [obs_id_1, obs_id_2],
                delete_request_id_2: [obs_id_3, obs_id_4]
            }
        """

        # If the user has supplied delete requests via the CLI, use those, but still run them through
        # the get_delete_requests function so that only ids which are approved, not cancelled and not actioned
        # are actioned. Otherwise, fetch them from the repository.
        delete_request_ids = self.repository.get_delete_requests(delete_request_ids)

        num_delete_requests = len(delete_request_ids)

        logger.info(f"Found {num_delete_requests} delete requests.")

        delete_requests = defaultdict(list)
        files = defaultdict(lambda: defaultdict(set))

        for index, delete_request_id in enumerate(delete_request_ids):
            # Check to see if there is a filetype_id specified for this delete_request
            filetype_id: Optional[int] = self.repository.get_filetype_id_for_delete_request(delete_request_id)
            # Get the obsids for this delete request
            obs_ids = self.repository.get_obs_ids_for_delete_request(delete_request_id)
            invalid_obs_ids = self.repository.validate_obsids(obs_ids)

            logger.info(f"Processing delete request ({index + 1}/{num_delete_requests}).")
            logger.info(f"Delete request {delete_request_id} contains {len(obs_ids)} obs_ids.")
            if filetype_id:
                logger.info(f"Delete request {delete_request_id} is for filetype_id {filetype_id} only.")

            if invalid_obs_ids and not self.force:
                # Shouldn't happen often, but catching the case where an observation was added to
                # a collection after the delete request was created, or where
                # calibrator obs are completely useless.
                logger.error(
                    f"Delete request {delete_request_id} contains observations that"
                    f" cannot be deleted. Invalid obsids: {invalid_obs_ids}. Please check and try again, "
                    "or pass the --force argument."
                )
                sys.exit(0)
            elif invalid_obs_ids and self.force:
                logger.warning(
                    f"Delete request {delete_request_id} contains observations that"
                    " can only be deleted with the --force argument passed in. Obsids: "
                    f"{invalid_obs_ids}. These obsids will be deleted as FORCED is TRUE"
                )

            for index, obs_id in enumerate(obs_ids):
                obs_files = self.repository.get_not_deleted_obs_data_files_except_ppds(obs_id, filetype_id)

                # Even if there are 0 files (for the whole obs or just this filetype) to delete
                # we still want to include this obsid
                delete_requests[delete_request_id].append(obs_id)

                logger.info(f"Processing obs_id {obs_id} ({index + 1}/{len(obs_ids)}).")
                logger.info(f"obs_id {obs_id} contains {len(obs_files)} files.")

                for index, file in enumerate(obs_files):
                    logger.info(f"Processing file {index + 1}/{len(obs_files)}")

                    files[file["location"]][file["bucket"]].add(file["key"])

        logger.info(f"Found files to delete in {len(files.keys())} locations.")

        return files, delete_requests

    def _process_file_structure(self, files: dict) -> None:
        """
        Iterates through the files dict (described above), and sends delete requests in batches of 1000

        Parameters
        ----------
        files: dict
            Described in docstring above
        """
        for index, location in enumerate(files.keys()):
            buckets = files[location]

            logger.info(f"Location {location} contains {len(buckets)} buckets with deleteable" " files.")
            logger.info(f"Processing location {index + 1}/{len(files.keys())}")

            for bucket in buckets:
                keys = files[location][bucket]
                keys_to_delete = []
                counter = 0

                logger.info(f"Bucket {bucket} contains {len(keys)} files to be deleted.")

                for key in keys:
                    keys_to_delete.append(key)
                    counter += 1

                    if counter == self.config.getint("delete_processor", "batch_size"):
                        # Delete in batches of 1000
                        self._batch_delete_objects(location, bucket, keys_to_delete)
                        keys_to_delete = []
                        counter = 0
                        # Now sleep for a bit to allow acacia to digest the deletes
                        if location == 2 or location == 4:
                            logger.info(f"Sleeping for {self.acacia_sleep_time} seconds")
                            time.sleep(self.acacia_sleep_time)

                # Delete any that are left!
                if keys_to_delete:
                    self._batch_delete_objects(location, bucket, keys_to_delete)

    def _review_delete_requests(self, delete_requests: dict) -> tuple[int, dict]:
        """
        Iterates through the delete_requests dict (described above).
        Check with obs_id had all of its files deleted, and marks the observation itself as deleted.
        If all files associated with a delete request were deleted, mark the delete request as actioned.

        Parameters
        ----------
        files: dict
            Described in docstring above

        Returns
        ----------
        num_actioned_delete_requests: int
            The number of delete requests that were actioned.
        failed_delete_requests: dict
            Dictionary containing the failed delete request ID as the key, and a
            list of observations that failed as the value.
        """
        num_actioned_delete_requests = 0
        failed_delete_requests = defaultdict(list)

        for delete_request_id in delete_requests:
            logger.info(f"Reviewing delete request {delete_request_id}.")

            delete_request_has_missing_files = False

            # Check if we are deleting all or only 1 type of file from this observation
            filetype_id: Optional[int] = self.repository.get_filetype_id_for_delete_request(delete_request_id)

            for obs_id in delete_requests[delete_request_id]:
                if self.terminate:
                    logger.warning("Exiting.")
                    sys.exit(0)

                obs_files = self.repository.get_not_deleted_obs_data_files_except_ppds(obs_id, filetype_id)

                if len(obs_files) > 0:
                    #
                    # If the type of delete was "entire observation", then
                    # if there are files remaining, this tells us the some deletes failed?
                    #
                    # If this was a "delete files of type", etc (i.e. a subset of the obs being
                    # deleted) then this is not an error.
                    #
                    logger.info(f"obs_id {obs_id} has {len(obs_files)} files that are not" " deleted.")

                    delete_request_has_missing_files = True
                    failed_delete_requests[delete_request_id].append(obs_id)
                else:
                    if not filetype_id:
                        logger.info(f"Updating obs_id {obs_id} to deleted.")
                        self.repository.set_obs_id_to_deleted(obs_id)
                    else:
                        logger.info(
                            f"Filetype_id {filetype_id} specified in delete request, so NOT "
                            f"updating obs_id {obs_id} to deleted."
                        )

            if not delete_request_has_missing_files:
                logger.info(f"Updating delete request {delete_request_id} to actioned.")

                self.repository.set_delete_request_to_actioned(delete_request_id)
                num_actioned_delete_requests += 1

        return num_actioned_delete_requests, failed_delete_requests

    def _print_summary(
        self,
        delete_requests: dict,
        num_actioned_delete_requests: int,
        failed_delete_requests: dict,
    ) -> None:
        """
        Logs a summary of what was processed.

        Parameters
        ----------
        delete_requests: int
            The delete requests structure described above
        num_actioned_delete_requests: int
            The number of delete requests that were actioned
        failed_delete_requests: int
            Dictionary of which delete request IDs failed (and their corresponding observations)
        """
        logger.info(f"Delete requests to process: {len(delete_requests.keys())}.")
        logger.info(f"Delete requests actioned:   {num_actioned_delete_requests}.")

        for failed_delete_request in failed_delete_requests.keys():
            logger.info(
                f"Delete request {failed_delete_request} was not actioned. The"
                " following observations were not deleted:"
                f" {failed_delete_requests[failed_delete_request]}."
            )

    def run(self, cli_ids: list[int] | None = None) -> None:
        """
        Main workflow. Does the processing bit of the processor.
        Will process CLI supplied delete_request IDs if they are given. If not, will fetch them from the database.
        """
        logger.info("Starting delete processor.")

        if cli_ids is not None:
            delete_request_ids = self._parse_ids(cli_ids)
            files, delete_requests = self._generate_data_structures(delete_request_ids)
        else:
            files, delete_requests = self._generate_data_structures()

        self._process_file_structure(files)

        (
            num_actioned_delete_requests,
            failed_delete_requests,
        ) = self._review_delete_requests(delete_requests)

        self._print_summary(delete_requests, num_actioned_delete_requests, failed_delete_requests)
