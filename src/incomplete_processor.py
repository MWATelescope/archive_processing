import datetime
import logging
import os
from configparser import ConfigParser
from cmd_utils import run_command_ext
from processor import Processor
from incomplete_repository import IncompleteRepository
from mwa_utils import get_gpstime

logger = logging.getLogger()


class IncompleteFile:
    def __init__(self, stdout_line: str):
        # for Acicia, each line should look like this
        # [2023-10-13 15:54:01 AEDT]     0B mwaingest-13791/1379109160_20230918215222_ch156_000.fits

        # Split the line by spaces- we only care about the last contiguous field
        split_line = stdout_line.split(" ")

        # Key is the main identifier
        self.key: str = split_line[-1]

        # Filename (this is the filename with no folder or path info)- it is the value after the last "/"
        self.filename: str = self.key.split("/")[-1]

        # obs_id is the first 10 characters of the filename
        self.obs_id: int = int(self.filename[0:10])

        # bucket (the thing before the first "/")
        self.bucket: str = self.key.split("/")[0]

        # This is the location of the file once we've downloaded it
        self.temp_filename = None

        # Checksum from database
        self.checksum_db = None

        # Checksum from file
        self.checksum_file = None

        # Flag to set if we are complete
        self.completed = False

    def __repr__(self):
        return f"key={self.key}; bucket={self.bucket}, filename={self.filename}"


class IncompleteProcessor(Processor):
    def __init__(
        self,
        repository: IncompleteRepository,
        dry_run: bool = False,
        config: ConfigParser = None,
    ):
        super().__init__(dry_run)
        self.config = config
        self.repository = repository

    def _get_incomplete_uploads(self) -> list[IncompleteFile]:
        """
        Runs `mc ls --incomplete {alias}` to get a list of filenames which have incomplete uploads

        {alias} refers to the mc alias (run `mc alias ls`) - i.e. Acacia or Banksia to check against.
        """
        cmd = f"{self.minio_client_path} ls --incomplete {self.minio_client_alias}"

        (success, stdout) = run_command_ext(
            logger=logger, command=cmd, numa_node=-1, timeout=180, use_shell=True
        )

        if success:
            # If success but stdout is empty it means nothing is incomplete
            if stdout.lstrip().rstrip() == "":
                return []
            else:
                return_list = []

                # Split std by cr/lf and for each line
                # parse it into an IncompleteFile structure
                # and return a list of them
                stdout_lines = stdout.splitlines(False)

                for line in stdout_lines:
                    new_file = IncompleteFile(line)

                    # Do not remove incompletes if they are recent
                    now_gpstime: int = get_gpstime(datetime.datetime.now())

                    # Leave anything newer than 7 days alone!
                    if (now_gpstime - new_file.obs_id) > (7 * 24 * 60 * 60):
                        return_list.append(new_file)
                        logger.debug(f"Found {new_file.key}")
                    else:
                        logger.warning(f"Ignoring {new_file.key} as it is too new!")

                return return_list

        else:
            logger.error(f"Error executing mc- {stdout}")
            exit(-1)

    def _get_checksum_from_database(self, incomplete_file: IncompleteFile):
        incomplete_file.checksum_db = self.repository.get_data_file_checksum(
            incomplete_file.filename
        )

        logger.debug(
            f"Database md5 checksum of {incomplete_file.filename} is"
            f" {incomplete_file.checksum_db}"
        )

    def _download_file(self, incomplete_file: IncompleteFile):
        # Append the incomplete filename to the temp_data_path
        incomplete_file.temp_filename = os.path.join(
            self.temp_data_path, incomplete_file.filename
        )

        # Assemble the download command
        copy_cmd = (
            f"{self.minio_client_path} cp"
            f" {self.minio_client_alias}/{incomplete_file.key} {incomplete_file.temp_filename}"
        )

        if self.dry_run:
            logger.info(f"Would have run: {copy_cmd}")
        else:
            (success, stdout) = run_command_ext(
                logger=logger,
                command=copy_cmd,
                numa_node=-1,
                timeout=600,
                use_shell=True,
            )

            if not success:
                raise Exception(
                    f"Error downloading file from {incomplete_file.key}: {stdout}"
                )

    def _get_checksum_from_file(self, incomplete_file: IncompleteFile):
        # Assemble the checksum command
        md5_cmd = f"md5sum {incomplete_file.temp_filename}"

        if self.dry_run:
            logger.info(f"Would have run: {md5_cmd}")
        else:
            (success, stdout) = run_command_ext(
                logger=logger,
                command=md5_cmd,
                numa_node=-1,
                timeout=180,
                use_shell=True,
            )

            if success:
                # If success but stdout is empty it means something went wrong
                if stdout.lstrip().rstrip() == "":
                    raise Exception(
                        f"Error getting md5 checksum from {incomplete_file.key}: no"
                        " checksum returned"
                    )
                else:
                    # the return value will contain a few spaces and then the filename
                    # So remove the filename and then remove any whitespace
                    checksum = stdout.replace(
                        incomplete_file.temp_filename, ""
                    ).rstrip()

                    # Checksum should be 32 chars
                    if len(checksum) == 32:
                        incomplete_file.checksum_file = checksum
                    else:
                        raise Exception(
                            f"Error getting md5 checksum from {incomplete_file.key}:"
                            " checksum returned is <> 32 characters: stdout:"
                            f" {stdout} checksum: {checksum}"
                        )
            else:
                raise Exception(
                    f"Error getting md5 checksum from {incomplete_file.key}: {stdout}"
                )

    def _rm_incomplete_upload(self, incomplete_file: IncompleteFile):
        # Assemble the rm incomplete command
        cmd = (
            f"{self.minio_client_path} rm --incomplete"
            f" {self.minio_client_alias}/{incomplete_file.key}"
        )

        if self.dry_run:
            logger.info(f"Would have run: {cmd}")
        else:
            (success, stdout) = run_command_ext(
                logger=logger,
                command=cmd,
                numa_node=-1,
                timeout=180,
                use_shell=True,
            )

            if success:
                logger.info(
                    f"SUCCESS - incomplete upload on {incomplete_file.key} removed."
                )
            else:
                raise Exception(
                    "Error: could not remove incomplete upload on"
                    f" {incomplete_file.key}"
                )

    def run(self, location: str) -> None:
        """
        Main workflow. Does the processing bit of the processor.
        Will process CLI supplied location (acacia or banksia) to look for incomplete uploads.
        For each incomplete upload, download the file, confirm the checksum matches and add to a report.
        At the end of the report display if the file(s) are OK or do not match checksum.
        """
        logger.info("Starting incomplete processor.")

        self.location = location
        # Get the minio path
        self.minio_client_path = self.config.get(
            "incomplete_processor", "minio_client_path"
        )
        # Get the minio aliases
        self.minio_client_alias = self.config.get(self.location, "minio_client_alias")

        # Set location to temporarily download file to
        self.temp_data_path = self.config.get("incomplete_processor", "temp_data_path")
        if not os.path.exists(self.temp_data_path):
            logger.error(
                f"temp_data_path {self.temp_data_path} does not exist. Exiting"
            )
            exit(-1)

        logger.info(f"Checking {self.location} for incomplete uploads...")
        incomplete_files = self._get_incomplete_uploads()
        logger.info(f"{len(incomplete_files)} incomplete files found")

        #
        # For each file:
        # * retrieve the checksum from database
        # * download the file
        # * perform a checksum on the file
        # * compare
        # * if it's a match then we can safely remove the incomplete file
        #
        for incomplete_file in incomplete_files:
            self._get_checksum_from_database(incomplete_file)

            self._download_file(incomplete_file)

            self._get_checksum_from_file(incomplete_file)

            # Print the result
            if incomplete_file.checksum_db != incomplete_file.checksum_file:
                logger.error(
                    f"Checksum (DB) {incomplete_file.checksum_db} does not match"
                    f" Checksum (file) {incomplete_file.checksum_file}"
                )
            else:
                logger.info(
                    f"Checksums match ({incomplete_file.checksum_db} vs"
                    f" {incomplete_file.checksum_file})"
                )

                # Good! Now remove the incomplete / partial upload
                self._rm_incomplete_upload(incomplete_file)
                incomplete_file.completed = True

        logger.info("Finished!")

        success: int = 0

        for incomplete_file in incomplete_files:
            if incomplete_file.completed:
                success += 1

        logger.info(f"Incomplete files processed: {len(incomplete_files)}")
        logger.info(f"Incomplete files successfully removed: {success}")
        if len(incomplete_files) != success:
            logger.info(f"Incomplete files FAILED: {len(incomplete_files)-success}")
