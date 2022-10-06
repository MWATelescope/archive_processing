from archive_processing.utils.mwa_metadata import (
    get_obs_data_files_filenames_except_ppds,
)
from archive_processing.utils.misc import divide_list_into_sublists


class Observation:
    def __init__(
        self,
        obs_id: int,
        logger,
        db_pool,
        delete_batch_size: int,
        execute: bool,
    ):
        self.obs_id = obs_id
        self.num_files_to_process = 0
        self.num_files_attempted = 0
        self.num_files_successful = 0
        self.num_files_failed = 0
        self.logger = logger
        self.db_pool = db_pool
        self.delete_batch_size = delete_batch_size
        self.execute = execute

    def log_status(self):
        self.logger.info(
            f"{self.obs_id}: Attempted: {self.num_files_attempted},"
            f" Successful: {self.num_files_successful}, Failed:"
            f" {self.num_files_failed}"
        )

    def delete(self) -> bool:
        file_list = get_obs_data_files_filenames_except_ppds(
            self.db_pool, self.obs_id
        )

        self.num_files_to_process = len(file_list)

        if self.num_files_to_process > 0:
            self.logger.info(
                f"{self.obs_id}:"
                f" Processing...({self.num_files_to_process} files)"
            )

            # Break the file list up into sub lists
            chunks_list = list(
                divide_list_into_sublists(file_list, self.delete_batch_size)
            )
            chunks = len(chunks_list)

            # loop through each "batch"/chunk
            self.logger.info(
                f"{self.obs_id}: {chunks} batches of up to"
                f" {self.delete_batch_size} files per batch."
            )

            for chunk_number, chunk_file_list in enumerate(chunks_list):
                self.num_files_attempted += len(chunk_file_list)

                if self.execute:
                    self.logger.info(
                        f"{self.obs_id}-Batch({chunk_number+1}/{chunks}):"
                        " Deleting"
                        " file_batch:"
                        f" {chunk_file_list[0]}..{chunk_file_list[-1]} "
                        f"({len(chunk_file_list)} files)",
                    )
                    self.num_files_successful += len(chunk_file_list)
                else:
                    self.logger.info(
                        f"{self.obs_id}-Batch({chunk_number+1}/{chunks}):"
                        " WOULD"
                        " HAVE"
                        " DELETED"
                        " file_batch:"
                        f" {chunk_file_list[0]}..{chunk_file_list[-1]} "
                        f"({len(chunk_file_list)} files)",
                    )
                    self.num_files_successful += len(chunk_file_list)
        else:
            self.logger.info(f"{self.obs_id}: No files to delete.")

        if self.execute:
            self.logger.info(
                f"{self.obs_id}: Updated observation with"
                " deleted_timestamp=now()",
            )
        else:
            self.logger.info(
                f"{self.obs_id}: WOULD HAVE updated observation"
                "with deleted_timestamp=now()",
            )

        return True
