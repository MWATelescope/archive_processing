from enum import Enum
from db import run_sql_get_many_rows, run_sql_get_one_row, run_sql_update

class MWAFileTypeFlags(Enum):
    GPUBOX_FILE = 8
    FLAG_FILE = 10
    VOLTAGE_RAW_FILE = 11
    MWA_PPD_FILE = 14
    VOLTAGE_ICS_FILE = 15
    VOLTAGE_RECOMBINED_ARCHIVE_FILE = 16
    MWAX_VOLTAGES = 17
    MWAX_VISIBILITIES = 18


def get_delete_requests(database_pool) -> list:
    #TODO add AND WHERE actioned_datetime is NULL
    sql = """SELECT id
             FROM deletion_requests
             WHERE cancelled_datetime IS NULL
             ORDER BY created_datetime"""

    # Execute query
    params = None
    results = run_sql_get_many_rows(database_pool, sql, params)

    if results:
        return [r["id"] for r in results]
    else:
        return []


def get_obs_ids_for_delete_request(database_pool, delete_request_id: int) -> list:
    sql = """SELECT obs_id
             FROM deletion_request_observation
             WHERE request_id = %s
             ORDER BY obs_id"""

    # Execute query
    params = (delete_request_id,)
    results = run_sql_get_many_rows(database_pool, sql, params)

    if results:
        return [r["obs_id"] for r in results]
    else:
        return []


def get_obs_data_files_filenames_except_ppds(database_pool, obs_id: int) -> list:
    sql = """SELECT location, bucket, CONCAT_WS('', folder, filename) as key
              FROM data_files
              WHERE filetype NOT IN (%s)
              AND observation_num = %s
              AND deleted_timestamp IS NULL
              AND remote_archived = True
              ORDER BY filename"""

    results = run_sql_get_many_rows(
        database_pool,
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
        #return [r["filename"] for r in results]
    else:
        return []


def get_undeleted_files_from_obs_id(database_pool, obs_id: int) -> list:
    sql = """SELECT filename
             FROM data_files
             WHERE observation_num = %s
             AND deleted_timestamp IS NULL"""

    # Execute query
    params = (obs_id,)
    results = run_sql_get_many_rows(database_pool, sql, params)

    if results:
        return [r["filename"] for r in results]
    else:
        return []


def set_obs_id_to_deleted(database_pool, obs_id: int):
    sql = """UPDATE mwa_setting
             SET deleted_timestamp = NOW()
             WHERE starttime = %s"""

    # Execute query
    params = (obs_id,)
    run_sql_update(database_pool, sql, params)


def set_delete_request_to_actioned(database_pool, delete_request_id: int):
    sql = """UPDATE delete_requests
             SET actioned_datetime = NOW()
             WHERE id = %s"""

    # Execute query
    params = (delete_request_id,)
    run_sql_update(database_pool, sql, params)