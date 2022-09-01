from archive_processing.utils.database import (
    run_sql_get_many_rows,
    run_sql_get_one_row,
)
from enum import Enum
import subprocess


class MWADataQualityFlags(Enum):
    GOOD = 1
    SOME_ISSUES = 2
    UNUSABLE = 3
    UNKNOWN = 4
    PROCESSED = 6


class MWAFileTypeFlags(Enum):
    GPUBOX_FILE = 8
    FLAG_FILE = 10
    VOLTAGE_RAW_FILE = 11
    MWA_PPD_FILE = 14
    VOLTAGE_ICS_FILE = 15
    VOLTAGE_RECOMBINED_ARCHIVE_FILE = 16
    MWAX_VOLTAGES = 17
    MWAX_VISIBILITIES = 18


class MWAModeFlags(Enum):
    HW_LFILES = "HW_LFILES"
    VOLTAGE_START = "VOLTAGE_START"
    VOLTAGE_BUFFER = "VOLTAGE_BUFFER"
    MWAX_VCS = "MWAX_VCS"
    MWAX_CORRELATOR = "MWAX_CORRELATOR"


def set_mwa_setting_deleted_timestamp(database_pool, obsid: int):
    #
    # Updates the mwa_setting row in the metadata database
    # to be deleted_timestamp = Now()
    #
    cursor = None
    con = None
    expected_updated = 1

    try:
        con = database_pool.getconn()
        cursor = con.cursor()
        cursor.execute(
            "UPDATE mwa_setting SET deleted_timestamp = Now() "
            "WHERE observation_num = %s",
            (obsid),
        )

    except Exception as e:
        if con:
            con.rollback()
            raise e
    else:
        rows_affected = cursor.rowcount

        if rows_affected == expected_updated:
            if con:
                con.commit()
        else:
            if con:
                con.rollback()
            raise Exception(
                f"Update mwa_setting affected: {rows_affected} rows- "
                f"not {expected_updated} as expected. Rolling back"
            )
    finally:
        if cursor:
            cursor.close()
        if con:
            database_pool.putconn(conn=con)


def set_mwa_data_files_deleted_timestamp(
    database_pool, filenames: list, obsid: int
):
    #
    # Updates the data_file rows in the metadata database
    # to be deleted_timestamp = Now()
    #
    cursor = None
    con = None
    expected_updated = len(filenames)

    try:
        con = database_pool.getconn()
        cursor = con.cursor()
        cursor.execute(
            "UPDATE data_files SET deleted_timestamp = Now() "
            "WHERE filename = any(%s) AND "
            "observation_num = %s",
            (filenames, obsid),
        )

    except Exception as e:
        if con:
            con.rollback()
            raise e
    else:
        rows_affected = cursor.rowcount

        if rows_affected == expected_updated:
            if con:
                con.commit()
        else:
            if con:
                con.rollback()
            raise Exception(
                f"Update data_files affected: {rows_affected} rows- "
                f"not {expected_updated} as expected. Rolling back"
            )
    finally:
        if cursor:
            cursor.close()
        if con:
            database_pool.putconn(conn=con)


def get_obs_frequencies(database_pool, obs_id: int):
    sql = "select frequencies FROM rf_stream WHERE starttime = %s"

    result = run_sql_get_one_row(database_pool, sql, (int(obs_id),))

    if result:
        return result["frequencies"]
    else:
        return None


def get_delete_requests(database_pool) -> list:
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


def get_delete_request_observations(
    database_pool, delete_request_id: int
) -> list:
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


# This takes in a list of obsids and puts it through a select to ensure
# the obsids are VOLTAGE observations and they are recombined and not deleted.
def get_archived_recombined_observations_with_list(
    database_pool, obs_id_list: list
) -> list:
    sql = """SELECT obs.starttime As obs_id
                      FROM mwa_setting As obs
                      WHERE
                       obs.processed = True
                       AND obs.deleted_timestamp IS NULL
                       AND obs.mode IN ('VOLTAGE_START', 'VOLTAGE_BUFFER')
                       AND obs.starttime = any(%s)
                      ORDER BY obs.starttime"""

    # Execute query
    params = (obs_id_list,)
    results = run_sql_get_many_rows(database_pool, sql, params)

    if results:
        return [r["obs_id"] for r in results]
    else:
        return []


def get_obs_data_files_filenames_except_ppds(
    database_pool,
    obs_id: int,
) -> list:
    sql = """SELECT filename
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
        return [r["filename"] for r in results]
    else:
        return []


def get_obs_gpubox_filenames(
    database_pool,
    obs_id: int,
) -> list:
    sql = """SELECT filename
              FROM data_files
              WHERE (filetype = %s OR filetype = %s)
              AND observation_num = %s
              AND deleted_timestamp IS NULL
              AND remote_archived = True
              ORDER BY filename"""

    results = run_sql_get_many_rows(
        database_pool,
        sql,
        (
            MWAFileTypeFlags.GPUBOX_FILE.value,
            MWAFileTypeFlags.MWAX_VISIBILITIES.value,
            int(obs_id),
        ),
    )

    if results:
        return [r["filename"] for r in results]
    else:
        return []


def get_vcs_raw_data_files_filenames(
    database_pool,
    obs_id: int,
) -> list:
    sql = """SELECT filename
              FROM data_files
              WHERE filetype  = %s
              AND observation_num = %s
              AND deleted_timestamp IS NULL
              AND remote_archived = True
              ORDER BY filename"""

    results = run_sql_get_many_rows(
        database_pool,
        sql,
        (
            MWAFileTypeFlags.VOLTAGE_RAW_FILE.value,
            int(obs_id),
        ),
    )

    if results:
        return [r["filename"] for r in results]
    else:
        return []


def get_vcs_tar_ics_data_files_filenames(
    database_pool,
    obs_id: int,
) -> list:
    sql = """SELECT filename
              FROM data_files
              WHERE (filetype = %s OR filetype = %s)
              AND observation_num = %s
              AND deleted_timestamp IS NULL
              AND remote_archived = True
              ORDER BY filename"""

    results = run_sql_get_many_rows(
        database_pool,
        sql,
        (
            MWAFileTypeFlags.VOLTAGE_RECOMBINED_ARCHIVE_FILE.value,
            MWAFileTypeFlags.VOLTAGE_ICS_FILE.value,
            int(obs_id),
        ),
    )

    if results:
        return [r["filename"] for r in results]
    else:
        return []


def invalidate_metadata_cache(obsid):
    wgetcmd = (
        f'wget "http://ws.mwatelescope.org/metadata/invalcache?obs_id={obsid}"'
        " -O /dev/null "
    )

    try:
        proc = subprocess.Popen(
            wgetcmd, stdout=subprocess.PIPE, shell=True, close_fds=True
        )

        out, err = proc.communicate()
        if proc.returncode != 0:
            raise Exception(
                f"Could not invalidate metadata cache {str(out)} {str(err)}"
            )

    except Exception as e:
        raise Exception(f"Could not invalidate metadata cache {e}")
