from processor.utils.database import run_sql_get_many_rows, run_sql_get_one_row
from enum import Enum


class MWADataQualityFlags(Enum):
    GOOD = 1
    SOME_ISSUES = 2
    UNUSABLE = 3
    DELETED = 4
    MARKED_FOR_DELETE = 5
    PROCESSED = 6


class MWAFileTypeFlags(Enum):
    GPUBOX_FILE = 8
    FLAG_FILE = 10
    VOLTAGE_RAW_FILE = 11
    MWA_PPD_FILE = 14
    VOLTAGE_ICS_FILE = 15
    VOLTAGE_RECOMBINED_ARCHIVE_FILE = 16


class MWAModeFlags(Enum):
    HW_LFILES = 'HW_LFILES'
    VOLTAGE_START = 'VOLTAGE_START'
    VOLTAGE_BUFFER = 'VOLTAGE_BUFFER'


def update_mwa_setting_dataquality(database_pool, obsid: int, dataquality: MWADataQualityFlags):
    #
    # Updates the mwa_setting row in the metadata database to be dataquality = dataquality
    #
    cursor = None
    con = None

    try:
        con = database_pool.getconn()
        cursor = con.cursor()
        cursor.execute(("UPDATE mwa_setting SET dataquality = %s WHERE starttime = %s"), (str(dataquality.value), obsid))

    except Exception as e:
        if con:
            con.rollback()
            raise e
    else:
        rows_affected = cursor.rowcount

        if rows_affected == 1:
            if con:
                con.commit()
        else:
            if con:
                con.rollback()
            raise Exception(f"Update mwa_setting affected: {rows_affected} rows- "
                            "not 1 as expected. Rolling back")
    finally:
        if cursor:
            cursor.close()
        if con:
            database_pool.putconn(conn=con)


def set_mwa_data_files_deleted_flag(database_pool, filenames: list, obsid: int):
    #
    # Updates the data_file rows in the metadata database to be deleted = True
    #
    cursor = None
    con = None
    expected_updated = len(filenames)

    try:
        con = database_pool.getconn()
        cursor = con.cursor()
        cursor.execute(("UPDATE data_files SET deleted = True WHERE filename = any(%s) AND observation_num = %s"), (filenames, obsid))

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
            raise Exception(f"Update data_files affected: {rows_affected} rows- "
                            f"not {expected_updated} as expected. Rolling back")
    finally:
        if cursor:
            cursor.close()
        if con:
            database_pool.putconn(conn=con)


def get_obs_frequencies(database_pool, obs_id: int):
    sql = "select frequencies FROM rf_stream WHERE starttime = %s"

    result = run_sql_get_one_row(database_pool, sql, (int(obs_id), ))

    if result:
        return result["frequencies"]
    else:
        return None


def get_obs_data_files_filenames_except_ppds(database_pool, obs_id: int,
                                             deleted: bool = False, remote_archived: bool = True) -> list:
    sql = f"""SELECT filename 
              FROM data_files 
              WHERE filetype NOT IN (%s) 
              AND observation_num = %s
              AND deleted = %s
              AND remote_archived = %s
              ORDER BY filename"""

    results = run_sql_get_many_rows(database_pool, sql, (MWAFileTypeFlags.MWA_PPD_FILE.value,
                                                         int(obs_id),
                                                         deleted,
                                                         remote_archived, ))

    if results:
        return [r['filename'] for r in results]
    else:
        return []


def get_obs_gpubox_filenames(database_pool, obs_id: int, deleted: bool = False, remote_archived: bool = True) -> list:
    sql = f"""SELECT filename 
              FROM data_files 
              WHERE filetype = %s  
              AND observation_num = %s
              AND deleted = %s
              AND remote_archived = %s
              ORDER BY filename"""

    results = run_sql_get_many_rows(database_pool, sql, (MWAFileTypeFlags.GPUBOX_FILE.value,
                                                         int(obs_id),
                                                         deleted,
                                                         remote_archived, ))

    if results:
        return [r['filename'] for r in results]
    else:
        return []
