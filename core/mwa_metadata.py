import psycopg2
from psycopg2.extras import RealDictCursor
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
    VOLTAGE_ICS_FILE = 15
    VOLTAGE_RECOMBINED_ARCHIVE_FILE = 16


class MWAModeFlags(Enum):
    HW_LFILES = 'HW_LFILES'
    VOLTAGE_START = 'VOLTAGE_START'
    VOLTAGE_BUFFER = 'VOLTAGE_BUFFER'


def run_sql_get_one_row(database_pool, sql, args):
    conn = None
    cur = None

    try:
        conn = database_pool.getconn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if args is None:
            cur.execute(sql)
        else:
            cur.execute(sql, args)

        record = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if cur:
            cur.close()
        if conn:
            database_pool.putconn(conn)

    return record


def run_sql_get_many_rows(database_pool, sql, args) -> list:
    conn = None
    cur = None

    try:
        conn = database_pool.getconn()
        cur = conn.cursor(cursor_factory=RealDictCursor)

        if args is None:
            cur.execute(sql)
        else:
            cur.execute(sql, args)

        records = cur.fetchall()

    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if cur:
            cur.close()
        if conn:
            database_pool.putconn(conn)

    return records


def get_obs_frequencies(database_pool, obs_id):
    sql = "select frequencies FROM rf_stream WHERE starttime = %s"

    result = run_sql_get_one_row(database_pool, sql, (int(obs_id), ))

    if result:
        return result["frequencies"]
    else:
        return None


def get_obs_gpubox_filenames(database_pool, obs_id, include_deleted=False, include_remote_archived=True) -> list:
    sql = f"""SELECT filename 
              FROM data_files 
              WHERE filetype = {MWAFileTypeFlags.GPUBOX_FILE.value} 
              AND observation_num = %s
              AND deleted = %s
              AND remote_archived = %s
              ORDER BY filename"""

    results = run_sql_get_many_rows(database_pool, sql, (int(obs_id), include_deleted, include_remote_archived, ))

    if results:
        return [r['filename'] for r in results]
    else:
        return []


def get_obs_gpubox_ngas_paths(database_pool, obs_id) -> list:
    sql = """SELECT distinct on (file_id) 
                    mount_point || '/' || file_name as path,  
                    ngas_hosts.ip_address as address 
             FROM ngas_files inner join ngas_disks 
                  ON ngas_disks.disk_id = ngas_files.disk_id 
             INNER JOIN ngas_hosts on ngas_disks.host_id = ngas_hosts.host_id 
             WHERE file_id similar to %s 
               AND ngas_disks.disk_id in 
                   ('35ecaa0a7c65795635087af61c3ce903', 
                    '54ab8af6c805f956c804ee1e4de92ca4', 
                    '921d259d7bc2a0ae7d9a532bccd049c7', 
                    'e3d87c5bc9fa1f17a84491d03b732afd') 
             ORDER BY file_id, file_version desc"""

    results = run_sql_get_many_rows(database_pool, sql, (f"{obs_id}%gpubox%.fits", ))

    if results:
        return [r['path'] for r in results]
    else:
        return []


def get_obs_ngas_all_file_paths(database_pool, obs_id) -> list:
    sql = """SELECT distinct on (file_id) 
                    mount_point || '/' || file_name as path, 
                    ngas_hosts.ip_address as address 
             FROM ngas_files inner join ngas_disks 
                  on ngas_disks.disk_id = ngas_files.disk_id 
             INNER JOIN ngas_hosts on ngas_disks.host_id = ngas_hosts.host_id 
             WHERE file_id like %s 
               AND ngas_disks.disk_id in 
                   ('35ecaa0a7c65795635087af61c3ce903', 
                    '54ab8af6c805f956c804ee1e4de92ca4', 
                    '921d259d7bc2a0ae7d9a532bccd049c7', 
                    'e3d87c5bc9fa1f17a84491d03b732afd') 
             ORDER BY file_id, file_version desc"""

    results = run_sql_get_many_rows(database_pool, sql, (f"{obs_id}%", ))

    if results:
        return [r['path'] for r in results]
    else:
        return []


def get_ngas_file_path_and_address_for_filename(database_pool, file_id):
    sql =  """SELECT distinct on (file_id) 
                     mount_point || '/' || file_name as path, 
                     ngas_hosts.ip_address as address 
              FROM ngas_files inner join ngas_disks 
                   on ngas_disks.disk_id = ngas_files.disk_id 
              inner join ngas_hosts on ngas_disks.host_id = ngas_hosts.host_id 
              WHERE file_id = %s 
                and ngas_disks.disk_id in 
                    ('35ecaa0a7c65795635087af61c3ce903', 
                     '54ab8af6c805f956c804ee1e4de92ca4', 
                     '921d259d7bc2a0ae7d9a532bccd049c7', 
                     'e3d87c5bc9fa1f17a84491d03b732afd') 
              order by file_id, file_version desc"""

    result = run_sql_get_one_row(database_pool, sql, (file_id, ))

    if result:
        return result['path']
    else:
        return None


def get_ngas_file_path_and_address_for_filename_list(database_pool, file_id_list) -> list:
    sql = """SELECT distinct on (file_id) 
                     mount_point || '/' || file_name as path, 
                     ngas_hosts.ip_address as address 
              FROM ngas_files inner join ngas_disks 
                   on ngas_disks.disk_id = ngas_files.disk_id 
              inner join ngas_hosts on ngas_disks.host_id = ngas_hosts.host_id 
              WHERE file_id = any(%s) 
                and ngas_disks.disk_id in 
                    ('35ecaa0a7c65795635087af61c3ce903', 
                     '54ab8af6c805f956c804ee1e4de92ca4', 
                     '921d259d7bc2a0ae7d9a532bccd049c7', 
                     'e3d87c5bc9fa1f17a84491d03b732afd') 
              order by file_id, file_version desc"""

    results = run_sql_get_many_rows(database_pool, sql, (file_id_list, ))

    if results:
        return [r['path'] for r in results]
    else:
        return []
