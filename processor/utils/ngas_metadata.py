from processor.utils.database import run_sql_get_one_row, run_sql_get_many_rows


def get_ngas_gpubox_paths(database_pool, obs_id: int) -> list:
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

    results = run_sql_get_many_rows(database_pool, sql, (f"{str(obs_id)}%gpubox%.fits", ))

    if results:
        return [r['path'] for r in results]
    else:
        return []


def get_ngas_all_file_paths(database_pool, obs_id: int) -> list:
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

    results = run_sql_get_many_rows(database_pool, sql, (f"{str(obs_id)}%", ))

    if results:
        return [r['path'] for r in results]
    else:
        return []


def get_ngas_file_path_and_address_for_filename(database_pool, file_id: str):
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


def get_ngas_file_path_and_address_for_filename_list(database_pool, file_id_list: list) -> list:
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

    results = run_sql_get_many_rows(database_pool, sql, (file_id_list,))

    if results:
        return [r['path'] for r in results]
    else:
        return []
