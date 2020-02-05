import psycopg2


def run_sql_get_one_row(database_pool, sql):
    conn = None
    cur = None

    try:
        conn = database_pool.getconn()
        cur = conn.cursor()

        cur.execute(sql)

        record = cur.fetchone()

    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if cur:
            cur.close()
        if conn:
            database_pool.putconn(conn)

    return record


def run_sql_get_many_rows(database_pool, sql):
    conn = None
    cur = None

    try:
        conn = database_pool.getconn()
        cur = conn.cursor()

        cur.execute(sql)

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
    sql = ("select frequencies from rf_stream where starttime = %s", int(obs_id))

    result = run_sql_get_one_row(database_pool, sql)

    if result:
        return result["frequencies"]
    else:
        return None


def get_obs_gpubox_filenames(database_pool, obs_id):
    sql = ("select filename from data_files where filetype = 8 and observation_num = %s order by filename", int(obs_id))

    results = run_sql_get_many_rows(database_pool, sql)

    if results:
        return [r['filename'] for r in results]
    else:
        return None


def get_obs_gpubox_ngas_paths(database_pool, obs_id):
    sql = ("select distinct on (file_id) "
           "mount_point || '/' || file_name as path, "
           "ngas_hosts.ip_address as address "
           "from ngas_files inner join ngas_disks "
           "on ngas_disks.disk_id = ngas_files.disk_id "
           "inner join ngas_hosts on ngas_disks.host_id = ngas_hosts.host_id "
           "where file_id like $1 and ngas_disks.disk_id in "
           "('35ecaa0a7c65795635087af61c3ce903', "
           "'54ab8af6c805f956c804ee1e4de92ca4', "
           "'921d259d7bc2a0ae7d9a532bccd049c7', "
           "'e3d87c5bc9fa1f17a84491d03b732afd') "
           "order by file_id, file_version desc", f"{obs_id}%gpubox%.fits")

    results = run_sql_get_many_rows(database_pool, sql)

    if results:
        return [r['path'] for r in results], [f"ngas@{r['address']}:{r['path']}" for r in results]
    else:
        return None


def get_obs_ngas_all_file_paths(database_pool, obs_id):
    sql = ("select distinct on (file_id) "
           "mount_point || '/' || file_name as path, "
           "ngas_hosts.ip_address as address "
           "from ngas_files inner join ngas_disks "
           "on ngas_disks.disk_id = ngas_files.disk_id "
           "inner join ngas_hosts on ngas_disks.host_id = ngas_hosts.host_id "
           "where file_id like $1 and ngas_disks.disk_id in "
           "('35ecaa0a7c65795635087af61c3ce903', "
           "'54ab8af6c805f956c804ee1e4de92ca4', "
           "'921d259d7bc2a0ae7d9a532bccd049c7', "
           "'e3d87c5bc9fa1f17a84491d03b732afd') "
           "order by file_id, file_version desc", f"{obs_id}%")

    results = run_sql_get_many_rows(database_pool, sql)

    if results:
        return [r['path'] for r in results], [f"ngas@{r['address']}:{r['path']}" for r in results]
    else:
        return None


def get_ngas_file_path_and_address_for_filename(database_pool, file_id):
    sql = ("select distinct on (file_id) "
           "mount_point || '/' || file_name as path, "
           "ngas_hosts.ip_address as address "
           "from ngas_files inner join ngas_disks "
           "on ngas_disks.disk_id = ngas_files.disk_id "
           "inner join ngas_hosts on ngas_disks.host_id = ngas_hosts.host_id "
           "where file_id = $1 and ngas_disks.disk_id in "
           "('35ecaa0a7c65795635087af61c3ce903', "
           "'54ab8af6c805f956c804ee1e4de92ca4', "
           "'921d259d7bc2a0ae7d9a532bccd049c7', "
           "'e3d87c5bc9fa1f17a84491d03b732afd') "
           "order by file_id, file_version desc", file_id)

    result = run_sql_get_one_row(database_pool, sql)

    if result:
        return result['path'], f"ngas@{result['address']}:{result['path']}"
    else:
        return None, None
