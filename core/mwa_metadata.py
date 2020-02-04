

async def get_mwa_project_for_obs(database_pool, obs_id):
    async with database_pool.acquire() as conn:
        result = await conn.fetchrow("select projectid from mwa_setting where starttime = $1",
                                     int(obs_id))
        if result:
            return result['projectid']
        return None


async def get_mwa_projects(database_pool):
    async with database_pool.acquire() as conn:
        result = await conn.fetch("select projectid, shortname, description from mwa_project "
                                  "order by projectid asc")
        return result


async def get_mwa_obs_freq(database_pool, obs_id):
    async with database_pool.acquire() as conn:
        result = await conn.fetchrow(("select frequencies from rf_stream "
                                      "where starttime = $1"), int(obs_id))

        if result:
            return result['frequencies']
        return None


async def get_mwa_obs_flag_file(database_pool, obs_id):
    async with database_pool.acquire() as conn:
        result = await conn.fetchrow(("select filename from data_files where filetype = 10 "
                                      "and observation_num = $1"), int(obs_id))
        if result:
            return result['filename']
        return None


async def get_mwa_obs_files(database_pool, obs_id):
    async with database_pool.acquire() as conn:
        result = await conn.fetch(("select filename from data_files where filetype = 8 "
                                   "and observation_num = $1"), int(obs_id))
        return [r['filename'] for r in result]


async def get_mwa_all_vis_files(database_pool, obs_id):
    async with database_pool.acquire() as conn:
        result = await conn.fetch(("select filename from data_files where filetype in (8, 10, 14) "
                                   "and observation_num = $1"), int(obs_id))
        return [r['filename'] for r in result]


async def get_mwa_vis_meta_files(database_pool, obs_id):
    async with database_pool.acquire() as conn:
        result = await conn.fetch(("select filename from data_files where filetype in (10, 14) "
                                   "and observation_num = $1"), int(obs_id))
        return [r['filename'] for r in result]


async def get_ngas_obs_file_paths(database_pool, obs_id):
    async with database_pool.acquire() as conn:
        result = await conn.fetch(("select distinct on (file_id) "
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
                                   "order by file_id, file_version desc"), f"{obs_id}%gpubox%.fits")
        return [r['path'] for r in result], [f"ngas@{r['address']}:{r['path']}" for r in result]


async def get_ngas_all_vis_file_paths(database_pool, obs_id):
    async with database_pool.acquire() as conn:
        result = await conn.fetch(("select distinct on (file_id) "
                                   "mount_point || '/' || file_name as path, "
                                   "ngas_hosts.ip_address as address "
                                   "from ngas_files inner join ngas_disks "
                                   "on ngas_disks.disk_id = ngas_files.disk_id "
                                   "inner join ngas_hosts on ngas_disks.host_id = ngas_hosts.host_id "
                                   "where file_id similar to $1 and ngas_disks.disk_id in "
                                   "('35ecaa0a7c65795635087af61c3ce903', "
                                   "'54ab8af6c805f956c804ee1e4de92ca4', "
                                   "'921d259d7bc2a0ae7d9a532bccd049c7', "
                                   "'e3d87c5bc9fa1f17a84491d03b732afd') "
                                   "order by file_id, file_version desc"), f"{obs_id}%(.zip|.fits|.gz)")
        return [r['path'] for r in result], [f"ngas@{r['address']}:{r['path']}" for r in result]


async def get_ngas_file_path(database_pool, file_id):
    async with database_pool.acquire() as conn:
        result = await conn.fetchrow(("select distinct on (file_id) "
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
                                      "order by file_id, file_version desc"), file_id)
        if result:
            return result['path'], f"ngas@{result['address']}:{result['path']}"
        return None, None


async def get_ngas_file_path_in(database_pool, file_ids):
    async with database_pool.acquire() as conn:
        result = await conn.fetch(("select distinct on (file_id) "
                                   "mount_point || '/' || file_name as path, "
                                   "ngas_hosts.ip_address as address "
                                   "from ngas_files inner join ngas_disks "
                                   "on ngas_disks.disk_id = ngas_files.disk_id "
                                   "inner join ngas_hosts on ngas_disks.host_id = ngas_hosts.host_id "
                                   "where file_id = any($1) and ngas_disks.disk_id in "
                                   "('35ecaa0a7c65795635087af61c3ce903', "
                                   "'54ab8af6c805f956c804ee1e4de92ca4', "
                                   "'921d259d7bc2a0ae7d9a532bccd049c7', "
                                   "'e3d87c5bc9fa1f17a84491d03b732afd') "
                                   "order by file_id, file_version desc"), file_ids)

        return [r['path'] for r in result], [f"ngas@{r['address']}:{r['path']}" for r in result]
