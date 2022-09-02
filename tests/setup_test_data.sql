-- Test data for the DeleteRequestProcessor

--
-- Insert our test project
--
INSERT INTO public.mwa_project VALUES ('TEST001', 'Test project', 'Test project') 
ON CONFLICT (projectid) DO NOTHING;

--
-- Insert data_file_types
--
INSERT INTO public.data_file_types VALUES (8, 'Raw Correlator Products') ON CONFLICT DO NOTHING;
INSERT INTO public.data_file_types VALUES (10, 'MWA Flag File') ON CONFLICT DO NOTHING;
INSERT INTO public.data_file_types VALUES (11, 'Raw Voltage') ON CONFLICT DO NOTHING;
INSERT INTO public.data_file_types VALUES (14, 'MWA PPD File') ON CONFLICT DO NOTHING;
INSERT INTO public.data_file_types VALUES (15, 'Voltage ICS') ON CONFLICT DO NOTHING;
INSERT INTO public.data_file_types VALUES (16, 'Voltage Recombined Archive') ON CONFLICT DO NOTHING;
INSERT INTO public.data_file_types VALUES (17, 'MWAX voltages') ON CONFLICT DO NOTHING;
INSERT INTO public.data_file_types VALUES (18, 'MWAX visibilities') ON CONFLICT DO NOTHING;

--
-- Insert some observations. Start them in May 23, 2043!
--
INSERT INTO public.mwa_setting (starttime, stoptime, obsname, creator, mode, projectid, groupid, deleted_timestamp) 
VALUES (2000000000, 2000000064, 'drp_test1/8', 'gsleap', 'MWAX_CORRELATOR', 'TEST001', 2000000000, NULL)
ON CONFLICT (starttime) DO UPDATE SET deleted_timestamp = NULL;
   
INSERT INTO public.mwa_setting (starttime, stoptime, obsname, creator, mode, projectid, groupid, deleted_timestamp) 
VALUES (2000000064, 2000000128, 'drp_test2/8', 'gsleap', 'MWAX_CORRELATOR', 'TEST001', 2000000000, NULL)
ON CONFLICT (starttime) DO UPDATE SET deleted_timestamp = NULL;
   
INSERT INTO public.mwa_setting (starttime, stoptime, obsname, creator, mode, projectid, groupid, deleted_timestamp) 
VALUES (2000000128, 2000000192, 'drp_test3/8', 'gsleap', 'MWAX_CORRELATOR', 'TEST001', 2000000000, NULL)
ON CONFLICT (starttime) DO UPDATE SET deleted_timestamp = NULL;
   
INSERT INTO public.mwa_setting (starttime, stoptime, obsname, creator, mode, projectid, groupid, deleted_timestamp) 
VALUES (2000000192, 2000000256, 'drp_test4/8', 'gsleap', 'MWAX_CORRELATOR', 'TEST001', 2000000000, NULL)
ON CONFLICT (starttime) DO UPDATE SET deleted_timestamp = NULL;
   
INSERT INTO public.mwa_setting (starttime, stoptime, obsname, creator, mode, projectid, groupid, deleted_timestamp) 
VALUES (2000000256, 2000000320, 'drp_test5/8', 'gsleap', 'MWAX_CORRELATOR', 'TEST001', 2000000000, NULL)
ON CONFLICT (starttime) DO UPDATE SET deleted_timestamp = NULL;
   
INSERT INTO public.mwa_setting (starttime, stoptime, obsname, creator, mode, projectid, groupid, deleted_timestamp) 
VALUES (2000000320, 2000000384, 'drp_test6/8', 'gsleap', 'MWAX_CORRELATOR', 'TEST001', 2000000000, NULL)
ON CONFLICT (starttime) DO UPDATE SET deleted_timestamp = NULL;
   
INSERT INTO public.mwa_setting (starttime, stoptime, obsname, creator, mode, projectid, groupid, deleted_timestamp) 
VALUES (2000000384, 2000000448, 'drp_test7/8', 'gsleap', 'MWAX_CORRELATOR', 'TEST001', 2000000000, NULL)
ON CONFLICT (starttime) DO UPDATE SET deleted_timestamp = NULL;
   
INSERT INTO public.mwa_setting (starttime, stoptime, obsname, creator, mode, projectid, groupid, deleted_timestamp) 
VALUES (2000000448, 2000000512, 'drp_test8/8', 'gsleap', 'MWAX_CORRELATOR', 'TEST001', 2000000000, NULL)
ON CONFLICT (starttime) DO UPDATE SET deleted_timestamp = NULL;

--
-- Insert some delete_requests
--
INSERT INTO public.deletion_requests(cancelled_datetime, request_name, data_volume_bytes) VALUES (NULL, 'existing_delete_request_1', 4920000) ON CONFLICT DO NOTHING;
INSERT INTO public.deletion_request_observation VALUES (lastval(), 2000000000) ON CONFLICT DO NOTHING;

INSERT INTO public.deletion_requests(cancelled_datetime, request_name, data_volume_bytes) VALUES ('2022-09-02 12:00:00', 'cancelled_delete_request_2', 4920000) ON CONFLICT DO NOTHING;
INSERT INTO public.deletion_request_observation VALUES (lastval(), 2000000064) ON CONFLICT DO NOTHING;

--
--  Insert some data files: 2000000000
--
INSERT INTO public.data_files(observation_num, filetype, size, filename, remote_archived, deleted, location, deleted_timestamp, checksum_type, checksum, folder, bucket)
VALUES (2000000000, 14, 1230, '2000000000_metafits_ppds.fits', True, False, 3, NULL, 1, 'abcdefgh','/test/2043-05-23/1/','test_bucket01')
ON CONFLICT (filename) DO UPDATE SET deleted_timestamp = NULL, folder=excluded.folder;

INSERT INTO public.data_files(observation_num, filetype, size, filename, remote_archived, deleted, location, deleted_timestamp, checksum_type, checksum, folder, bucket)
VALUES (2000000000, 8, 1230000, '2000000000_20430523133404_gpubox01_00.fits', True, False, 3, NULL, 1, 'abcdefgh','/test/2043-05-23/1/','test_bucket01')
ON CONFLICT (filename) DO UPDATE SET deleted_timestamp = NULL, folder=excluded.folder;

INSERT INTO public.data_files(observation_num, filetype, size, filename, remote_archived, deleted, location, deleted_timestamp, checksum_type, checksum, folder, bucket)
VALUES (2000000000, 8, 1230000, '2000000000_20430523133404_gpubox02_00.fits', True, False, 3, NULL, 1, 'abcdefgh','/test/2043-05-23/1/','test_bucket01')
ON CONFLICT (filename) DO UPDATE SET deleted_timestamp = NULL, folder=excluded.folder;

INSERT INTO public.data_files(observation_num, filetype, size, filename, remote_archived, deleted, location, deleted_timestamp, checksum_type, checksum, folder, bucket)
VALUES (2000000000, 8, 1230000, '2000000000_20430523133404_gpubox03_00.fits', True, False, 3, NULL, 1, 'abcdefgh','/test/2043-05-23/1/','test_bucket01')
ON CONFLICT (filename) DO UPDATE SET deleted_timestamp = NULL, folder=excluded.folder;

INSERT INTO public.data_files(observation_num, filetype, size, filename, remote_archived, deleted, location, deleted_timestamp, checksum_type, checksum, folder, bucket)
VALUES (2000000000, 8, 1230000, '2000000000_20430523133404_gpubox04_00.fits', True, False, 3, NULL, 1, 'abcdefgh','/test/2043-05-23/1/','test_bucket01')
ON CONFLICT (filename) DO UPDATE SET deleted_timestamp = NULL, folder=excluded.folder;

--
--  Insert some data files: 2000000064
--
INSERT INTO public.data_files(observation_num, filetype, size, filename, remote_archived, deleted, location, deleted_timestamp, checksum_type, checksum, folder, bucket)
VALUES (2000000064, 14, 1230, '2000000064_metafits_ppds.fits', True, False, 3, NULL, 1, 'abcdefgh','/test/2043-05-24/1/','test_bucket01')
ON CONFLICT (filename) DO UPDATE SET deleted_timestamp = NULL, folder=excluded.folder;

INSERT INTO public.data_files(observation_num, filetype, size, filename, remote_archived, deleted, location, deleted_timestamp, checksum_type, checksum, folder, bucket)
VALUES (2000000064, 8, 1230000, '2000000064_20430523133404_gpubox01_00.fits', True, False, 3, NULL, 1, 'abcdefgh','/test/2043-05-24/1/','test_bucket01')
ON CONFLICT (filename) DO UPDATE SET deleted_timestamp = NULL, folder=excluded.folder;


