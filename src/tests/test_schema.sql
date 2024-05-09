CREATE TABLE deletion_requests (
  id SERIAL PRIMARY KEY,
  created_datetime time DEFAULT NOW(),
  cancelled_datetime time DEFAULT NULL,
  actioned_datetime time DEFAULT NULL,
  approved_datetime time DEFAULT NOW(),
  request_name varchar(20),
  filetype_id INT NULL
);

CREATE TABLE mwa_setting (
  starttime BIGINT PRIMARY KEY,
  deleted BOOLEAN DEFAULT NULL,
  deleted_timestamp time DEFAULT NULL
);

CREATE TABLE deletion_request_observation (
  request_id INT,
  obs_id BIGINT,
  CONSTRAINT fk_dr
    FOREIGN KEY(request_id)
      REFERENCES deletion_requests(id) 
        ON DELETE SET NULL,
  CONSTRAINT fk_dro_mwa_setting
    FOREIGN KEY(obs_id)
      REFERENCES mwa_setting(starttime)
        ON DELETE SET NULL
);

CREATE TABLE data_files (  
  observation_num BIGINT,
  filename VARCHAR(256),  
  location INT,
  bucket VARCHAR(20),
  folder VARCHAR(256),  
  filetype INT,
  deleted BOOLEAN DEFAULT FALSE,
  deleted_timestamp time DEFAULT NULL,
  remote_archived BOOLEAN DEFAULT TRUE,
  CONSTRAINT fk_df_mwa_setting
    FOREIGN KEY(observation_num)
      REFERENCES mwa_setting(starttime)
        ON DELETE SET NULL,
  PRIMARY KEY (observation_num, filename)
);

INSERT INTO deletion_requests (request_name, filetype_id) 
VALUES 
  ('dr1_all', NULL), 
  ('dr2_all', NULL),
  ('dr3_all', NULL),
  ('dr4_subfiles', 17);
  
INSERT INTO mwa_setting (starttime)
VALUES
  (9000000011),
  (9000000012),
  (9000000013),
  (9000000021),
  (9000000022),
  (9000000023),
  (9000000031),
  (9000000032),
  (9000000033),
  (9000000041),
  (9000000042);

INSERT INTO data_files(observation_num, filetype, location, bucket, folder, filename)
VALUES
  (9000000011, 8, 2, 'mwa01fs', '/bucket1/', '9000000011_1.fits'),
  (9000000011, 8, 2, 'mwa02fs', '/bucket2/', '9000000011_2.fits'),
  (9000000011, 8, 2, 'mwa03fs', '/bucket3/', '9000000011_3.fits'),
  (9000000011, 8, 2, 'mwa04fs', '/bucket4/', '9000000011_4.fits'),
  (9000000011, 8, 2, 'mwa01fs', '/bucket1/', '9000000011_5.fits'),
  (9000000011, 8, 2, 'mwa01fs', '/bucket1/', '9000000011_6.fits'),
  (9000000012, 8, 2, 'mwa02fs', '/bucket2/', '9000000012_1.fits'),
  (9000000012, 8, 2, 'mwa03fs', '/bucket3/', '9000000012_2.fits'),
  (9000000012, 8, 2, 'mwa04fs', '/bucket4/', '9000000012_3.fits'),
  (9000000012, 8, 2, 'mwa01fs', '/bucket1/', '9000000012_4.fits'),
  (9000000012, 8, 2, 'mwa02fs', '/bucket2/', '9000000012_5.fits'),
  (9000000012, 8, 2, 'mwa03fs', '/bucket3/', '9000000012_6.fits'),
  (9000000013, 8, 2, 'mwa04fs', '/bucket4/', '9000000013_1.fits'),
  (9000000013, 8, 2, 'mwa01fs', '/bucket1/', '9000000013_2.fits'),
  (9000000013, 8, 2, 'mwa02fs', '/bucket2/', '9000000013_3.fits'),
  (9000000013, 8, 2, 'mwa03fs', '/bucket3/', '9000000013_4.fits'),
  (9000000013, 8, 2, 'mwa03fs', '/bucket3/', '9000000013_5.fits'),
  (9000000013, 8, 2, 'mwa03fs', '/bucket3/', '9000000013_6.fits'),
  (9000000021, 8, 2, 'mwa04fs', '/bucket4/', '9000000021_1.fits'),
  (9000000021, 8, 2, 'mwa01fs', '/bucket1/', '9000000021_2.fits'),
  (9000000021, 8, 2, 'mwa02fs', '/bucket2/', '9000000021_3.fits'),
  (9000000021, 8, 2, 'mwa03fs', '/bucket3/', '9000000021_4.fits'),
  (9000000021, 8, 2, 'mwa04fs', '/bucket4/', '9000000021_5.fits'),
  (9000000021, 8, 2, 'mwa01fs', '/bucket1/', '9000000021_6.fits'),
  (9000000022, 8, 2, 'mwa01fs', '/bucket2/', '9000000022_1.fits'),
  (9000000022, 8, 2, 'mwa02fs', '/bucket3/', '9000000022_2.fits'),
  (9000000022, 8, 2, 'mwa03fs', '/bucket4/', '9000000022_3.fits'),
  (9000000022, 8, 2, 'mwa04fs', '/bucket4/', '9000000022_4.fits'),
  (9000000022, 8, 2, 'mwa01fs', '/bucket4/', '9000000022_5.fits'),
  (9000000022, 8, 2, 'mwa02fs', '/bucket4/', '9000000022_6.fits'),
  (9000000023, 18, 3, 'mwaingest-23', NULL, '9000000023_1.fits'),
  (9000000023, 18, 3, 'mwaingest-23', NULL, '9000000023_2.fits'),
  (9000000023, 18, 3, 'mwaingest-23', NULL, '9000000023_3.fits'),
  (9000000023, 18, 3, 'mwaingest-23', NULL, '9000000023_4.fits'),
  (9000000023, 18, 3, 'mwaingest-23', NULL, '9000000023_5.fits'),
  (9000000023, 18, 3, 'mwaingest-23', NULL, '9000000023_6.fits'),
  (9000000031, 18, 3, 'mwaingest-31', NULL, '9000000031_1.fits'),
  (9000000031, 18, 3, 'mwaingest-31', NULL, '9000000031_2.fits'),
  (9000000031, 18, 3, 'mwaingest-31', NULL, '9000000031_3.fits'),
  (9000000031, 18, 3, 'mwaingest-31', NULL, '9000000031_4.fits'),
  (9000000031, 18, 3, 'mwaingest-31', NULL, '9000000031_5.fits'),
  (9000000031, 18, 3, 'mwaingest-31', NULL, '9000000031_6.fits'),
  (9000000032, 18, 3, 'mwaingest-32', NULL, '9000000032_1.fits'),
  (9000000032, 18, 3, 'mwaingest-32', NULL, '9000000032_2.fits'),
  (9000000032, 18, 3, 'mwaingest-32', NULL, '9000000032_3.fits'),
  (9000000032, 18, 3, 'mwaingest-32', NULL, '9000000032_4.fits'),
  (9000000032, 18, 3, 'mwaingest-32', NULL, '9000000032_5.fits'),
  (9000000032, 18, 3, 'mwaingest-32', NULL, '9000000032_6.fits'),
  (9000000033, 18, 3, 'mwaingest-33', NULL, '9000000033_1.fits'),
  (9000000033, 18, 3, 'mwaingest-33', NULL, '9000000033_2.fits'),
  (9000000033, 18, 3, 'mwaingest-33', NULL, '9000000033_3.fits'),
  (9000000033, 18, 3, 'mwaingest-33', NULL, '9000000033_4.fits'),
  (9000000033, 18, 3, 'mwaingest-33', NULL, '9000000033_5.fits'),
  (9000000033, 18, 3, 'mwaingest-33', NULL, '9000000033_6.fits'),
  (9000000041, 18, 3, 'mwaingest-41', NULL, '9000000041_1.fits'),
  (9000000041, 18, 3, 'mwaingest-41', NULL, '9000000041_2.fits'),
  (9000000041, 17, 3, 'mwaingest-41', NULL, '9000000041_3.sub'),
  (9000000041, 17, 3, 'mwaingest-41', NULL, '9000000041_4.sub'),
  (9000000042, 18, 3, 'mwaingest-42', NULL, '9000000042_1.fits');

INSERT INTO deletion_request_observation (request_id, obs_id)
VALUES
  (1, 9000000011),
  (1, 9000000012),
  (1, 9000000013),
  (2, 9000000021),
  (2, 9000000022),
  (2, 9000000023),
  (3, 9000000031),
  (3, 9000000032),
  (3, 9000000033),
  (4, 9000000041),
  (4, 9000000042);