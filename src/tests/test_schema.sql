CREATE TABLE deletion_requests (
  id SERIAL PRIMARY KEY,
  created_datetime time DEFAULT NOW(),
  cancelled_datetime time DEFAULT NULL,
  actioned_datetime time DEFAULT NULL,
  request_name varchar(20)
);

CREATE TABLE mwa_setting (
  starttime INT PRIMARY KEY,
  deleted BOOLEAN DEFAULT NULL,
  deleted_timestamp time DEFAULT NULL
);

CREATE TABLE deletion_request_observation (
  request_id INT,
  obs_id INT,
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
  id SERIAL PRIMARY KEY,
  location INT,
  bucket VARCHAR(20),
  folder VARCHAR(256),
  filename VARCHAR(256),
  observation_num INT,
  filetype INT,
  deleted BOOLEAN DEFAULT FALSE,
  deleted_timestamp time DEFAULT NULL,
  remote_archived BOOLEAN DEFAULT TRUE,
  CONSTRAINT fk_df_mwa_setting
    FOREIGN KEY(observation_num)
      REFERENCES mwa_setting(starttime)
        ON DELETE SET NULL
);

INSERT INTO deletion_requests (request_name) 
VALUES 
  ('abc123'), 
  ('def456'),
  ('ghi789');
  
INSERT INTO mwa_setting (starttime)
VALUES
  (11),
  (12),
  (13),
  (21),
  (22),
  (23),
  (31),
  (32),
  (33);

INSERT INTO data_files(observation_num, filetype, location, bucket, folder, filename)
VALUES
  (11, 8, 2, 'mwa01fs', '/bucket1/', '11_1.fits'),
  (11, 8, 2, 'mwa02fs', '/bucket2/', '11_2.fits'),
  (11, 8, 2, 'mwa03fs', '/bucket3/', '11_3.fits'),
  (11, 8, 2, 'mwa04fs', '/bucket4/', '11_4.fits'),
  (11, 8, 2, 'mwa01fs', '/bucket1/', '11_5.fits'),
  (11, 8, 2, 'mwa01fs', '/bucket1/', '11_6.fits'),
  (12, 8, 2, 'mwa02fs', '/bucket2/', '12_1.fits'),
  (12, 8, 2, 'mwa03fs', '/bucket3/', '12_2.fits'),
  (12, 8, 2, 'mwa04fs', '/bucket4/', '12_3.fits'),
  (12, 8, 2, 'mwa01fs', '/bucket1/', '12_4.fits'),
  (12, 8, 2, 'mwa02fs', '/bucket2/', '12_5.fits'),
  (12, 8, 2, 'mwa03fs', '/bucket3/', '12_6.fits'),
  (13, 8, 2, 'mwa04fs', '/bucket4/', '13_1.fits'),
  (13, 8, 2, 'mwa01fs', '/bucket1/', '13_2.fits'),
  (13, 8, 2, 'mwa02fs', '/bucket2/', '13_3.fits'),
  (13, 8, 2, 'mwa03fs', '/bucket3/', '13_4.fits'),
  (13, 8, 2, 'mwa03fs', '/bucket3/', '13_5.fits'),
  (13, 8, 2, 'mwa03fs', '/bucket3/', '13_6.fits'),
  (21, 8, 2, 'mwa04fs', '/bucket4/', '21_1.fits'),
  (21, 8, 2, 'mwa01fs', '/bucket1/', '21_2.fits'),
  (21, 8, 2, 'mwa02fs', '/bucket2/', '21_3.fits'),
  (21, 8, 2, 'mwa03fs', '/bucket3/', '21_4.fits'),
  (21, 8, 2, 'mwa04fs', '/bucket4/', '21_5.fits'),
  (21, 8, 2, 'mwa01fs', '/bucket1/', '21_6.fits'),
  (22, 8, 2, 'mwa01fs', '/bucket2/', '22_1.fits'),
  (22, 8, 2, 'mwa02fs', '/bucket3/', '22_2.fits'),
  (22, 8, 2, 'mwa03fs', '/bucket4/', '22_3.fits'),
  (22, 8, 2, 'mwa04fs', '/bucket4/', '22_4.fits'),
  (22, 8, 2, 'mwa01fs', '/bucket4/', '22_5.fits'),
  (22, 8, 2, 'mwa02fs', '/bucket4/', '22_6.fits'),
  (23, 18, 3, 'mwaingest-23', NULL, '23_1.fits'),
  (23, 18, 3, 'mwaingest-23', NULL, '23_2.fits'),
  (23, 18, 3, 'mwaingest-23', NULL, '23_3.fits'),
  (23, 18, 3, 'mwaingest-23', NULL, '23_4.fits'),
  (23, 18, 3, 'mwaingest-23', NULL, '23_5.fits'),
  (23, 18, 3, 'mwaingest-23', NULL, '23_6.fits'),
  (31, 18, 3, 'mwaingest-31', NULL, '31_1.fits'),
  (31, 18, 3, 'mwaingest-31', NULL, '31_2.fits'),
  (31, 18, 3, 'mwaingest-31', NULL, '31_3.fits'),
  (31, 18, 3, 'mwaingest-31', NULL, '31_4.fits'),
  (31, 18, 3, 'mwaingest-31', NULL, '31_5.fits'),
  (31, 18, 3, 'mwaingest-31', NULL, '31_6.fits'),
  (32, 18, 3, 'mwaingest-32', NULL, '32_1.fits'),
  (32, 18, 3, 'mwaingest-32', NULL, '32_2.fits'),
  (32, 18, 3, 'mwaingest-32', NULL, '32_3.fits'),
  (32, 18, 3, 'mwaingest-32', NULL, '32_4.fits'),
  (32, 18, 3, 'mwaingest-32', NULL, '32_5.fits'),
  (32, 18, 3, 'mwaingest-32', NULL, '32_6.fits'),
  (33, 18, 3, 'mwaingest-33', NULL, '33_1.fits'),
  (33, 18, 3, 'mwaingest-33', NULL, '33_2.fits'),
  (33, 18, 3, 'mwaingest-33', NULL, '33_3.fits'),
  (33, 18, 3, 'mwaingest-33', NULL, '33_4.fits'),
  (33, 18, 3, 'mwaingest-33', NULL, '33_5.fits'),
  (33, 18, 3, 'mwaingest-33', NULL, '33_6.fits');

INSERT INTO deletion_request_observation (request_id, obs_id)
VALUES
  (1, 11),
  (1, 12),
  (1, 13),
  (2, 21),
  (2, 22),
  (2, 23),
  (3, 31),
  (3, 32),
  (3, 33);