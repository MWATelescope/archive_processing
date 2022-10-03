CREATE SCHEMA car_schedule;
CREATE TABLE car_schedule.drivers (
  id SERIAL PRIMARY KEY, 
  name VARCHAR(100) NOT NULL, 
  license_number VARCHAR(100) NOT NULL
);
INSERT INTO car_schedule.drivers (name, license_number) 
VALUES 
  ('Jeff', 'abc123'), 
  ('Dan', 'def456');