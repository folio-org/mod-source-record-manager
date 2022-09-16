-- create job_execution_started_date_idx index
CREATE INDEX IF NOT EXISTS job_execution_started_date_idx ON job_execution USING BTREE (started_date);
