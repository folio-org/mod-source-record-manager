CREATE TABLE IF NOT EXISTS incoming_records (
  id               uuid  NOT NULL,
  job_execution_id uuid  NOT NULL,
  incoming_record  jsonb NOT NULL,
  CONSTRAINT incoming_records_pkey PRIMARY KEY (id),
  CONSTRAINT incoming_records_jobexecutionid_fkey FOREIGN KEY (job_execution_id)
    REFERENCES job_execution (id)
);

CREATE INDEX IF NOT EXISTS incoming_records_jobexecutionid_index ON incoming_records USING BTREE (job_execution_id);
