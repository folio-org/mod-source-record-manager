CREATE TABLE IF NOT EXISTS job_execution_source_chunks
(
  id             uuid  NOT NULL,
  jsonb          jsonb NOT NULL,
  jobexecutionid uuid  NOT NULL,
  CONSTRAINT job_execution_source_chunks_pkey PRIMARY KEY (id),
  CONSTRAINT job_execution_source_chunks_jobexecutionid_fkey FOREIGN KEY (jobexecutionid)
    REFERENCES job_execution (id)
);

CREATE INDEX IF NOT EXISTS job_execution_source_chunks_last_index ON job_execution_source_chunks USING btree ("left"(lower(f_unaccent((jsonb ->> 'last'::text))), 600));
CREATE INDEX IF NOT EXISTS job_execution_source_chunks_jobexecutionid_index ON job_execution_source_chunks USING BTREE (jobExecutionId);

DO $$ BEGIN
  ALTER TABLE IF EXISTS job_execution_source_chunks
    ADD CONSTRAINT job_execution_source_chunks_jobexecutionid_fkey FOREIGN KEY (jobexecutionid) REFERENCES job_execution(id);
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;
