-- Request to delete existing update_job_execution_progress_references function.
DROP FUNCTION IF EXISTS update_job_execution_progress_references() CASCADE;

-- Request to delete existing job_execution_progress table.
DROP TABLE IF EXISTS job_execution_progress CASCADE;

-- Create table job_execution_progress
CREATE TABLE IF NOT EXISTS job_execution_progress (
  job_execution_id uuid PRIMARY KEY,
  total_records_count int,
  succeeded_records_count int,
  error_records_count int
);

-- Create references to job_execution.id column if it's not exist
DO $$ BEGIN
  ALTER TABLE IF EXISTS job_execution_progress
    ADD CONSTRAINT job_execution_progress_job_execution_id_fkey FOREIGN KEY (job_execution_id) REFERENCES job_execution(id);
EXCEPTION
  WHEN duplicate_object THEN NULL;
END $$;
