-- Custom script to create a function to check if processing is_completed for JobExecution.
-- Changes in this file will not result in an update of the function.
-- To change the function, update this script and copy it to the appropriate scripts.snippet field of the schema.json

CREATE OR REPLACE FUNCTION is_processing_completed(jobExecId uuid)
RETURNS boolean AS $completed$
DECLARE
	completed boolean;
BEGIN
    SELECT count(id) =
        (
            SELECT count(id)
            FROM job_execution_source_chunks
            WHERE (jsonb->>'jobExecutionId')::uuid = jobExecId
        ) INTO completed
    FROM job_execution_source_chunks
    WHERE (jsonb->>'jobExecutionId')::uuid = jobExecId AND jsonb->>'state' IN ('COMPLETED', 'ERROR');

    RETURN completed;
END;
$completed$ LANGUAGE plpgsql;
