-- Custom script to create a function to check if any errors occurred for JobExecution.
-- Changes in this file will not result in an update of the function.
-- To change the function, update this script and copy it to the appropriate scripts.snippet field of the schema.json

CREATE OR REPLACE FUNCTION processing_contains_error_chunks(jobExecId uuid)
RETURNS boolean AS $has_errors$
DECLARE
	has_errors boolean;
BEGIN
 SELECT count(_id) > 0 into has_errors
 FROM
   job_execution_source_chunks
    WHERE (jsonb->>'jobExecutionId')::uuid = jobExecId
	 AND jsonb->>'state' = 'ERROR';
RETURN has_errors;
END;
$has_errors$ LANGUAGE plpgsql;
