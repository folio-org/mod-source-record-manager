-- Custom script to create a function to check if processing is_completed and whether it has_errors for JobExecution.
-- Changes in this file will not result in an update of the function.
-- To change the function, update this script and copy it to the appropriate scripts.snippet field of the schema.json

CREATE OR REPLACE FUNCTION get_processing_state(jobExecId uuid)
RETURNS TABLE (completed boolean) AS $result$
BEGIN
RETURN QUERY
SELECT count(_id) =
	(SELECT count(_id)
		FROM job_execution_source_chunks
 			WHERE (jsonb->>'jobExecutionId')::uuid = jobExecId)
	as completed FROM
	job_execution_source_chunks
	WHERE (jsonb->>'jobExecutionId')::uuid = jobExecId
		AND jsonb->>'state' IN ('COMPLETED', 'ERROR')
UNION ALL
(SELECT count(_id) > 0
 FROM
   job_execution_source_chunks
    WHERE (jsonb->>'jobExecutionId')::uuid = jobExecId
	 AND jsonb->>'state' = 'ERROR');
END;
$result$ LANGUAGE plpgsql;
