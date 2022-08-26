-- This actions should be run if there is non-clean deployment to return structure for source_chunks table(this issue was caused via changing creation this table not via rmb)

-- Add NOT NULL constraint
ALTER TABLE IF EXISTS job_execution_source_chunks ALTER COLUMN jobexecutionid SET NOT NULL;

-- Add triggers/functions
CREATE TRIGGER set_id_in_jsonb
BEFORE INSERT OR UPDATE
ON job_execution_source_chunks
FOR EACH ROW
EXECUTE PROCEDURE set_id_in_jsonb();

CREATE FUNCTION update_job_execution_source_chunks_references () RETURNS trigger AS
$$
BEGIN
NEW.jobExecutionId = (NEW.jsonb->>'jobExecutionId');
RETURN NEW;
END;
$$
LANGUAGE 'plpgsql' COST 100;

ALTER FUNCTION update_job_execution_source_chunks_references () OWNER TO folio;

CREATE TRIGGER update_job_execution_source_chunks_references
BEFORE INSERT OR UPDATE
ON job_execution_source_chunks
FOR EACH ROW
EXECUTE PROCEDURE update_job_execution_source_chunks_references();
