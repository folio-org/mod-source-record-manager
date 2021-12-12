-- create jobExecution status enum if not exists
DO $$ BEGIN
    CREATE TYPE job_execution_status AS ENUM (
        'PARENT',
        'NEW',
        'FILE_UPLOADED',
        'PARSING_IN_PROGRESS',
        'PARSING_FINISHED',
        'PROCESSING_IN_PROGRESS',
        'PROCESSING_FINISHED',
        'COMMIT_IN_PROGRESS',
        'COMMITTED',
        'ERROR',
        'DISCARDED');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- create jobExecution subordination type enum if not exists
DO $$ BEGIN
    CREATE TYPE job_execution_subordination_type AS ENUM ('CHILD', 'PARENT_SINGLE', 'PARENT_MULTIPLE');
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- create jobExecution uiStatus enum if not exists
DO $$ BEGIN
    CREATE TYPE job_execution_ui_status AS ENUM (
        'PARENT',
        'INITIALIZATION',
        'PREPARING_FOR_PREVIEW',
        'READY_FOR_PREVIEW',
        'RUNNING',
        'RUNNING_COMPLETE',
        'ERROR',
        'DISCARDED'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- create jobExecution error status enum if not exists
DO $$ BEGIN
    CREATE TYPE job_execution_error_status AS ENUM (
        'SNAPSHOT_UPDATE_ERROR',
        'RECORD_UPDATE_ERROR',
        'FILE_PROCESSING_ERROR',
        'INSTANCE_CREATING_ERROR',
        'PROFILE_SNAPSHOT_CREATING_ERROR'
    );
EXCEPTION
    WHEN duplicate_object THEN NULL;
END $$;

-- create table job_execution
CREATE TABLE IF NOT EXISTS job_execution (
    id uuid PRIMARY KEY,
    hrid bigint,
    parent_job_id uuid,
    subordination_type ${myuniversity}_${mymodule}.job_execution_subordination_type,
    source_path text,
    file_name text,
    progress_current int,
    progress_total int,
    started_date timestamptz,
    completed_date timestamptz,
    status ${myuniversity}_${mymodule}.job_execution_status,
    ui_status ${myuniversity}_${mymodule}.job_execution_ui_status,
    error_status ${myuniversity}_${mymodule}.job_execution_error_status,
    job_user_first_name text,
    job_user_last_name text,
    user_id uuid,
    job_profile_id uuid,
    job_profile_name text,
    job_profile_data_type text,
    job_profile_snapshot_wrapper jsonb
);


-- create job_execution_status_idx index
CREATE INDEX IF NOT EXISTS job_execution_status_idx ON job_execution USING BTREE (status);

-- create job_execution_ui_status_idx index
CREATE INDEX IF NOT EXISTS job_execution_ui_status_idx ON job_execution USING BTREE (ui_status);

-- create job_execution_completed_date_idx index
CREATE INDEX IF NOT EXISTS job_execution_completed_date_idx ON job_execution USING BTREE (completed_date);

-- create job_execution_job_profile_id_idx index
CREATE INDEX IF NOT EXISTS job_execution_job_profile_id_idx ON job_execution USING BTREE (job_profile_id);

-- drop references to job_executions.id column
ALTER TABLE IF EXISTS job_execution_source_chunks DROP CONSTRAINT IF EXISTS jobexecutionid_job_executions_fkey;
ALTER TABLE IF EXISTS job_execution_progress DROP CONSTRAINT IF EXISTS jobexecutionid_job_executions_fkey;
ALTER TABLE IF EXISTS journal_records DROP CONSTRAINT IF EXISTS journal_records_job_execution_id_fkey;
ALTER TABLE IF EXISTS job_monitoring DROP CONSTRAINT IF EXISTS job_monitoring_job_execution_id_fkey;

