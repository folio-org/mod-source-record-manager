SELECT
  CASE
    WHEN job_execution_progress.jsonb IS NULL THEN job_executions.jsonb #- '{jobProfileSnapshotWrapper}'
    ELSE jsonb_set(job_executions.jsonb #- '{jobProfileSnapshotWrapper}', '{progress}', jsonb_build_object('jobExecutionId', job_execution_progress.jsonb -> 'jobExecutionId',
                                                                          'total', job_execution_progress.jsonb -> 'total',
                                                                          'current', (job_execution_progress.jsonb ->> 'currentlySucceeded')::int + (job_execution_progress.jsonb ->> 'currentlyFailed')::int))
  END as jsonb, (SELECT COUNT(id) FROM %s.job_executions %s) AS total_rows
FROM %s.job_executions
LEFT JOIN %s.job_execution_progress ON job_executions.id = job_execution_progress.jobExecutionId
%s
%s
LIMIT %s OFFSET %s;
