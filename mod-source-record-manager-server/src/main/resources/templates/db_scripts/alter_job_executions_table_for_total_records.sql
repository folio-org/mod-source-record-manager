alter table job_execution
  add column if not exists total_records_in_file integer;
