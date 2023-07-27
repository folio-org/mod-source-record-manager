alter type job_execution_subordination_type
  add value if not exists 'COMPOSITE_PARENT';

alter type job_execution_subordination_type
  add value if not exists 'COMPOSITE_CHILD';

alter table job_execution
  add column if not exists job_part_number integer default 1;

alter table job_execution
  add column if not exists total_job_parts integer default 1;
