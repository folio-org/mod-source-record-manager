update diku_mod_source_record_manager.job_execution SET file_name = 'No file name' WHERE file_name IS NULL;
ALTER TABLE diku_mod_source_record_manager.job_execution ALTER COLUMN file_name SET NOT NULL;
update diku_mod_source_record_manager.job_execution SET source_path = 'No file name' WHERE source_path IS NULL;
ALTER TABLE diku_mod_source_record_manager.job_execution ALTER COLUMN source_path SET NOT NULL;


