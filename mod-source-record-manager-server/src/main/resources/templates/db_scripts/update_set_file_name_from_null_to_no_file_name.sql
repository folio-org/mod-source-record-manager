update ${myuniversity}_${mymodule}.job_execution SET file_name = 'No file name' WHERE file_name IS NULL;
ALTER TABLE ${myuniversity}_${mymodule}.job_execution ALTER COLUMN file_name SET NOT NULL;
update ${myuniversity}_${mymodule}.job_execution SET source_path = 'No file name' WHERE source_path IS NULL;
ALTER TABLE ${myuniversity}_${mymodule}.job_execution ALTER COLUMN source_path SET NOT NULL;

