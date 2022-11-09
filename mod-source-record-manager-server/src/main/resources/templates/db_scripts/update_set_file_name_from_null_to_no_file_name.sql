update ${myuniversity}_${mymodule}.job_execution SET file_name = 'No file name' WHERE file_name IS NULL AND subordination_type != 'PARENT_MULTIPLE';
update ${myuniversity}_${mymodule}.job_execution SET source_path = 'No file name' WHERE source_path IS NULL AND subordination_type != 'PARENT_MULTIPLE';
