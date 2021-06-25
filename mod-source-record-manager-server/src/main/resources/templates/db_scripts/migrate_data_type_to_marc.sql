-- change values: from data type MARC_BIB, MARC_AUTHORITY, MARC_HOLDING to MARC
UPDATE ${myuniversity}_${mymodule}.job_executions
SET jsonb = jsonb_set(jsonb, '{jobProfileInfo, dataType}', '"MARC"')
WHERE jsonb -> 'jobProfileInfo' ->>'dataType' IN ('MARC_BIB', 'MARC_AUTHORITY', 'MARC_HOLDINGS');
