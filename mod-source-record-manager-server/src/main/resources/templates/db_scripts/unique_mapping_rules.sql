-- Delete bogus rules: Rules without "mappingRules" property in their jsonb field; and
-- all but the first (smallest id) rule of that recordType with "mappingRules" property.

DELETE FROM mapping_rules WHERE record_type = 'MARC_BIB' AND id IS DISTINCT FROM
    (SELECT min(id) FROM mapping_rules WHERE record_type = 'MARC_BIB' AND jsonb ? 'mappingRules');
DELETE FROM mapping_rules WHERE record_type = 'MARC_HOLDING' AND id IS DISTINCT FROM
    (SELECT min(id) FROM mapping_rules WHERE record_type = 'MARC_HOLDING' AND jsonb ? 'mappingRules');
DELETE FROM mapping_rules WHERE record_type = 'MARC_AUTHORITY' AND id IS DISTINCT FROM
    (SELECT min(id) FROM mapping_rules WHERE record_type = 'MARC_AUTHORITY' AND jsonb ? 'mappingRules');

DO $$
BEGIN
  BEGIN
    ALTER TABLE mapping_rules ADD CONSTRAINT unique_record_type UNIQUE (record_type);
  EXCEPTION
    WHEN duplicate_table THEN NULL;
  END;
END $$;
