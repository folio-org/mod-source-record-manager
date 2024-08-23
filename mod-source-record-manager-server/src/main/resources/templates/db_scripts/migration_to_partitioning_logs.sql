CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_entity_type (
  id                    UUID,
  job_execution_id      UUID,
  source_id             UUID,
  entity_type           text,
  entity_id             text,
  entity_hrid           text,
  action_type           text,
  action_status         text,
  action_date           timestamp,
  source_record_order   integer NULL,
  error                 text NULL,
  title                 text NULL,
  instance_id           text,
  holdings_id           text,
  order_id              text,
  permanent_location_id text,
  tenant_id             text,
  PRIMARY KEY (id, entity_type)
) partition by list (entity_type);

CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_marc_bibliographic PARTITION OF journal_records_entity_type FOR VALUES IN ('MARC_BIBLIOGRAPHIC');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_po_line PARTITION OF journal_records_entity_type FOR VALUES IN ('PO_LINE');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_marc_holdings PARTITION OF journal_records_entity_type FOR VALUES IN ('MARC_HOLDINGS');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_marc_authority PARTITION OF journal_records_entity_type FOR VALUES IN ('MARC_AUTHORITY');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_holdings PARTITION OF journal_records_entity_type FOR VALUES IN ('HOLDINGS');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_authority PARTITION OF journal_records_entity_type FOR VALUES IN ('AUTHORITY');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_instance PARTITION OF journal_records_entity_type FOR VALUES IN ('INSTANCE');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_item PARTITION OF journal_records_entity_type FOR VALUES IN ('ITEM');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_invoice PARTITION OF journal_records_entity_type FOR VALUES IN ('INVOICE');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_edifact PARTITION OF journal_records_entity_type FOR VALUES IN ('EDIFACT');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_empty_entity_type PARTITION OF journal_records_entity_type FOR VALUES IN ('');

INSERT INTO ${myuniversity}_${mymodule}.journal_records_entity_type (id, job_execution_id, source_id, entity_type, entity_id, entity_hrid,
                                         action_type, action_status, action_date, source_record_order, error, title,
                                         instance_id, holdings_id, order_id, permanent_location_id, tenant_id)
SELECT id, job_execution_id, source_id, entity_type, entity_id, entity_hrid, action_type, action_status,
       action_date, source_record_order, error, title, instance_id, holdings_id, order_id, permanent_location_id,
       tenant_id
FROM ${myuniversity}_${mymodule}.journal_records;

ALTER TABLE ${myuniversity}_${mymodule}.journal_records RENAME TO journal_records_backup;
ALTER TABLE ${myuniversity}_${mymodule}.journal_records_entity_type RENAME TO journal_records;

DO $$
DECLARE
  index_record RECORD;
  new_indexdef TEXT;
BEGIN
FOR index_record IN
  SELECT indexname, indexdef
  FROM pg_indexes
  WHERE schemaname = '${myuniversity}_${mymodule}' AND tablename = 'journal_records'
    LOOP
      new_indexdef := REPLACE(index_record.indexdef,
             '${myuniversity}_${mymodule}.journal_records_backup', '${myuniversity}_${mymodule}.journal_records');
      EXECUTE new_indexdef;
    END LOOP;
END $$;
