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
  tenant_id             text
) partition by list (entity_type);

CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_marc_bibliographic PARTITION OF journal_records_entity_type FOR VALUES IN ('MARC_BIBLIOGRAPHIC');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_po_line PARTITION OF journal_records_entity_type FOR VALUES IN ('PO_LINE');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_marc_holdings PARTITION OF journal_records_entity_type FOR VALUES IN ('MARC_HOLDINGS');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_marc_authority PARTITION OF journal_records_entity_type FOR VALUES IN ('MARC_AUTHORITY');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_holdings PARTITION OF journal_records_entity_type FOR VALUES IN ('HOLDINGS');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_authority PARTITION OF journal_records_entity_type FOR VALUES IN ('AUTHORITY');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_instance PARTITION OF journal_records_entity_type FOR VALUES IN ('INSTANCE');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_item PARTITION OF journal_records_entity_type FOR VALUES IN ('ITEM');
CREATE TABLE IF NOT EXISTS ${myuniversity}_${mymodule}.journal_records_order PARTITION OF journal_records_entity_type FOR VALUES IN ('ORDER');
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

CREATE UNIQUE INDEX journal_records_et_pkey ON ${myuniversity}_${mymodule}.journal_records_entity_type USING btree (id, entity_type);
CREATE INDEX journal_records_et_job_execution_id_idx ON ${myuniversity}_${mymodule}.journal_records_entity_type USING btree (job_execution_id);
CREATE INDEX journal_records_et_source_id_idx ON ${myuniversity}_${mymodule}.journal_records_entity_type USING btree (source_id);
CREATE INDEX journal_records_et_action_type_idx ON ${myuniversity}_${mymodule}.journal_records_entity_type USING btree (action_type);

ALTER TABLE ${myuniversity}_${mymodule}.journal_records RENAME TO journal_records_backup;
ALTER TABLE ${myuniversity}_${mymodule}.journal_records_entity_type RENAME TO journal_records;

DROP FUNCTION IF EXISTS get_record_processing_log_bak(uuid, uuid);

CREATE OR REPLACE FUNCTION get_record_processing_log_bak(jobExecutionId uuid, recordId uuid)
  RETURNS TABLE(job_execution_id uuid, incoming_record_id uuid, source_id uuid, source_record_order integer, title text, source_record_action_status text, source_entity_error text, source_record_tenant_id text, instance_action_status text, instance_entity_id text, instance_entity_hrid text, instance_entity_error text, instance_entity_tenant_id text, holdings_action_status text, holdings_entity_hrid text, holdings_entity_id text, holdings_permanent_location_id text, holdings_entity_error text, item_action_status text, item_entity_hrid text, item_entity_id text, item_entity_error text, authority_action_status text, authority_entity_id text, authority_entity_error text, po_line_action_status text, po_line_entity_id text, po_line_entity_hrid text, po_line_entity_error text, order_entity_id text, invoice_action_status text, invoice_entity_id text[], invoice_entity_hrid text[], invoice_entity_error text, invoice_line_action_status text, invoice_line_entity_id text, invoice_line_entity_hrid text, invoice_line_entity_error text)
AS $$
BEGIN
  RETURN QUERY
    WITH temp_result AS (SELECT id, journal_records_backup.job_execution_id, journal_records_backup.source_id, journal_records_backup.entity_type, journal_records_backup.entity_id, journal_records_backup.entity_hrid,
                                CASE WHEN action_type = 'PARSE'
                                       THEN 'PARSED'
                                     WHEN error_max != '' OR action_type = 'NON_MATCH'
                                       THEN 'DISCARDED'
                                     WHEN action_type = 'CREATE'
                                       THEN 'CREATED'
                                     WHEN action_type = 'UPDATE'
                                       THEN 'UPDATED'
                                  END AS action_type, journal_records_backup.action_status, journal_records_backup.action_date, journal_records_backup.source_record_order, journal_records_backup.error, journal_records_backup.title, journal_records_backup.tenant_id, journal_records_backup.instance_id, journal_records_backup.holdings_id, journal_records_backup.order_id, journal_records_backup.permanent_location_id
                         FROM journal_records_backup
                                INNER JOIN
                              (SELECT entity_type as entity_type_max, entity_id as entity_id_max,action_status as action_status_max, max(error) AS error_max,(array_agg(id ORDER BY array_position(array['CREATE', 'UPDATE', 'NON_MATCH'], action_type)))[1] AS id_max
                               FROM journal_records_backup
                               WHERE journal_records_backup.job_execution_id = jobExecutionId AND journal_records_backup.source_id = recordId AND journal_records_backup.entity_type NOT IN ('EDIFACT', 'INVOICE') AND action_type != 'MATCH'
                               GROUP BY entity_type,entity_id,action_status) AS action_type_by_source ON journal_records_backup.id = action_type_by_source.id_max
                         UNION ALL
                         SELECT id, journal_records_backup.job_execution_id, journal_records_backup.source_id, journal_records_backup.entity_type, journal_records_backup.entity_id, journal_records_backup.entity_hrid,
                                CASE WHEN error_max != '' OR action_type = 'MATCH' THEN 'DISCARDED'
                                  END AS action_type, journal_records_backup.action_status, journal_records_backup.action_date, journal_records_backup.source_record_order, journal_records_backup.error, journal_records_backup.title, journal_records_backup.tenant_id, journal_records_backup.instance_id, journal_records_backup.holdings_id, journal_records_backup.order_id, journal_records_backup.permanent_location_id
                         FROM journal_records_backup
                                INNER JOIN
                              (SELECT entity_type as entity_type_max, entity_id as entity_id_max,action_status as action_status_max, max(error) AS error_max,(array_agg(id ORDER BY array_position(array['NON_MATCH', 'MATCH'], action_type)))[1] AS id_max
                               FROM journal_records_backup
                               WHERE journal_records_backup.job_execution_id = jobExecutionId AND journal_records_backup.source_id = recordId AND journal_records_backup.entity_type NOT IN ('EDIFACT', 'INVOICE') AND action_type = 'MATCH'
                                 AND NOT EXISTS (SELECT 1 FROM journal_records_backup WHERE journal_records_backup.job_execution_id = jobExecutionId AND journal_records_backup.source_id = recordId AND action_type NOT IN ('MATCH', 'PARSE'))
                               GROUP BY entity_type,entity_id,action_status) AS action_type_by_source ON journal_records_backup.id = action_type_by_source.id_max)
      (SELECT
         COALESCE(marc.job_execution_id,instances.job_execution_id,holdings.job_execution_id,items.job_execution_id) AS job_execution_id,
         COALESCE(marc.source_id, instances.source_id, holdings.source_id, items.source_id, authority.source_id) as incoming_record_id,
         marc_entity_id::uuid AS source_id,
         COALESCE(marc.source_record_order,instances.source_record_order,holdings.source_record_order,items.source_record_order) AS source_record_order,
         COALESCE(marc.title,instances.title,holdings.title,items.title) AS title,
         marc.action_type AS source_record_action_status,
         marc.error AS source_entity_error,
         marc.tenant_id AS source_record_tenant_id,

         instances.action_type AS instance_action_status,
         COALESCE(instances.entity_id,holdings.instance_id,items.instance_id) AS instance_entity_id,
         instances.entity_hrid AS instance_entity_hrid,
         instances.error AS instance_entity_error,
         instances.tenant_id AS instance_entity_tenant_id,

         holdings.action_type AS holdings_action_status,
         holdings.entity_hrid AS holdings_entity_hrid,
         COALESCE(holdings.entity_id,items.holdings_id) AS holdings_entity_id,
         holdings.permanent_location_id AS holdings_permanent_location_id,
         holdings.error AS holdings_entity_error,

         items.action_type AS item_action_status,
         items.entity_hrid AS item_entity_hrid,
         items.entity_id AS item_entity_id,
         items.error AS item_entity_error,

         authority.action_type AS authority_action_status,
         authority.entity_id AS authority_entity_id,
         authority.error AS authority_entity_error,

         po_lines.action_type AS po_line_action_status,
         po_lines.entity_id AS po_line_entity_id,
         po_lines.entity_hrid AS po_line_entity_hrid,
         po_lines.error AS po_line_entity_error,
         po_lines.order_id AS order_entity_id,

         null AS invoice_action_status,
         null AS invoice_entity_id,
         null AS invoice_entity_hrid,
         null AS invoice_entity_error,
         null AS invoice_line_action_status,
         null AS invoice_line_entity_id,
         null AS invoice_line_entity_hrid,
         null AS invoice_line_entity_error
       FROM (SELECT temp_result.source_id FROM temp_result WHERE action_type = 'PARSED') as parsed
              LEFT JOIN
            (SELECT temp_result.job_execution_id, entity_id, temp_result.title, temp_result.source_record_order, action_type, error, temp_result.source_id, temp_result.tenant_id
             FROM temp_result WHERE entity_type IN ('MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY', 'PO_LINE') AND entity_id IS NOT NULL
             UNION ALL
             SELECT temp_result.job_execution_id, entity_id, temp_result.title, temp_result.source_record_order, action_type, error, temp_result.source_id, temp_result.tenant_id
             FROM temp_result
             WHERE entity_type IN ('MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY', 'PO_LINE') AND entity_id IS NULL AND NOT EXISTS
               (SELECT 1
                FROM temp_result as tr2
                WHERE tr2.entity_type IN ('MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY', 'PO_LINE') AND tr2.source_id = temp_result.source_id and tr2.entity_id IS NOT NULL)) AS marc
            ON marc.source_id = parsed.source_id
              LEFT JOIN
            (SELECT action_type, entity_id, temp_result.source_id, entity_hrid, error, temp_result.job_execution_id, temp_result.title, temp_result.source_record_order, temp_result.tenant_id
             FROM temp_result WHERE entity_type = 'INSTANCE' AND entity_id IS NOT NULL
             UNION ALL
             SELECT action_type, entity_id, temp_result.source_id, entity_hrid, error, temp_result.job_execution_id, temp_result.title, temp_result.source_record_order, temp_result.tenant_id
             FROM temp_result
             WHERE entity_type = 'INSTANCE' AND entity_id IS NULL AND NOT EXISTS
               (SELECT 1
                FROM temp_result as tr2
                WHERE tr2.entity_type = 'INSTANCE' AND tr2.source_id = temp_result.source_id and tr2.entity_id IS NOT NULL)) AS instances
            ON marc.source_id = instances.source_id
              LEFT JOIN
            (SELECT action_type, entity_id, temp_result.source_id, error, temp_result.job_execution_id, temp_result.title, temp_result.source_record_order
             FROM temp_result WHERE entity_type = 'AUTHORITY') AS authority
            ON authority.source_id = marc.source_id
              LEFT JOIN
            (SELECT action_type,entity_id,entity_hrid,temp_result.source_id,error,order_id,temp_result.job_execution_id,temp_result.title,temp_result.source_record_order
             FROM temp_result WHERE entity_type = 'PO_LINE') AS po_lines
            ON po_lines.source_id = marc.source_id
              FULL JOIN
            (SELECT tmp.action_type, tmp.entity_type, tmp.entity_id, tmp.entity_hrid, tmp.error, tmp.instance_id,
                    tmp.permanent_location_id, tmp.job_execution_id, tmp.source_id, tmp.title, tmp.source_record_order
             FROM temp_result tmp
                    INNER JOIN
                  (SELECT
                     CASE
                       WHEN EXISTS (SELECT condition_result.entity_id FROM temp_result condition_result
                                    WHERE (condition_result.action_type='CREATED' AND condition_result.entity_type='HOLDINGS')
                                       OR
                                      (condition_result.action_type='DISCARDED' AND condition_result.error != '' AND condition_result.entity_type='HOLDINGS'))
                         THEN
                         (SELECT deep_nested.id
                          FROM temp_result deep_nested
                          WHERE
                            (deep_nested.action_type='CREATED' AND deep_nested.id = nested_result.id)
                             OR
                            (deep_nested.action_type='DISCARDED' AND deep_nested.error != '' AND deep_nested.id = nested_result.id))
                       ELSE
                         nested_result.id
                       END
                   FROM temp_result nested_result) AS joining_table
                  ON tmp.id = joining_table.id
             WHERE  tmp.entity_type='HOLDINGS')
              AS holdings
            ON instances.entity_id = holdings.instance_id
              FULL JOIN
            (SELECT tmp.action_type, tmp.entity_id, tmp.holdings_id, tmp.entity_hrid, tmp.error, tmp.instance_id,
                    tmp.job_execution_id, tmp.source_id, tmp.title, tmp.source_record_order
             FROM temp_result tmp
                    INNER JOIN
                  (SELECT
                     CASE
                       WHEN EXISTS (SELECT condition_result.entity_id FROM temp_result condition_result
                                    WHERE (condition_result.action_type IN ('CREATED','UPDATED') AND condition_result.entity_type='ITEM')
                                       OR
                                      (condition_result.action_type='DISCARDED' AND condition_result.error != '' AND condition_result.entity_type='ITEM'))
                         THEN
                         (SELECT deep_nested.id
                          FROM temp_result deep_nested
                          WHERE
                            (deep_nested.action_type IN ('CREATED','UPDATED') AND deep_nested.id = nested_result.id)
                             OR
                            (deep_nested.action_type='DISCARDED' AND deep_nested.error != '' AND deep_nested.id = nested_result.id))
                       ELSE
                         nested_result.id
                       END
                   FROM temp_result nested_result) AS joining_table
                  ON tmp.id = joining_table.id
             WHERE  tmp.entity_type='ITEM') AS items
            ON holdings.entity_id = items.holdings_id
              LEFT JOIN (
         SELECT entity_id AS marc_entity_id, temp_result.source_id AS marc_source_id
         FROM temp_result WHERE entity_type IN ('MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY') AND entity_id IS NOT NULL
       ) AS marc_identifiers ON marc.source_id = marc_identifiers.marc_source_id
       ORDER BY holdings.entity_hrid)
    UNION
    SELECT invoice_line_info.job_execution_id,
           records_actions.source_id as incoming_record_id,
           records_actions.source_id as source_id,
           records_actions.source_record_order,
           invoice_line_info.title,
           CASE WHEN edifact_errors_number != 0 THEN 'DISCARDED'
                WHEN edifact_actions[array_length(edifact_actions, 1)] = 'CREATE' THEN 'CREATED'
             END AS source_record_action_status,
           records_actions.source_record_error[1],
           records_actions.source_record_tenant_id,
           null AS instance_action_status,
           null AS instance_entity_id,
           null AS instance_entity_hrid,
           null AS instance_entity_error,
           null AS instance_entity_tenant_id,
           null AS holdings_action_status,
           null AS holdings_entity_hrid,
           null AS holdings_entity_id,
           null AS holdings_permanent_location_id,
           null AS holdings_entity_error,
           null AS item_action_status,
           null AS item_entity_hrid,
           null AS item_entity_id,
           null AS item_entity_error,
           null AS authority_action_status,
           null AS authority_entity_id,
           null AS authority_entity_error,
           null AS po_line_action_status,
           null AS po_line_entity_id,
           null AS po_line_entity_hrid,
           null AS po_line_entity_error,
           null AS order_entity_id,
           get_entity_status(records_actions.invoice_actions, records_actions.invoice_errors_number) AS invoice_action_status,
           records_actions.invoice_entity_id,
           records_actions.invoice_entity_hrid,
           records_actions.invoice_entity_error[1],
           CASE WHEN action_status = 'ERROR' THEN 'DISCARDED'
                WHEN inv_line_actions = 'CREATE' THEN 'CREATED'
             END AS invoice_line_action_status,
           invoice_line_info.invoice_line_entity_id,
           invoice_line_info.invoice_line_entity_hrid,
           invoice_line_info.invoice_line_entity_error
    FROM (
           SELECT journal_records_backup.source_id,
                  journal_records_backup.job_execution_id,
                  journal_records_backup.title,
                  journal_records_backup.action_type AS inv_line_actions,
                  action_status,
                  entity_hrid AS invoice_line_entity_hrid,
                  entity_id AS invoice_line_entity_id,
                  error AS invoice_line_entity_error
           FROM journal_records_backup
           WHERE journal_records_backup.id = recordId AND journal_records_backup.entity_type = 'INVOICE' AND journal_records_backup.title != 'INVOICE'
         ) AS invoice_line_info
           LEFT JOIN LATERAL (
      SELECT journal_records_backup.source_id,
             journal_records_backup.source_record_order,
             array_agg(action_type) FILTER (WHERE entity_type = 'EDIFACT') AS edifact_actions,
             count(journal_records_backup.source_id) FILTER (WHERE entity_type = 'EDIFACT' AND journal_records_backup.error != '') AS edifact_errors_number,
             array_agg(error) FILTER (WHERE entity_type = 'EDIFACT') AS source_record_error,
             journal_records_backup.tenant_id AS source_record_tenant_id,

             array_agg(action_type) FILTER (WHERE entity_type = 'INVOICE' AND journal_records_backup.title = 'INVOICE') AS invoice_actions,
             count(journal_records_backup.source_id) FILTER (WHERE entity_type = 'INVOICE' AND journal_records_backup.title = 'INVOICE' AND journal_records_backup.error != '') AS invoice_errors_number,
             array_agg(entity_hrid) FILTER (WHERE entity_type = 'INVOICE' AND journal_records_backup.title = 'INVOICE') AS invoice_entity_hrid,
             array_agg(entity_id) FILTER (WHERE entity_type = 'INVOICE' AND journal_records_backup.title = 'INVOICE') AS invoice_entity_id,
             array_agg(error) FILTER (WHERE entity_type = 'INVOICE' AND journal_records_backup.title = 'INVOICE') AS invoice_entity_error
      FROM journal_records_backup
      WHERE journal_records_backup.source_id = invoice_line_info.source_id AND (entity_type = 'EDIFACT' OR journal_records_backup.title = 'INVOICE')
      GROUP BY journal_records_backup.source_id, journal_records_backup.job_execution_id,journal_records_backup.source_record_order, journal_records_backup.tenant_id
      ) AS records_actions ON TRUE;
END;
$$ LANGUAGE plpgsql;


