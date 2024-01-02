-- Script to create function to determine processed entities status based on performed actions and occurred errors number.
CREATE OR REPLACE FUNCTION get_entity_status(actions text[], errorsNumber bigint) RETURNS text AS $$
DECLARE status text;
BEGIN
    SELECT
        CASE WHEN errorsNumber != 0 THEN 'DISCARDED'
             WHEN array_length(actions, 1) > 0 THEN
                CASE actions[1]
                    WHEN 'CREATE' THEN 'CREATED'
                    WHEN 'UPDATE' THEN 'UPDATED'
                    WHEN 'NON_MATCH' THEN 'DISCARDED'
                END
        END
    INTO status;

    RETURN status;
END;
$$ LANGUAGE plpgsql;

-- Request to delete the old version of get_job_log_entries with a different return format.
DROP FUNCTION IF EXISTS get_job_log_entries(uuid,text,text,bigint,bigint);

-- Request to delete get_job_log_entries with a different return format.
DROP FUNCTION IF EXISTS get_job_log_entries(uuid,text,text,bigint,bigint,boolean,text);

-- Script to create function to get data import job log entries (jobLogEntry).
CREATE OR REPLACE FUNCTION get_job_log_entries(jobExecutionId uuid, sortingField text, sortingDir text, limitVal bigint, offsetVal bigint, errorsOnly boolean, entityType text)
    RETURNS TABLE(job_execution_id uuid, incoming_record_id uuid, source_id uuid, source_record_order integer, invoiceline_number text, title text,
                  source_record_action_status text, source_entity_error text, source_record_tenant_id text,instance_action_status text, instance_entity_id text, instance_entity_hrid text, instance_entity_error text,
                  instance_entity_tenant_id text, holdings_action_status text, holdings_entity_hrid text, holdings_entity_id text, holdings_permanent_location_id text,
                  holdings_entity_error text, item_action_status text, item_entity_hrid text, item_entity_id text, item_entity_error text, authority_action_status text,
                  authority_entity_id text, authority_entity_error text, po_line_action_status text, po_line_entity_id text, po_line_entity_hrid text, po_line_entity_error text,
                  order_entity_id text, invoice_action_status text, invoice_entity_id text[], invoice_entity_hrid text[], invoice_entity_error text, invoice_line_action_status text,
                  invoice_line_entity_id text, invoice_line_entity_hrid text, invoice_line_entity_error text, total_count bigint,
                  invoice_line_journal_record_id uuid, source_record_entity_type text, source_record_order_array integer[])
AS $$

DECLARE
    v_sortingField text DEFAULT sortingfield;
    v_entityAttribute text[] DEFAULT ARRAY[upper(entityType)];
BEGIN
-- Using the source_record_order column in the array type provides support for sorting invoices and marc records.
    IF sortingField = 'source_record_order' THEN
        v_sortingField := 'source_record_order_array';
    END IF;

    IF entityType = 'MARC' THEN
        v_entityAttribute := ARRAY['MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY'];
    END IF;

    RETURN QUERY EXECUTE format('
WITH records_actions AS
       (SELECT journal_records.source_id,
               journal_records.source_record_order,
               journal_records.job_execution_id,
               array_agg(action_type
               ORDER BY array_position(array[''MATCH'', ''NON_MATCH'', ''MODIFY'', ''UPDATE'', ''CREATE''], action_type)) FILTER (
                 WHERE entity_type IN (''MARC_BIBLIOGRAPHIC'', ''MARC_HOLDINGS'', ''MARC_AUTHORITY'')) AS marc_actions,
               count(journal_records.source_id) FILTER (
                 WHERE (entity_type = ''MARC_BIBLIOGRAPHIC''
                   OR entity_type = ''MARC_HOLDINGS''
                   OR entity_type = ''MARC_AUTHORITY'')
                   AND journal_records.error != '''') AS marc_errors_number,
               array_agg(action_type
               ORDER BY array_position(array[''CREATE'', ''MODIFY'', ''UPDATE'', ''NON_MATCH'', ''MATCH''], action_type)) FILTER (
                 WHERE entity_type = ''INSTANCE''
                   AND (entity_id IS NOT NULL
                     OR action_type = ''NON_MATCH'')) AS instance_actions,
               count(journal_records.source_id) FILTER (
                 WHERE entity_type = ''INSTANCE''
                   AND journal_records.error != '''') AS instance_errors_number,
               array_agg(action_type
               ORDER BY array_position(array[''CREATE'', ''MODIFY'', ''UPDATE'', ''NON_MATCH''], action_type)) FILTER (
                 WHERE entity_type = ''HOLDINGS'') AS holdings_actions,
               count(journal_records.source_id) FILTER (
                 WHERE entity_type = ''HOLDINGS''
                   AND journal_records.error != '''') AS holdings_errors_number,
               array_agg(action_type
               ORDER BY array_position(array[''CREATE'', ''MODIFY'', ''UPDATE'', ''NON_MATCH''], action_type)) FILTER (
                 WHERE entity_type = ''ITEM'') AS item_actions,
               count(journal_records.source_id) FILTER (
                 WHERE entity_type = ''ITEM''
                   AND journal_records.error != '''') AS item_errors_number,
               array_agg(action_type
               ORDER BY array_position(array[''CREATE'', ''MODIFY'', ''UPDATE'', ''NON_MATCH''], action_type)) FILTER (
                 WHERE entity_type = ''AUTHORITY'') AS authority_actions,
               count(journal_records.source_id) FILTER (
                 WHERE entity_type = ''AUTHORITY''
                   AND journal_records.error != '''') AS authority_errors_number,
               array_agg(action_type
               ORDER BY array_position(array[''CREATE'', ''MODIFY'', ''UPDATE'', ''NON_MATCH''], action_type)) FILTER (
                 WHERE entity_type = ''PO_LINE'') AS po_line_actions,
               count(journal_records.source_id) FILTER (
                 WHERE entity_type = ''PO_LINE''
                   AND journal_records.error != '''') AS po_line_errors_number,
               count(journal_records.source_id) OVER () AS total_count,
               (array_agg(journal_records.entity_type) FILTER (
                 WHERE entity_type IN (''MARC_BIBLIOGRAPHIC'', ''MARC_HOLDINGS'', ''MARC_AUTHORITY'')))[1] AS source_record_entity_type,
               array_agg(journal_records.entity_hrid) FILTER (
                 WHERE entity_hrid !=''''
                   AND entity_type = ''HOLDINGS'') AS holdings_entity_hrid
        FROM journal_records
        WHERE journal_records.job_execution_id = ''%1$s''
          AND entity_type in (''MARC_BIBLIOGRAPHIC'',
                              ''MARC_HOLDINGS'',
                              ''MARC_AUTHORITY'',
                              ''INSTANCE'',
                              ''HOLDINGS'',
                              ''ITEM'',
                              ''AUTHORITY'',
                              ''PO_LINE'')
        GROUP BY journal_records.source_id,
                 journal_records.source_record_order,
                 journal_records.job_execution_id
        HAVING count(journal_records.source_id) FILTER (
          WHERE (%3$L = ''ALL''
                  OR entity_type = ANY(%4$L))
            AND (NOT %2$L
                                                          OR journal_records.error <> '''')) > 0),
     temp_result AS
       (SELECT id,
               job_execution_id,
               source_id,
               entity_type,
               entity_id,
               entity_hrid,
               CASE
                 WHEN error_max != ''''
                   OR action_type = ''NON_MATCH'' THEN ''DISCARDED''
                 WHEN action_type = ''CREATE'' THEN ''CREATED''
                 WHEN action_type IN (''UPDATE'',
                                      ''MODIFY'') THEN ''UPDATED''
                 END AS action_type,
               action_status,
               action_date,
               source_record_order,
               error,
               title,
               tenant_id,
               instance_id,
               holdings_id,
               order_id,
               permanent_location_id
        FROM journal_records
               INNER JOIN
             (SELECT entity_type AS entity_type_max,
                     entity_id AS entity_id_max,
                     action_status AS action_status_max,
                     max(error) AS error_max,
                     (array_agg(id
                                ORDER BY array_position(array[''CREATE'', ''UPDATE'', ''MODIFY'', ''NON_MATCH''], action_type)))[1] AS id_max
              FROM journal_records
              WHERE job_execution_id = ''%1$s''
                AND entity_type NOT IN (''EDIFACT'',
                                        ''INVOICE'')
              GROUP BY entity_type,
                       entity_id,
                       action_status) AS action_type_by_source ON journal_records.id = action_type_by_source.id_max),
     instances AS
       (SELECT action_type,
               entity_id,
               source_id,
               entity_hrid,
               error,
               job_execution_id,
               title,
               source_record_order,
               tenant_id
        FROM temp_result
        WHERE entity_type = ''INSTANCE'' ),
     holdings AS
       (SELECT action_type,
               entity_id,
               entity_hrid,
               error,
               instance_id,
               permanent_location_id,
               temp_result.job_execution_id,
               temp_result.source_id,
               temp_result.title,
               temp_result.source_record_order
        FROM temp_result
        WHERE entity_type = ''HOLDINGS'' ),
     items AS
       (SELECT action_type,
               entity_id,
               holdings_id,
               entity_hrid,
               error,
               instance_id,
               temp_result.job_execution_id,
               temp_result.source_id,
               temp_result.title,
               temp_result.source_record_order
        FROM temp_result
        WHERE entity_type = ''ITEM'' ),
     po_lines AS
       (SELECT action_type,
               entity_id,
               entity_hrid,
               temp_result.source_id,
               error,
               order_id,
               temp_result.job_execution_id,
               temp_result.title,
               temp_result.source_record_order
        FROM temp_result
        WHERE entity_type = ''PO_LINE'' ),
     authorities AS
       (SELECT action_type,
               entity_id,
               temp_result.source_id,
               error,
               temp_result.job_execution_id,
               temp_result.title,
               temp_result.source_record_order
        FROM temp_result
        WHERE entity_type = ''AUTHORITY'' )
SELECT records_actions.job_execution_id AS job_execution_id,
       records_actions.source_id AS source_id,
       records_actions.source_id AS incoming_record_id,
       records_actions.source_record_order AS source_record_order,
       '''' AS invoiceline_number,
       rec_titles.title,
       CASE
         WHEN marc_errors_number != 0
           OR marc_actions[array_length(marc_actions, 1)] = ''NON_MATCH'' THEN ''DISCARDED''
         WHEN marc_actions[array_length(marc_actions, 1)] = ''CREATE'' THEN ''CREATED''
         WHEN marc_actions[array_length(marc_actions, 1)] IN (''UPDATE'',
                                                              ''MODIFY'') THEN ''UPDATED''
         END AS source_record_action_status,
       NULL AS source_entity_error,
       NULL AS source_record_tenant_id,
       get_entity_status(instance_actions, instance_errors_number) AS instance_action_status,
       instance_info.instance_entity_id AS instance_entity_id,
       instance_info.instance_entity_hrid AS instance_entity_hrid,
       instance_info.instance_entity_error AS instance_entity_error,
       instance_info.instance_entity_tenant_id AS instance_entity_tenant_id,
       get_entity_status(holdings_actions, holdings_errors_number) AS holdings_action_status,
       holdings_info.holdings_entity_id AS holdings_entity_id,
       holdings_info.holdings_entity_hrid AS holdings_entity_hrid,
       holdings_info.holdings_permanent_location_id AS holdings_permanent_location_id,
       holdings_info.holdings_entity_error AS holdings_entity_error,
       get_entity_status(item_actions, item_errors_number) AS item_action_status,
       items_info.items_entity_id AS item_entity_id,
       items_info.items_entity_hrid AS item_entity_hrid,
       items_info.items_entity_error AS item_entity_error,
       get_entity_status(authority_actions, authority_errors_number) AS authority_action_status,
       authority_info.authority_entity_id AS authority_entity_id,
       authority_info.authority_entity_error AS authority_entity_error,
       get_entity_status(po_line_actions, po_line_errors_number) AS po_line_action_status,
       po_lines_info.po_lines_entity_id AS po_lines_entity_id,
       po_lines_info.po_lines_entity_hrid AS po_lines_entity_hrid,
       po_lines_info.po_lines_entity_error AS po_lines_entity_error,
       NULL AS order_entity_id,
       NULL AS invoice_action_status,
       NULL::text[] AS invoice_entity_id,
       NULL::text[] AS invoice_entity_hrid,
       NULL AS invoice_entity_error,
       NULL AS invoice_line_action_status,
       NULL AS invoice_line_entity_id,
       NULL AS invoice_line_entity_hrid,
       NULL AS invoice_line_entity_error,
       records_actions.total_count,
       NULL::UUID AS invoice_line_journal_record_id,
       records_actions.source_record_entity_type, ARRAY[records_actions.source_record_order] AS source_record_order_array
FROM
  (SELECT journal_records.source_id,
          journal_records.source_record_order,
          journal_records.job_execution_id,
          array_agg(action_type
          ORDER BY array_position(array[''MATCH'', ''NON_MATCH'', ''MODIFY'', ''UPDATE'', ''CREATE''], action_type)) FILTER (
            WHERE entity_type IN (''MARC_BIBLIOGRAPHIC'', ''MARC_HOLDINGS'', ''MARC_AUTHORITY'')) AS marc_actions,
          count(journal_records.source_id) FILTER (
            WHERE (entity_type = ''MARC_BIBLIOGRAPHIC''
              OR entity_type = ''MARC_HOLDINGS''
              OR entity_type = ''MARC_AUTHORITY'')
              AND journal_records.error != '''') AS marc_errors_number,
          array_agg(action_type
          ORDER BY array_position(array[''CREATE'', ''MODIFY'', ''UPDATE'', ''NON_MATCH'', ''MATCH''], action_type)) FILTER (
            WHERE entity_type = ''INSTANCE''
              AND (entity_id IS NOT NULL
                OR action_type = ''NON_MATCH'')) AS instance_actions,
          count(journal_records.source_id) FILTER (
            WHERE entity_type = ''INSTANCE''
              AND journal_records.error != '''') AS instance_errors_number,
          array_agg(action_type
          ORDER BY array_position(array[''CREATE'', ''MODIFY'', ''UPDATE'', ''NON_MATCH''], action_type)) FILTER (
            WHERE entity_type = ''HOLDINGS'') AS holdings_actions,
          count(journal_records.source_id) FILTER (
            WHERE entity_type = ''HOLDINGS''
              AND journal_records.error != '''') AS holdings_errors_number,
          array_agg(action_type
          ORDER BY array_position(array[''CREATE'', ''MODIFY'', ''UPDATE'', ''NON_MATCH''], action_type)) FILTER (
            WHERE entity_type = ''ITEM'') AS item_actions,
          count(journal_records.source_id) FILTER (
            WHERE entity_type = ''ITEM''
              AND journal_records.error != '''') AS item_errors_number,
          array_agg(action_type
          ORDER BY array_position(array[''CREATE'', ''MODIFY'', ''UPDATE'', ''NON_MATCH''], action_type)) FILTER (
            WHERE entity_type = ''AUTHORITY'') AS authority_actions,
          count(journal_records.source_id) FILTER (
            WHERE entity_type = ''AUTHORITY''
              AND journal_records.error != '''') AS authority_errors_number,
          array_agg(action_type
          ORDER BY array_position(array[''CREATE'', ''MODIFY'', ''UPDATE'', ''NON_MATCH''], action_type)) FILTER (
            WHERE entity_type = ''PO_LINE'') AS po_line_actions,
          count(journal_records.source_id) FILTER (
            WHERE entity_type = ''PO_LINE''
              AND journal_records.error != '''') AS po_line_errors_number,
          count(journal_records.source_id) OVER () AS total_count,
          (array_agg(journal_records.entity_type) FILTER (
            WHERE entity_type IN (''MARC_BIBLIOGRAPHIC'', ''MARC_HOLDINGS'', ''MARC_AUTHORITY'')))[1] AS source_record_entity_type,
          array_agg(journal_records.entity_hrid) FILTER (
            WHERE entity_hrid !=''''
              AND entity_type = ''HOLDINGS'') AS holdings_entity_hrid
   FROM journal_records
   WHERE journal_records.job_execution_id = ''%1$s''
     AND entity_type in (''MARC_BIBLIOGRAPHIC'',
                         ''MARC_HOLDINGS'',
                         ''MARC_AUTHORITY'',
                         ''INSTANCE'',
                         ''HOLDINGS'',
                         ''ITEM'',
                         ''AUTHORITY'',
                         ''PO_LINE'')
   GROUP BY journal_records.source_id,
            journal_records.source_record_order,
            journal_records.job_execution_id
   HAVING count(journal_records.source_id) FILTER (
     WHERE (%3$L = ''ALL''
             OR entity_type = ANY(%4$L))
       AND (NOT %2$L
                                                          OR journal_records.error <> '''')) > 0) AS records_actions
    LEFT JOIN
  (SELECT instances.source_id AS source_id,
          instances.entity_id AS instance_entity_id,
          instances.entity_hrid AS instance_entity_hrid,
          instances.error AS instance_entity_error,
          instances.tenant_id AS instance_entity_tenant_id
   FROM
     (SELECT temp_result.job_execution_id,
             entity_id,
             title,
             source_record_order,
             action_type,
             error,
             source_id,
             tenant_id
      FROM temp_result
      WHERE entity_type IN (''MARC_BIBLIOGRAPHIC'',
                            ''MARC_HOLDINGS'',
                            ''MARC_AUTHORITY'') ) AS marc
       LEFT JOIN instances ON marc.source_id = instances.source_id) AS instance_info ON instance_info.source_id = records_actions.source_id
    LEFT JOIN
  (SELECT holdings.source_id AS source_id,
          holdings.entity_id AS holdings_entity_id,
          holdings.entity_hrid AS holdings_entity_hrid,
          holdings.permanent_location_id AS holdings_permanent_location_id,
          holdings.error AS holdings_entity_error
   FROM
     (SELECT temp_result.job_execution_id,
             entity_id,
             title,
             source_record_order,
             action_type,
             error,
             source_id,
             tenant_id
      FROM temp_result
      WHERE entity_type IN (''MARC_BIBLIOGRAPHIC'',
                            ''MARC_HOLDINGS'',
                            ''MARC_AUTHORITY'') ) AS marc
       LEFT JOIN holdings ON marc.source_id = holdings.source_id) AS holdings_info ON holdings_info.source_id = records_actions.source_id
    LEFT JOIN
  (SELECT items.source_id AS source_id,
          items.entity_id AS items_entity_id,
          items.entity_hrid AS items_entity_hrid,
          items.error AS items_entity_error
   FROM
     (SELECT temp_result.job_execution_id,
             entity_id,
             title,
             source_record_order,
             action_type,
             error,
             source_id,
             tenant_id
      FROM temp_result
      WHERE entity_type IN (''MARC_BIBLIOGRAPHIC'',
                            ''MARC_HOLDINGS'',
                            ''MARC_AUTHORITY'') ) AS marc
       LEFT JOIN items ON marc.source_id = items.source_id) AS items_info ON items_info.source_id = records_actions.source_id
    LEFT JOIN
  (SELECT po_lines.source_id AS source_id,
          po_lines.entity_id AS po_lines_entity_id,
          po_lines.entity_hrid AS po_lines_entity_hrid,
          po_lines.error AS po_lines_entity_error
   FROM
     (SELECT temp_result.job_execution_id,
             entity_id,
             title,
             source_record_order,
             action_type,
             error,
             source_id,
             tenant_id
      FROM temp_result
      WHERE entity_type IN (''MARC_BIBLIOGRAPHIC'',
                            ''MARC_HOLDINGS'',
                            ''MARC_AUTHORITY'') ) AS marc
       LEFT JOIN po_lines ON marc.source_id = po_lines.source_id) AS po_lines_info ON po_lines_info.source_id = records_actions.source_id
    LEFT JOIN
  (SELECT authorities.source_id AS source_id,
          authorities.entity_id AS authority_entity_id,
          authorities.error AS authority_entity_error
   FROM
     (SELECT temp_result.job_execution_id,
             entity_id,
             title,
             source_record_order,
             action_type,
             error,
             source_id,
             tenant_id
      FROM temp_result
      WHERE entity_type IN (''MARC_BIBLIOGRAPHIC'',
                            ''MARC_HOLDINGS'',
                            ''MARC_AUTHORITY'') ) AS marc
       LEFT JOIN authorities ON marc.source_id = authorities.source_id) AS authority_info ON authority_info.source_id = records_actions.source_id
    LEFT JOIN
  (SELECT journal_records.source_id,
          CASE
            WHEN COUNT(*) = 1 THEN array_to_string(array_agg(journal_records.error), '', '')
            ELSE ''['' || array_to_string(array_agg(journal_records.error), '', '') || '']''
            END AS error
   FROM journal_records
   WHERE journal_records.job_execution_id = ''%1$s''
     AND journal_records.error != ''''
   GROUP BY journal_records.source_id) AS rec_errors ON rec_errors.source_id = records_actions.source_id
    LEFT JOIN
  (SELECT journal_records.source_id,
          journal_records.title
   FROM journal_records
   WHERE journal_records.job_execution_id = ''%1$s'') AS rec_titles ON rec_titles.source_id = records_actions.source_id
    AND rec_titles.title IS NOT NULL
UNION
SELECT records_actions.job_execution_id AS job_execution_id,
       records_actions.source_id AS source_id,
       records_actions.source_id AS incoming_record_id,
       records_actions.source_record_order AS source_record_order,
       entity_hrid AS invoiceline_number,
       records_actions.title,
       CASE
         WHEN marc_errors_number != 0
           OR marc_actions[array_length(marc_actions, 1)] = ''NON_MATCH'' THEN ''DISCARDED''
         WHEN marc_actions[array_length(marc_actions, 1)] = ''CREATE'' THEN ''CREATED''
         WHEN marc_actions[array_length(marc_actions, 1)] IN (''UPDATE'',
                                                              ''MODIFY'') THEN ''UPDATED''
         END AS source_record_action_status,
       NULL AS source_entity_error,
       NULL AS source_record_tenant_id,
       NULL AS instance_action_status,
       NULL AS instance_entity_id,
       NULL AS instance_entity_hrid,
       NULL AS instance_entity_error,
       NULL AS instance_entity_tenant_id,
       NULL AS holdings_action_status,
       NULL AS holdings_entity_hrid,
       NULL AS holdings_entity_id,
       NULL AS holdings_permanent_location_id,
       NULL AS holdings_entity_error,
       NULL AS item_action_status,
       NULL AS item_entity_hrid,
       NULL AS item_entity_id,
       NULL AS item_entity_error,
       NULL AS authority_action_status,
       NULL AS authority_entity_id,
       NULL AS authority_entity_error,
       NULL AS po_line_action_status,
       NULL AS po_line_entity_id,
       NULL AS po_line_entity_hrid,
       NULL AS po_line_entity_error,
       NULL AS order_entity_id,
       get_entity_status(invoice_actions, invoice_errors_number) AS invoice_action_status,
       invoice.invoice_entity_id,
       invoice.invoice_entity_hrid,
       invoice.invoice_entity_error,
       invoice.invoice_line_action_status,
       invoice.invoice_line_entity_id,
       invoice.invoice_line_entity_hrid,
       invoice.invoice_line_entity_error,
       records_actions.total_count,
       invoiceLineJournalRecordId AS invoice_line_journal_record_id,
       records_actions.source_record_entity_type,
       CASE
         WHEN get_entity_status(invoice_actions, invoice_errors_number) IS NOT NULL THEN string_to_array(entity_hrid, ''-'')::int[]
         ELSE ARRAY[records_actions.source_record_order]
         END AS source_record_order_array
FROM
  (SELECT journal_records.source_id,
          journal_records.job_execution_id,
          source_record_order,
          entity_hrid,
          title,
          error, array[]::varchar[] AS marc_actions,
          cast(0 AS integer) AS marc_errors_number,
          array_agg(action_type) FILTER (
            WHERE entity_type = ''INVOICE'') AS invoice_actions,
          count(journal_records.source_id) FILTER (
            WHERE entity_type = ''INVOICE''
              AND journal_records.error != '''') AS invoice_errors_number,
          count(journal_records.source_id) OVER () AS total_count,
          id AS invoiceLineJournalRecordId,
          (array_agg(entity_type) FILTER (
            WHERE entity_type IN (''EDIFACT'')))[1] AS source_record_entity_type, array[]::varchar[] AS holdings_entity_hrid
   FROM journal_records
   WHERE journal_records.job_execution_id = ''%1$s''
     AND entity_type = ''INVOICE''
     AND title != ''INVOICE''
   GROUP BY journal_records.source_id,
            journal_records.source_record_order,
            journal_records.job_execution_id,
            entity_hrid,
            title,
            error,
            id
   HAVING count(journal_records.source_id) FILTER (
     WHERE (%3$L IN (''ALL'', ''INVOICE''))
       AND (NOT %2$L
                                                          OR journal_records.error <> '''')) > 0) AS records_actions
    LEFT JOIN LATERAL
    (SELECT invoice_line_info.job_execution_id,
            invoice_line_info.source_id,
            records_actions.source_record_order,
            invoice_line_info.title,
            CASE
              WHEN edifact_errors_number != 0 THEN ''DISCARDED''
              WHEN edifact_actions[array_length(edifact_actions, 1)] = ''CREATE'' THEN ''CREATED''
              END AS source_record_action_status,
            records_actions.source_record_error[1], records_actions.source_record_tenant_id,
            NULL AS instance_action_status,
            NULL AS instance_entity_id,
            NULL AS instance_entity_hrid,
            NULL AS instance_entity_error,
            NULL AS instance_entity_tenant_id,
            NULL AS holdings_action_status,
            NULL AS holdings_entity_hrid,
            NULL AS holdings_entity_id,
            NULL AS holdings_permanent_location_id,
            NULL AS holdings_entity_error,
            NULL AS item_action_status,
            NULL AS item_entity_hrid,
            NULL AS item_entity_id,
            NULL AS item_entity_error,
            NULL AS authority_action_status,
            NULL AS authority_entity_id,
            NULL AS authority_entity_error,
            NULL AS po_line_action_status,
            NULL AS po_line_entity_id,
            NULL AS po_line_entity_hrid,
            NULL AS po_line_entity_error,
            NULL AS order_entity_id,
            get_entity_status(records_actions.invoice_actions, records_actions.invoice_errors_number) AS invoice_action_status,
            records_actions.invoice_entity_id,
            records_actions.invoice_entity_hrid,
            records_actions.invoice_entity_error[1], CASE
                                                       WHEN action_status = ''ERROR'' THEN ''DISCARDED''
                                                       WHEN inv_line_actions = ''CREATE'' THEN ''CREATED''
              END AS invoice_line_action_status,
            invoice_line_info.invoice_line_entity_id,
            invoice_line_info.invoice_line_entity_hrid,
            invoice_line_info.invoice_line_entity_error
     FROM
       (SELECT journal_records.source_id,
               journal_records.job_execution_id,
               journal_records.title,
               journal_records.action_type AS inv_line_actions,
               action_status,
               entity_hrid AS invoice_line_entity_hrid,
               entity_id AS invoice_line_entity_id,
               error AS invoice_line_entity_error
        FROM journal_records
        WHERE journal_records.job_execution_id = ''%1$s''
          AND journal_records.entity_type = ''INVOICE''
          AND journal_records.title != ''INVOICE'' ) AS invoice_line_info
         LEFT JOIN LATERAL
         (SELECT journal_records.source_id,
                 journal_records.source_record_order,
                 array_agg(action_type) FILTER (
                   WHERE entity_type = ''EDIFACT'') AS edifact_actions,
                 count(journal_records.source_id) FILTER (
                   WHERE entity_type = ''EDIFACT''
                     AND journal_records.error != '''') AS edifact_errors_number,
                 array_agg(error) FILTER (
                   WHERE entity_type = ''EDIFACT'') AS source_record_error,
                 journal_records.tenant_id AS source_record_tenant_id,
                 array_agg(action_type) FILTER (
                   WHERE entity_type = ''INVOICE''
                     AND journal_records.title = ''INVOICE'') AS invoice_actions,
                 count(journal_records.source_id) FILTER (
                   WHERE entity_type = ''INVOICE''
                     AND journal_records.title = ''INVOICE''
                     AND journal_records.error != '''') AS invoice_errors_number,
                 array_agg(entity_hrid) FILTER (
                   WHERE entity_type = ''INVOICE''
                     AND journal_records.title = ''INVOICE'') AS invoice_entity_hrid,
                 array_agg(entity_id) FILTER (
                   WHERE entity_type = ''INVOICE''
                     AND journal_records.title = ''INVOICE'') AS invoice_entity_id,
                 array_agg(error) FILTER (
                   WHERE entity_type = ''INVOICE''
                     AND journal_records.title = ''INVOICE'') AS invoice_entity_error
          FROM journal_records
          WHERE journal_records.source_id = invoice_line_info.source_id
            AND (entity_type = ''EDIFACT''
            OR journal_records.title = ''INVOICE'')
          GROUP BY journal_records.source_id,
                   journal_records.job_execution_id,
                   journal_records.source_record_order,
                   journal_records.tenant_id) AS records_actions ON TRUE) AS invoice ON TRUE
ORDER BY %5$I %6$s
LIMIT %7$s OFFSET %8$s;',
                              jobExecutionId, errorsOnly, entityType, v_entityAttribute, v_sortingField, sortingDir, limitVal, offsetVal);
END;
$$ LANGUAGE plpgsql;
