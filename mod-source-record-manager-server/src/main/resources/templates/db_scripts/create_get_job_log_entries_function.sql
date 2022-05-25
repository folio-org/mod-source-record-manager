-- Script to create function to determine processed entities status based on performed actions and occurred errors number.
CREATE OR REPLACE FUNCTION get_entity_status(actions text[], errorsNumber bigint) RETURNS text AS $$
DECLARE status text;
BEGIN
    SELECT
        CASE WHEN errorsNumber != 0 THEN 'DISCARDED'
             WHEN array_length(actions, 1) > 1 THEN 'MULTIPLE'
             WHEN array_length(actions, 1) = 1 THEN
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

-- Script to create function to get data import job log entries (jobLogEntry).
CREATE OR REPLACE FUNCTION get_job_log_entries(jobExecutionId uuid, sortingField text, sortingDir text, limitVal bigint, offsetVal bigint, errorsOnly boolean)
    RETURNS TABLE(job_execution_id uuid, source_id uuid, source_record_order integer, invoiceline_number text, title text,
                  source_record_action_status text, instance_action_status text, holdings_action_status text, item_action_status text,
                  authority_action_status text, order_action_status text, invoice_action_status text, error text, total_count bigint,
                  invoice_line_journal_record_id uuid, source_record_entity_type text, holdings_entity_hrid text[], source_record_order_array integer[])
AS $$
DECLARE
    v_sortingfield text := sortingfield;
BEGIN
-- Using the source_record_order column in the array type provides support for sorting invoices and marc records.
    IF sortingfield = 'source_record_order' THEN
      v_sortingfield := 'source_record_order_array';
    END IF;

    RETURN QUERY EXECUTE format('
SELECT records_actions.job_execution_id, records_actions.source_id, records_actions.source_record_order, '''' as invoiceline_number,
       rec_titles.title,
       CASE
           WHEN marc_errors_number != 0 OR marc_actions[array_length(marc_actions, 1)] = ''NON_MATCH'' THEN ''DISCARDED''
           WHEN marc_actions[array_length(marc_actions, 1)] = ''CREATE'' THEN ''CREATED''
           WHEN marc_actions[array_length(marc_actions, 1)] IN (''UPDATE'', ''MODIFY'') THEN ''UPDATED''
       END AS source_record_action_status,
       get_entity_status(instance_actions, instance_errors_number) AS instance_action_status,
       get_entity_status(holdings_actions, holdings_errors_number) AS holdings_action_status,
       get_entity_status(item_actions, item_errors_number) AS item_action_status,
       get_entity_status(authority_actions, authority_errors_number) AS authority_action_status,
       get_entity_status(order_actions, order_errors_number) AS order_action_status,
       null AS invoice_action_status, rec_errors.error, records_actions.total_count,
       null AS invoiceLineJournalRecordId,
       records_actions.source_record_entity_type,
       records_actions.holdings_entity_hrid,
       ARRAY[records_actions.source_record_order] AS source_record_order_array
FROM (
         SELECT journal_records.source_id, journal_records.source_record_order, journal_records.job_execution_id,
                array_agg(action_type ORDER BY action_date ASC) FILTER (WHERE entity_type IN (''MARC_BIBLIOGRAPHIC'', ''MARC_HOLDINGS'', ''MARC_AUTHORITY'')) AS marc_actions,
                count(journal_records.source_id) FILTER (WHERE (entity_type = ''MARC_BIBLIOGRAPHIC'' OR entity_type = ''MARC_HOLDINGS'' OR entity_type = ''MARC_AUTHORITY'') AND journal_records.error != '''') AS marc_errors_number,
                array_agg(action_type) FILTER (WHERE entity_type = ''INSTANCE'') AS instance_actions,
                count(journal_records.source_id) FILTER (WHERE entity_type = ''INSTANCE'' AND journal_records.error != '''') AS instance_errors_number,
                array_agg(action_type) FILTER (WHERE entity_type = ''HOLDINGS'') AS holdings_actions,
                count(journal_records.source_id) FILTER (WHERE entity_type = ''HOLDINGS'' AND journal_records.error != '''') AS holdings_errors_number,
                array_agg(action_type) FILTER (WHERE entity_type = ''ITEM'') AS item_actions,
                count(journal_records.source_id) FILTER (WHERE entity_type = ''ITEM'' AND journal_records.error != '''') AS item_errors_number,
                array_agg(action_type) FILTER (WHERE entity_type = ''AUTHORITY'') AS authority_actions,
                count(journal_records.source_id) FILTER (WHERE entity_type = ''AUTHORITY'' AND journal_records.error != '''') AS authority_errors_number,
                array_agg(action_type) FILTER (WHERE entity_type = ''ORDER'') AS order_actions,
                count(journal_records.source_id) FILTER (WHERE entity_type = ''ORDER'' AND journal_records.error != '''') AS order_errors_number,
                count(journal_records.source_id) OVER () AS total_count,
                (array_agg(journal_records.entity_type) FILTER (WHERE entity_type IN (''MARC_BIBLIOGRAPHIC'', ''MARC_HOLDINGS'', ''MARC_AUTHORITY'')))[1] AS source_record_entity_type,
 				array_agg(journal_records.entity_hrid) FILTER (WHERE entity_hrid !='''' and  entity_type = ''HOLDINGS'') as holdings_entity_hrid
         FROM journal_records
         WHERE journal_records.job_execution_id = ''%1$s'' and
               entity_type in (''MARC_BIBLIOGRAPHIC'', ''MARC_HOLDINGS'', ''MARC_AUTHORITY'', ''INSTANCE'', ''HOLDINGS'', ''ITEM'', ''ORDER'', ''AUTHORITY'')
         GROUP BY journal_records.source_id, journal_records.source_record_order, journal_records.job_execution_id
     ) AS records_actions
         LEFT JOIN (SELECT journal_records.source_id, journal_records.error
                    FROM journal_records
                    WHERE journal_records.job_execution_id = ''%1$s'') AS rec_errors
            ON rec_errors.source_id = records_actions.source_id AND rec_errors.error != ''''
         LEFT JOIN (SELECT journal_records.source_id, journal_records.title
                    FROM journal_records
                    WHERE journal_records.job_execution_id = ''%1$s'') AS rec_titles
            ON rec_titles.source_id = records_actions.source_id AND rec_titles.title IS NOT NULL
WHERE NOT %6$L or rec_errors.error = '''' IS FALSE

UNION

SELECT records_actions.job_execution_id, records_actions.source_id, source_record_order, entity_hrid as invoiceline_number, title,
       CASE
           WHEN marc_errors_number != 0 OR marc_actions[array_length(marc_actions, 1)] = ''NON_MATCH'' THEN ''DISCARDED''
           WHEN marc_actions[array_length(marc_actions, 1)] = ''CREATE'' THEN ''CREATED''
           WHEN marc_actions[array_length(marc_actions, 1)] IN (''UPDATE'', ''MODIFY'') THEN ''UPDATED''
       END AS source_record_action_status,
       null AS instance_action_status,
       null AS holdings_action_status,
       null AS item_action_status,
       null AS authority_action_status,
       null AS order_action_status,
       get_entity_status(invoice_actions, invoice_errors_number) AS invoice_action_status,
       error,
       records_actions.total_count,
       invoiceLineJournalRecordId,
       records_actions.source_record_entity_type,
       records_actions.holdings_entity_hrid,
       CASE
           WHEN get_entity_status(invoice_actions, invoice_errors_number) IS NOT null THEN string_to_array(entity_hrid, ''-'')::int[]
           ELSE ARRAY[source_record_order]
       END AS source_record_order_array
FROM (
         SELECT journal_records.source_id, journal_records.job_execution_id, source_record_order, entity_hrid, title, error,
                array[]::varchar[] AS marc_actions,
                cast(0 as integer) AS marc_errors_number,
                array_agg(action_type) FILTER (WHERE entity_type = ''INVOICE'') AS invoice_actions,
                count(journal_records.source_id) FILTER (WHERE entity_type = ''INVOICE'' AND journal_records.error != '''') AS invoice_errors_number,
                count(journal_records.source_id) OVER () AS total_count,
                id AS invoiceLineJournalRecordId,
                (array_agg(entity_type) FILTER (WHERE entity_type IN (''EDIFACT'')))[1] AS source_record_entity_type,
                array[]::varchar[] as holdings_entity_hrid
         FROM journal_records
         WHERE journal_records.job_execution_id = ''%1$s'' and entity_type = ''INVOICE'' and title != ''INVOICE'' and (NOT %6$L or error = '''' IS FALSE)
         GROUP BY journal_records.source_id, journal_records.source_record_order, journal_records.job_execution_id,
                  entity_hrid, title, error, id
     ) AS records_actions
ORDER BY %2$I %3$s
LIMIT %4$s OFFSET %5$s;',
                              jobExecutionId, v_sortingField, sortingDir, limitVal, offsetVal, errorsOnly);
END;
$$ LANGUAGE plpgsql;
