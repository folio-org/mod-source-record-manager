-- Script to create function to get data import record processing log entries (recordProcessingLogDto).
CREATE OR REPLACE FUNCTION get_record_processing_log(jobExecutionId uuid, recordId uuid)
    RETURNS TABLE(job_execution_id uuid, source_id uuid, source_record_order integer, title text, source_record_action_status text, source_entity_id uuid, source_entity_hrid text, source_entity_error text, instance_action_status text, instance_entity_id uuid, instance_entity_hrid text, instance_entity_error text, holdings_action_status text, holdings_entity_id uuid, holdings_entity_hrid text, holdings_entity_error text, item_action_status text, item_entity_id uuid, item_entity_hrid text, item_entity_error text, order_action_status text, order_entity_id uuid, order_entity_hrid text, order_entity_error text, invoice_action_status text, invoice_entity_id uuid, invoice_entity_hrid text, invoice_entity_error text)
AS $$
BEGIN
    RETURN QUERY EXECUTE format('
SELECT records_actions.job_execution_id,
               records_actions.source_id,
               records_actions.source_record_order,
               rec_titles.title,
               CASE WHEN marc_errors_number != 0 OR marc_actions[array_length(marc_actions, 1)] = ''NON_MATCH''
                        THEN ''DISCARDED''
                    WHEN marc_actions[array_length(marc_actions, 1)] = ''CREATE''
                        THEN ''CREATED''
                    WHEN marc_actions[array_length(marc_actions, 1)] IN (''UPDATE'', ''MODIFY'')
                        THEN ''UPDATED''
               END AS source_record_action_status,
			   source_entity_id[1],
			   source_entity_hrid[1],
			   source_entity_error[1],
               get_entity_status(instance_actions, instance_errors_number) AS instance_action_status,
			   instance_entity_id[1],
			   instance_entity_hrid[1],
			   instance_entity_error[1],
               get_entity_status(holdings_actions, holdings_errors_number) AS holdings_action_status,
			   holdings_entity_id[1],
			   holdings_entity_hrid[1],
			   holdings_entity_error[1],
               get_entity_status(item_actions, item_errors_number)         AS item_action_status,
			   item_entity_id[1],
			   item_entity_hrid[1],
			   item_entity_error[1],
               get_entity_status(order_actions, order_errors_number)       AS order_action_status,
			   order_entity_id[1],
			   order_entity_hrid[1],
			   order_entity_error[1],
               get_entity_status(invoice_actions, invoice_errors_number)   AS invoice_action_status,
			   order_entity_id[1],
			   order_entity_hrid[1],
			   order_entity_error[1]
        FROM (
               SELECT journal_records.source_id,
                      journal_records.source_record_order,
                      journal_records.job_execution_id,
                      array_agg(action_type ORDER BY action_date ASC) FILTER (WHERE entity_type = ''MARC_BIBLIOGRAPHIC'') AS marc_actions,
                      count(journal_records.source_id) FILTER (WHERE entity_type = ''MARC_BIBLIOGRAPHIC'' AND journal_records.error != '''') AS marc_errors_number,

			          array_agg(entity_hrid) FILTER (WHERE entity_type = ''MARC_BIBLIOGRAPHIC'') AS source_entity_hrid,
			          array_agg(entity_id) FILTER (WHERE entity_type = ''MARC_BIBLIOGRAPHIC'') AS source_entity_id,
			          array_agg(error) FILTER (WHERE entity_type = ''MARC_BIBLIOGRAPHIC'') AS source_entity_error,

                      array_agg(action_type) FILTER (WHERE entity_type = ''INSTANCE'') AS instance_actions,
                      count(journal_records.source_id) FILTER (WHERE entity_type = ''INSTANCE'' AND journal_records.error != '''') AS instance_errors_number,

			          array_agg(entity_hrid) FILTER (WHERE entity_type = ''INSTANCE'') AS instance_entity_hrid,
			          array_agg(entity_id) FILTER (WHERE entity_type = ''INSTANCE'') AS instance_entity_id,
				      array_agg(error) FILTER (WHERE entity_type = ''INSTANCE'') AS instance_entity_error,


                      array_agg(action_type) FILTER (WHERE entity_type = ''HOLDINGS'') AS holdings_actions,
                      count(journal_records.source_id) FILTER (WHERE entity_type = ''HOLDINGS'' AND journal_records.error != '''') AS holdings_errors_number,

					  array_agg(entity_hrid) FILTER (WHERE entity_type = ''HOLDINGS'') AS holdings_entity_hrid,
			          array_agg(entity_id) FILTER (WHERE entity_type = ''HOLDINGS'') AS holdings_entity_id,
					  array_agg(entity_id) FILTER (WHERE entity_type = ''HOLDINGS'') AS holdings_entity_error,


                      array_agg(action_type) FILTER (WHERE entity_type = ''ITEM'') AS item_actions,
                      count(journal_records.source_id) FILTER (WHERE entity_type = ''ITEM'' AND journal_records.error != '''') AS item_errors_number,

					  array_agg(entity_hrid) FILTER (WHERE entity_type = ''ITEM'') AS item_entity_hrid,
			          array_agg(entity_id) FILTER (WHERE entity_type = ''ITEM'') AS item_entity_id,
					  array_agg(entity_id) FILTER (WHERE entity_type = ''ITEM'') AS item_entity_error,

                      array_agg(action_type) FILTER (WHERE entity_type = ''ORDER'') AS order_actions,
                      count(journal_records.source_id) FILTER (WHERE entity_type = ''ORDER'' AND journal_records.error != '''') AS order_errors_number,

					  array_agg(entity_hrid) FILTER (WHERE entity_type = ''ORDER'') AS order_entity_hrid,
			          array_agg(entity_id) FILTER (WHERE entity_type = ''ORDER'') AS order_entity_id,
					  array_agg(entity_id) FILTER (WHERE entity_type = ''ORDER'') AS order_entity_error,

                      array_agg(action_type) FILTER (WHERE entity_type = ''INVOICE'') AS invoice_actions,
                      count(journal_records.source_id) FILTER (WHERE entity_type = ''INVOICE'' AND journal_records.error != '''') AS invoice_errors_number,

					  array_agg(entity_hrid) FILTER (WHERE entity_type = ''INVOICE'') AS invoice_entity_hrid,
			          array_agg(entity_id) FILTER (WHERE entity_type = ''INVOICE'') AS invoice_entity_id,
					  array_agg(entity_id) FILTER (WHERE entity_type = ''INVOICE'') AS invoice_entity_error

               FROM journal_records
               WHERE journal_records.job_execution_id = ''%s'' AND journal_records.source_id = ''%s''
               GROUP BY journal_records.source_id, journal_records.source_record_order, journal_records.job_execution_id)
			   AS records_actions
                LEFT JOIN (
                    SELECT journal_records.source_id, journal_records.title FROM journal_records
                    WHERE journal_records.job_execution_id = ''%s'' AND journal_records.source_id = ''%s''
                  ) AS rec_titles ON rec_titles.source_id = records_actions.source_id AND rec_titles.title IS NOT NULL',
        jobExecutionId, recordId);
END;
$$ LANGUAGE plpgsql;
