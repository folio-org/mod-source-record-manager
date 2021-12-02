-- Script to create function to get data import record processing log entries (recordProcessingLogDto).
DROP FUNCTION IF EXISTS get_record_processing_log(uuid, uuid);

CREATE OR REPLACE FUNCTION get_record_processing_log(jobExecutionId uuid, recordId uuid)
    RETURNS TABLE(job_execution_id uuid, source_id uuid, source_record_order integer, title text, source_record_action_status text, source_entity_error text, instance_action_status text, instance_entity_id text[], instance_entity_hrid text[], instance_entity_error text, holdings_action_status text, holdings_entity_id text[], holdings_entity_hrid text[], holdings_entity_error text, item_action_status text, item_entity_id text[], item_entity_hrid text[], item_entity_error text, authority_action_status text, authority_entity_id text[], authority_entity_error text, order_action_status text, order_entity_id text[], order_entity_hrid text[], order_entity_error text, invoice_action_status text, invoice_entity_id text[], invoice_entity_hrid text[], invoice_entity_error text, invoice_line_action_status text, invoice_line_entity_id text, invoice_line_entity_hrid text, invoice_line_entity_error text)
AS $$
BEGIN
    RETURN QUERY
        SELECT records_actions.job_execution_id,
               records_actions.source_id,
               records_actions.source_record_order,
               rec_titles.title,
               CASE WHEN marc_errors_number != 0 OR marc_actions[array_length(marc_actions, 1)] = 'NON_MATCH'
                        THEN 'DISCARDED'
                    WHEN marc_actions[array_length(marc_actions, 1)] = 'CREATE'
                        THEN 'CREATED'
                    WHEN marc_actions[array_length(marc_actions, 1)] IN ('UPDATE', 'MODIFY')
                        THEN 'UPDATED'
               END AS source_record_action_status,
			   records_actions.source_entity_error[1],
               get_entity_status(instance_actions, instance_errors_number) AS instance_action_status,
			   records_actions.instance_entity_id,
			   records_actions.instance_entity_hrid,
			   records_actions.instance_entity_error[1],
               get_entity_status(holdings_actions, holdings_errors_number) AS holdings_action_status,
			   records_actions.holdings_entity_id,
			   records_actions.holdings_entity_hrid,
			   records_actions.holdings_entity_error[1],
               get_entity_status(item_actions, item_errors_number)         AS item_action_status,
			   records_actions.item_entity_id,
			   records_actions.item_entity_hrid,
			   records_actions.item_entity_error[1],
			         get_entity_status(authority_actions, authority_errors_number)         AS authority_action_status,
			   records_actions.authority_entity_id,
			   records_actions.authority_entity_error[1],
               get_entity_status(order_actions, order_errors_number)       AS order_action_status,
			   records_actions.order_entity_id,
			   records_actions.order_entity_hrid,
			   records_actions.order_entity_error[1],
               null AS invoice_action_status,
			   null AS invoice_entity_id,
			   null AS invoice_entity_hrid,
			   null AS invoice_entity_error,
               null AS invoice_line_action_status,
               null AS invoice_line_entity_id,
               null AS invoice_line_entity_hrid,
               null AS invoice_line_entity_error
        FROM (
               SELECT journal_records.source_id,
                      journal_records.source_record_order,
                      journal_records.job_execution_id,
                      array_agg(action_type ORDER BY action_date ASC) FILTER (WHERE entity_type = 'MARC_BIBLIOGRAPHIC' OR entity_type = 'MARC_HOLDINGS' OR entity_type = 'MARC_AUTHORITY') AS marc_actions,
                      count(journal_records.source_id) FILTER (WHERE (entity_type = 'MARC_BIBLIOGRAPHIC' OR entity_type = 'MARC_HOLDINGS' OR entity_type = 'MARC_AUTHORITY') AND journal_records.error != '') AS marc_errors_number,

			          array_agg(error) FILTER (WHERE entity_type = 'MARC_BIBLIOGRAPHIC' OR entity_type = 'MARC_HOLDINGS' OR entity_type = 'MARC_AUTHORITY') AS source_entity_error,

                      array_agg(action_type) FILTER (WHERE entity_type = 'INSTANCE') AS instance_actions,
                      count(journal_records.source_id) FILTER (WHERE entity_type = 'INSTANCE' AND journal_records.error != '') AS instance_errors_number,

			          array_agg(entity_hrid) FILTER (WHERE entity_type = 'INSTANCE') AS instance_entity_hrid,
			          array_agg(entity_id) FILTER (WHERE entity_type = 'INSTANCE') AS instance_entity_id,
				      array_agg(error) FILTER (WHERE entity_type = 'INSTANCE') AS instance_entity_error,


                      array_agg(action_type) FILTER (WHERE entity_type = 'HOLDINGS') AS holdings_actions,
                      count(journal_records.source_id) FILTER (WHERE entity_type = 'HOLDINGS' AND journal_records.error != '') AS holdings_errors_number,

					  array_agg(entity_hrid) FILTER (WHERE entity_type = 'HOLDINGS') AS holdings_entity_hrid,
			          array_agg(entity_id) FILTER (WHERE entity_type = 'HOLDINGS') AS holdings_entity_id,
					  array_agg(error) FILTER (WHERE entity_type = 'HOLDINGS') AS holdings_entity_error,


                      array_agg(action_type) FILTER (WHERE entity_type = 'ITEM') AS item_actions,
                      count(journal_records.source_id) FILTER (WHERE entity_type = 'ITEM' AND journal_records.error != '') AS item_errors_number,

					  array_agg(entity_hrid) FILTER (WHERE entity_type = 'ITEM') AS item_entity_hrid,
			          array_agg(entity_id) FILTER (WHERE entity_type = 'ITEM') AS item_entity_id,
					  array_agg(error) FILTER (WHERE entity_type = 'ITEM') AS item_entity_error,

					  array_agg(action_type) FILTER (WHERE entity_type = 'AUTHORITY') AS authority_actions,
                      count(journal_records.source_id) FILTER (WHERE entity_type = 'AUTHORITY' AND journal_records.error != '') AS authority_errors_number,

			          array_agg(entity_id) FILTER (WHERE entity_type = 'AUTHORITY') AS authority_entity_id,
					  array_agg(error) FILTER (WHERE entity_type = 'AUTHORITY') AS authority_entity_error,

                      array_agg(action_type) FILTER (WHERE entity_type = 'ORDER') AS order_actions,
                      count(journal_records.source_id) FILTER (WHERE entity_type = 'ORDER' AND journal_records.error != '') AS order_errors_number,

					  array_agg(entity_hrid) FILTER (WHERE entity_type = 'ORDER') AS order_entity_hrid,
			          array_agg(entity_id) FILTER (WHERE entity_type = 'ORDER') AS order_entity_id,
					  array_agg(error) FILTER (WHERE entity_type = 'ORDER') AS order_entity_error
               FROM journal_records
               WHERE journal_records.job_execution_id = jobExecutionId AND journal_records.source_id = recordId AND journal_records.entity_type NOT IN ('EDIFACT', 'INVOICE')
               GROUP BY journal_records.source_id, journal_records.source_record_order, journal_records.job_execution_id)
			   AS records_actions
        LEFT JOIN (
                    SELECT journal_records.source_id, journal_records.title FROM journal_records
                    WHERE journal_records.job_execution_id = jobExecutionId AND journal_records.source_id = recordId
                  ) AS rec_titles ON rec_titles.source_id = records_actions.source_id AND rec_titles.title IS NOT NULL
        UNION
            SELECT invoice_line_info.job_execution_id,
                   invoice_line_info.source_id,
                   records_actions.source_record_order,
                   invoice_line_info.title,
                   CASE WHEN edifact_errors_number != 0 THEN 'DISCARDED'
                        WHEN edifact_actions[array_length(edifact_actions, 1)] = 'CREATE' THEN 'CREATED'
                   END AS source_record_action_status,
                   records_actions.source_record_error[1],
                   null AS instance_action_status,
                   null AS instance_entity_id,
                   null AS instance_entity_hrid,
                   null AS instance_entity_error,
                   null AS holdings_action_status,
                   null AS holdings_entity_id,
                   null AS holdings_entity_hrid,
                   null AS holdings_entity_error,
                   null AS item_action_status,
                   null AS item_entity_id,
                   null AS item_entity_hrid,
                   null AS item_entity_error,
                   null AS authority_action_status,
                   null AS authority_entity_id,
                   null AS authority_entity_error,
                   null AS order_action_status,
                   null AS order_entity_id,
                   null AS order_entity_hrid,
                   null AS order_entity_error,
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
                   SELECT journal_records.source_id,
                          journal_records.job_execution_id,
                          journal_records.title,
                          journal_records.action_type AS inv_line_actions,
                          action_status,
                          entity_hrid AS invoice_line_entity_hrid,
                          entity_id AS invoice_line_entity_id,
                          error AS invoice_line_entity_error
                   FROM journal_records
                   WHERE journal_records.id = recordId AND journal_records.entity_type = 'INVOICE' AND journal_records.title != 'INVOICE'
            ) AS invoice_line_info
            LEFT JOIN (
                   SELECT journal_records.source_id,
                          journal_records.source_record_order,
                          array_agg(action_type) FILTER (WHERE entity_type = 'EDIFACT') AS edifact_actions,
                          count(journal_records.source_id) FILTER (WHERE entity_type = 'EDIFACT' AND journal_records.error != '') AS edifact_errors_number,
                          array_agg(error) FILTER (WHERE entity_type = 'EDIFACT') AS source_record_error,

                          array_agg(action_type) FILTER (WHERE entity_type = 'INVOICE' AND journal_records.title = 'INVOICE') AS invoice_actions,
                          count(journal_records.source_id) FILTER (WHERE entity_type = 'INVOICE' AND journal_records.title = 'INVOICE' AND journal_records.error != '') AS invoice_errors_number,
                          array_agg(entity_hrid) FILTER (WHERE entity_type = 'INVOICE' AND journal_records.title = 'INVOICE') AS invoice_entity_hrid,
                          array_agg(entity_id) FILTER (WHERE entity_type = 'INVOICE' AND journal_records.title = 'INVOICE') AS invoice_entity_id,
                          array_agg(error) FILTER (WHERE entity_type = 'INVOICE' AND journal_records.title = 'INVOICE') AS invoice_entity_error
                   FROM journal_records
                   WHERE journal_records.job_execution_id = jobExecutionId and entity_type = 'EDIFACT' OR journal_records.title = 'INVOICE'
                   GROUP BY journal_records.source_id, journal_records.job_execution_id, journal_records.source_record_order
            ) AS records_actions ON records_actions.source_id = invoice_line_info.source_id;
END;
$$ LANGUAGE plpgsql;
