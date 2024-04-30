-- Request to delete get_job_execution_summary with a different return format.
DROP FUNCTION IF EXISTS get_job_execution_summary(uuid);

CREATE OR REPLACE FUNCTION get_job_execution_summary(job_id uuid)
RETURNS TABLE(
  job_execution_id uuid, total_errors bigint,
  total_created_source_records bigint, total_updated_source_records bigint, total_discarded_source_records bigint, total_source_records_errors bigint,
  total_created_instances bigint, total_updated_instances bigint, total_discarded_instances bigint, total_instances_errors bigint,
  total_created_holdings bigint, total_updated_holdings bigint, total_discarded_holdings bigint, total_holdings_errors bigint,
  total_created_items bigint, total_updated_items bigint, total_discarded_items bigint, total_items_errors bigint,
  total_created_authorities bigint, total_updated_authorities bigint, total_discarded_authorities bigint, total_authorities_errors bigint,
  total_created_orders bigint, total_updated_orders integer, total_discarded_orders bigint, total_orders_errors bigint,
  total_created_invoices bigint, total_updated_invoices integer, total_discarded_invoices bigint, total_invoices_errors bigint
) AS $$
BEGIN
  RETURN QUERY
    SELECT
      journal_records.job_execution_id,
      COUNT(DISTINCT(source_id)) FILTER (WHERE action_status = 'ERROR') AS total_errors,
      COUNT(DISTINCT(source_id)) FILTER (WHERE entity_type IN ('MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY') AND action_type = 'CREATE' AND action_status = 'COMPLETED') AS total_created_source_records,
      COUNT(DISTINCT(source_id)) FILTER (WHERE entity_type IN ('MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY') AND action_type = 'UPDATE' AND action_status = 'COMPLETED') AS total_updated_source_records,
      COUNT(*) FILTER (WHERE entity_type IN ('MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY') AND ((action_type = 'NON_MATCH' AND action_type_max = 'NON_MATCH') OR (action_type = 'MATCH' AND action_type_max = 'MATCH') OR action_status = 'ERROR')) AS total_discarded_source_records,
      COUNT(*) FILTER (WHERE entity_type IN ('MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY') AND action_status = 'ERROR') AS total_source_records_errors,

      COUNT(DISTINCT(entity_id)) FILTER (WHERE entity_type = 'INSTANCE' AND action_type = 'CREATE' AND action_status = 'COMPLETED') AS total_created_instances,
      COUNT(DISTINCT(entity_id)) FILTER (WHERE entity_type = 'INSTANCE' AND action_type = 'UPDATE' AND action_status = 'COMPLETED') AS total_updated_instances,
      COUNT(*) FILTER (WHERE entity_type = 'INSTANCE' AND ((action_type = 'NON_MATCH' AND action_type_max = 'NON_MATCH') OR (action_type = 'MATCH' AND action_type_max = 'MATCH') OR action_status = 'ERROR')) AS total_discarded_instances,
      COUNT(*) FILTER (WHERE entity_type = 'INSTANCE' AND action_status = 'ERROR') AS total_instances_errors,

      COUNT(*) FILTER (WHERE entity_type = 'HOLDINGS' AND action_type = 'CREATE' AND action_status = 'COMPLETED') AS total_created_holdings,
      COUNT(*) FILTER (WHERE entity_type = 'HOLDINGS' AND action_type = 'UPDATE' AND action_status = 'COMPLETED') AS total_updated_holdings,
      COUNT(*) FILTER (WHERE entity_type = 'HOLDINGS' AND ((action_type = 'NON_MATCH' AND action_type_max = 'NON_MATCH') OR (action_type = 'MATCH' AND action_type_max = 'MATCH') OR action_status = 'ERROR')) AS total_discarded_holdings,
      COUNT(*) FILTER (WHERE entity_type = 'HOLDINGS' AND action_status = 'ERROR') AS total_holdings_errors,

      COUNT(*) FILTER (WHERE entity_type = 'ITEM' AND action_type = 'CREATE' AND action_status = 'COMPLETED') AS total_created_items,
      COUNT(*) FILTER (WHERE entity_type = 'ITEM' AND action_type = 'UPDATE' AND action_status = 'COMPLETED') AS total_updated_items,
      COUNT(*) FILTER (WHERE entity_type = 'ITEM' AND ((action_type = 'NON_MATCH' AND action_type_max = 'NON_MATCH') OR (action_type = 'MATCH' AND action_type_max = 'MATCH') OR action_status = 'ERROR')) AS total_discarded_items,
      COUNT(*) FILTER (WHERE entity_type = 'ITEM' AND action_status = 'ERROR') AS total_items_errors,

      COUNT(*) FILTER (WHERE entity_type = 'AUTHORITY' AND action_type = 'CREATE' AND action_status = 'COMPLETED') AS total_created_authorities,
      COUNT(*) FILTER (WHERE entity_type = 'AUTHORITY' AND action_type = 'UPDATE' AND action_status = 'COMPLETED') AS total_updated_authorities,
      COUNT(*) FILTER (WHERE entity_type = 'AUTHORITY' AND ((action_type = 'NON_MATCH' AND action_type_max = 'NON_MATCH') OR (action_type = 'MATCH' AND action_type_max = 'MATCH') OR action_status = 'ERROR')) AS total_discarded_authorities,
      COUNT(*) FILTER (WHERE entity_type = 'AUTHORITY' AND action_status = 'ERROR') AS total_authorities_errors,

      COUNT(DISTINCT(source_id)) FILTER (WHERE entity_type = 'PO_LINE' AND action_type = 'CREATE' AND action_status = 'COMPLETED') AS total_created_orders,
      0 AS total_updated_orders,
      COUNT(DISTINCT(source_id)) FILTER (WHERE entity_type = 'PO_LINE' AND ((action_type = 'NON_MATCH' AND action_type_max = 'NON_MATCH') OR (action_type = 'MATCH' AND action_type_max = 'MATCH') OR action_status = 'ERROR')) AS total_discarded_orders,
      COUNT(DISTINCT(source_id)) FILTER (WHERE entity_type = 'PO_LINE' AND action_status = 'ERROR') AS total_orders_errors,

      COUNT(DISTINCT(source_id)) FILTER (WHERE entity_type = 'INVOICE' AND action_status = 'COMPLETED') AS total_created_invoices,
      0 AS total_updated_invoices,
      COUNT(DISTINCT(source_id)) FILTER (WHERE entity_type = 'INVOICE' AND ((action_type = 'NON_MATCH' AND action_type_max = 'NON_MATCH') OR (action_type = 'MATCH' AND action_type_max = 'MATCH') OR action_status = 'ERROR')) AS total_discarded_invoices,
      COUNT(DISTINCT(source_id)) FILTER (WHERE entity_type = 'INVOICE' AND action_status = 'ERROR') AS total_invoices_errors
    FROM journal_records
	    INNER JOIN (SELECT entity_id as entity_id_max, entity_type as entity_type_max, action_status as action_status_max,(array_agg(id ORDER BY array_position(array['CREATE', 'UPDATE', 'NON_MATCH'], action_type)))[1] as id,
	    source_id as temp_source_id
        FROM journal_records
        WHERE journal_records.job_execution_id = job_id AND action_type != 'MATCH'
			  GROUP BY source_id, entity_id, entity_type, action_status
			  UNION ALL
			  SELECT entity_id as entity_id_max, entity_type as entity_type_max, action_status as action_status_max,(array_agg(id ORDER BY array_position(array['MATCH'], action_type)))[1] as id,
        	    source_id as temp_source_id
        FROM journal_records
        WHERE journal_records.job_execution_id = job_id AND action_type = 'MATCH'
        AND NOT EXISTS (SELECT 1 FROM journal_records jr
                        WHERE jr.job_execution_id = job_id
                        AND jr.action_type NOT IN ('MATCH', 'PARSE')
                        AND jr.source_id = journal_records.source_id)
        GROUP BY source_id, entity_id, entity_type, action_status) AS actions
      ON actions.id = journal_records.id AND actions.temp_source_id = journal_records.source_id
      INNER JOIN (SELECT (array_agg(action_type ORDER BY array_position(array['CREATE', 'UPDATE', 'NON_MATCH', 'MATCH'], action_type)))[1] as action_type_max, source_id as source_id_max, entity_type as entity_type_max
        FROM journal_records
        WHERE journal_records.job_execution_id = job_id
        GROUP BY entity_type_max, source_id_max) AS action_type_by_source
      ON action_type_by_source.source_id_max = journal_records.source_id AND action_type_by_source.entity_type_max = journal_records.entity_type
    GROUP BY (journal_records.job_execution_id);
END;
$$ LANGUAGE plpgsql;
