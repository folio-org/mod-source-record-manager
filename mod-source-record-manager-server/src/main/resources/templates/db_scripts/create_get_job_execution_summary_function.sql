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
    WITH base_data AS (
      SELECT id, jr.job_execution_id, source_id, entity_id, entity_type, action_type, action_status,
             ROW_NUMBER() OVER (
               PARTITION BY source_id, entity_id, entity_type, action_status
               ORDER BY CASE action_type
                 WHEN 'CREATE' THEN 1
                 WHEN 'UPDATE' THEN 2
                 WHEN 'NON_MATCH' THEN 3
                 WHEN 'MATCH' THEN 4
                 ELSE 99 END) as row_num_per_entity,
             FIRST_VALUE(action_type) OVER (
               PARTITION BY source_id, entity_type
               ORDER BY CASE action_type
                 WHEN 'CREATE' THEN 1
                 WHEN 'UPDATE' THEN 2
                 WHEN 'NON_MATCH' THEN 3
                 WHEN 'MATCH' THEN 4
                 ELSE 99 END
               ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as action_type_max,
             COUNT(CASE WHEN action_type NOT IN ('MATCH', 'PARSE') THEN 1 END) OVER (
               PARTITION BY source_id) > 0 as has_non_match_actions
      FROM journal_records jr
      WHERE jr.job_execution_id = job_id
    ),
    filtered_data AS (
      SELECT * FROM base_data WHERE row_num_per_entity = 1 AND (action_type != 'MATCH' OR (action_type = 'MATCH' AND NOT has_non_match_actions))
    )
    SELECT fd.job_execution_id,
           COUNT(DISTINCT source_id) FILTER (WHERE action_status = 'ERROR') AS total_errors,
           COUNT(DISTINCT source_id) FILTER (WHERE entity_type IN ('MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY') AND action_type = 'CREATE' AND action_status = 'COMPLETED') AS total_created_source_records,
           COUNT(DISTINCT source_id) FILTER (WHERE entity_type IN ('MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY') AND action_type = 'UPDATE' AND action_status = 'COMPLETED') AS total_updated_source_records,
           COUNT(*) FILTER (WHERE entity_type IN ('MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY') AND ((action_type = 'NON_MATCH' AND action_type_max = 'NON_MATCH') OR (action_type = 'MATCH' AND action_type_max = 'MATCH') OR action_status = 'ERROR')) AS total_discarded_source_records,
           COUNT(*) FILTER (WHERE entity_type IN ('MARC_BIBLIOGRAPHIC', 'MARC_HOLDINGS', 'MARC_AUTHORITY') AND action_status = 'ERROR') AS total_source_records_errors,
           COUNT(DISTINCT entity_id) FILTER (WHERE entity_type = 'INSTANCE' AND action_type = 'CREATE' AND action_status = 'COMPLETED') AS total_created_instances,
           COUNT(DISTINCT entity_id) FILTER (WHERE entity_type = 'INSTANCE' AND action_type = 'UPDATE' AND action_status = 'COMPLETED') AS total_updated_instances,
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
           COUNT(DISTINCT source_id) FILTER (WHERE entity_type = 'PO_LINE' AND action_type = 'CREATE' AND action_status = 'COMPLETED') AS total_created_orders,
           0 AS total_updated_orders,
           COUNT(DISTINCT source_id) FILTER (WHERE entity_type = 'PO_LINE' AND ((action_type = 'NON_MATCH' AND action_type_max = 'NON_MATCH') OR (action_type = 'MATCH' AND action_type_max = 'MATCH') OR action_status = 'ERROR')) AS total_discarded_orders,
           COUNT(DISTINCT source_id) FILTER (WHERE entity_type = 'PO_LINE' AND action_status = 'ERROR') AS total_orders_errors,
           COUNT(DISTINCT source_id) FILTER (WHERE entity_type = 'INVOICE' AND action_status = 'COMPLETED') AS total_created_invoices,
           0 AS total_updated_invoices,
           COUNT(DISTINCT source_id) FILTER (WHERE entity_type = 'INVOICE' AND ((action_type = 'NON_MATCH' AND action_type_max = 'NON_MATCH') OR (action_type = 'MATCH' AND action_type_max = 'MATCH') OR action_status = 'ERROR')) AS total_discarded_invoices,
           COUNT(DISTINCT source_id) FILTER (WHERE entity_type = 'INVOICE' AND action_status = 'ERROR') AS total_invoices_errors
    FROM filtered_data fd
    GROUP BY fd.job_execution_id;
END;
$$ LANGUAGE plpgsql;

CREATE INDEX IF NOT EXISTS idx_journal_records_window_opt ON journal_records (job_execution_id, source_id, entity_type, entity_id, action_status) INCLUDE (action_type);
