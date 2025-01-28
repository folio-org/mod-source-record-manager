CREATE OR REPLACE FUNCTION insert_journal_records(
    records jsonb[]
)
    RETURNS void AS
$$
BEGIN
    PERFORM pg_advisory_xact_lock(9817236405);

    INSERT INTO journal_records (
        id,
        job_execution_id,
        source_id,
        entity_type,
        entity_id,
        entity_hrid,
        action_type,
        action_status,
        action_date,
        source_record_order,
        error,
        title,
        instance_id,
        holdings_id,
        order_id,
        permanent_location_id,
        tenant_id
    )
    SELECT (r ->> 'id')::uuid,
           (r ->> 'job_execution_id')::uuid,
           (r ->> 'source_id')::uuid,
           r ->> 'entity_type',
           r ->> 'entity_id',
           r ->> 'entity_hrid',
           r ->> 'action_type',
           r ->> 'action_status',
           to_timestamp((r ->> 'action_date')::bigint / 1000),
           (r ->> 'source_record_order')::integer,
           r ->> 'error',
           r ->> 'title',
           r ->> 'instance_id',
           r ->> 'holdings_id',
           r ->> 'order_id',
           r ->> 'permanent_location_id',
           r ->> 'tenant_id'
    FROM unnest(records) AS r
    ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE plpgsql;
