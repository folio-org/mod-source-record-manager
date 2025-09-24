CREATE TABLE IF NOT EXISTS rule_migration_change_log (
    id uuid PRIMARY KEY,
    migration_id uuid,
    record_type rule_type,
    file_name text,
    description text,
    timestamp timestamp
);
