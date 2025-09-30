DROP TABLE IF EXISTS rule_migration_change_log CASCADE;
CREATE TABLE IF NOT EXISTS rule_migration_change_log (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
    migration_id uuid UNIQUE,
    record_type rule_type,
    file_name text,
    description text,
    timestamp timestamp DEFAULT now() NOT NULL
);

-- try to avoid the "unsafe use of new value "MARC_AUTHORITY" of enum type rule_type (55P04)" error.
ALTER TYPE rule_type ADD VALUE IF NOT EXISTS 'MARC_AUTHORITY';
COMMIT;

-- save already applied migrations
INSERT INTO rule_migration_change_log (migration_id, record_type, file_name, description)
VALUES
('7a7a4270-7a9a-420e-a27e-1763ed03a57f', 'MARC_HOLDING', 'HoldingsMapping852CallNumberTypeCustomMigration',
'Holdings mapping rules: update rule for callNumberType'),
('85067d3d-5c64-440e-b00b-e952d0c37473', 'MARC_AUTHORITY', 'AuthorityCancelledLccnMappingRenamingMigration',
'Authority mapping rules: rename Cancelled LCCN to Canceled LCCN'),
('cdfc50e7-0eb2-42cf-8987-9d78bc2b8032', 'MARC_AUTHORITY', 'AuthorityMapping010LccnCustomMigration',
'Authority mapping rules: update rule for LCCN'),
('09ceac2a-07a0-452f-a3b5-b26a60723a7a', 'MARC_AUTHORITY', 'AuthorityMappingNameSubjectMetadataCustomMigration',
'Authority mapping rules: update rules for name fields with subject metadata'),
('43021382-2631-4a00-9acc-1aaefaf6b202', 'MARC_BIB', 'BibliographicCancelledLccnMappingRenamingMigration',
'Bibliographic mapping rules: rename Cancelled LCCN to Canceled LCCN'),
('9e1d0eb6-4965-4e56-af56-45d0677c0ddc', 'MARC_AUTHORITY', 'AuthorityMappingChronSubdivisionCustomMigration',
'Authority mapping rules: add rules for chronological subdivision fields'),
('f27072af-e322-4574-8667-81c066c41ac8', 'MARC_AUTHORITY', 'AuthorityMappingChronTermCustomMigration',
'Authority mapping rules: add rules for chronological term fields'),
('c9c0805c-4a12-4ded-9771-44ff6abea785', 'MARC_AUTHORITY', 'AuthorityMappingFormSubdivisionCustomMigration',
'Authority mapping rules: add rules for form subdivision fields'),
('4f07c706-a740-4e8b-a6a3-1c53f46a314c', 'MARC_AUTHORITY', 'AuthorityMappingGeneralSubdivisionCustomMigration',
'Authority mapping rules: add rules for general subdivision fields'),
('6ba3ce92-f327-45f3-855f-5d8147885566', 'MARC_AUTHORITY', 'AuthorityMappingGeographicSubdivisionCustomMigration',
'Authority mapping rules: add rules for geographic subdivision fields'),
('fa562314-18b1-4cc1-9585-a06530a25809', 'MARC_AUTHORITY', 'AuthorityMappingMediumPerfTermCustomMigration',
'Authority mapping rules: add rules for medium of performance term fields'),
('6d17fe92-39f3-494f-9e5f-e104fdabe78a', 'MARC_AUTHORITY', 'AuthorityMappingNamedEventCustomMigration',
'Authority mapping rules: add rules for named event fields')
ON CONFLICT (migration_id) DO NOTHING;
