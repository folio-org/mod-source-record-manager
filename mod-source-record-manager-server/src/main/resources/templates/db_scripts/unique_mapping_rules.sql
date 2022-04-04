DO $$
BEGIN
  BEGIN
    ALTER TABLE mapping_rules ADD CONSTRAINT unique_record_type UNIQUE (record_type);
  EXCEPTION
    WHEN duplicate_table THEN NULL;  -- ignore if constraint already exists
  END;
END $$;
