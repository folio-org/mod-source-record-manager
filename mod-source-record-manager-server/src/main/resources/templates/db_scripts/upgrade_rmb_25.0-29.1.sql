-- upgrade RMB version from 25.0 to 29.1.5

-- rename columns _id to id
DO $$ BEGIN ALTER TABLE job_executions RENAME COLUMN _id TO id; EXCEPTION WHEN OTHERS THEN END; $$;
DO $$ BEGIN ALTER TABLE job_execution_source_chunks RENAME COLUMN _id TO id; EXCEPTION WHEN OTHERS THEN END; $$;
DO $$ BEGIN ALTER TABLE mapping_rules RENAME COLUMN _id TO id; EXCEPTION WHEN OTHERS THEN END; $$;
DO $$ BEGIN ALTER TABLE journal_records RENAME COLUMN _id TO id; EXCEPTION WHEN OTHERS THEN END; $$;

-- drop RMB functions that no longer exists in RMB 29
DROP FUNCTION IF EXISTS count_estimate_smart (in query text) CASCADE;
DROP FUNCTION IF EXISTS count_estimate_smart_depricated (in query text) CASCADE;

-- UUID functions
-- Return the smallest UUID, or null if both UUIDs are null.
CREATE OR REPLACE FUNCTION uuid_smaller(uuid, uuid) RETURNS uuid AS $$
BEGIN
  IF $1 IS NULL THEN
    RETURN $2;
  END IF;
  IF $2 IS NULL THEN
    RETURN $1;
  END IF;
  IF $1 < $2 THEN
    RETURN $1;
  ELSE
    RETURN $2;
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Return the largest UUID, or null if both UUIDs are null.
CREATE OR REPLACE FUNCTION uuid_larger(uuid, uuid) RETURNS uuid AS $$
BEGIN
  IF $1 IS NULL THEN
    RETURN $2;
  END IF;
  IF $2 IS NULL THEN
    RETURN $1;
  END IF;
  IF $1 > $2 THEN
    RETURN $1;
  ELSE
    RETURN $2;
  END IF;
END;
$$ LANGUAGE plpgsql;

-- Return the next UUID (xxxxxxxx-xxxx-Mxxx-Nxxx-xxxxxxxxxxxx) by adding 1 to x
-- but change neither version byte M nor variant byte N.
CREATE OR REPLACE FUNCTION next_uuid(uuid) RETURNS uuid AS $$
DECLARE
  uuid text;
  digit text;
BEGIN
  uuid = $1;
  FOR i IN REVERSE 36..1 LOOP
    digit := substring(uuid from i for 1);
    -- skip minus, version byte M and variant byte N
    CONTINUE WHEN digit = '-' OR i = 15 OR i = 20;
    CASE digit
      WHEN '0' THEN digit := '1';
      WHEN '1' THEN digit := '2';
      WHEN '2' THEN digit := '3';
      WHEN '3' THEN digit := '4';
      WHEN '4' THEN digit := '5';
      WHEN '5' THEN digit := '6';
      WHEN '6' THEN digit := '7';
      WHEN '7' THEN digit := '8';
      WHEN '8' THEN digit := '9';
      WHEN '9' THEN digit := 'a';
      WHEN 'a' THEN digit := 'b';
      WHEN 'b' THEN digit := 'c';
      WHEN 'c' THEN digit := 'd';
      WHEN 'd' THEN digit := 'e';
      WHEN 'e' THEN digit := 'f';
      WHEN 'f' THEN digit := '0';
      ELSE NULL;
    END CASE;
    uuid = overlay(uuid placing digit from i);
    EXIT WHEN digit <> '0';
  END LOOP;
  RETURN uuid;
END;
$$
LANGUAGE plpgsql;
