BEGIN;

-- 1. Clean up empty strings
UPDATE speaker
SET created_at = CURRENT_TIMESTAMP
WHERE created_at IS NULL;

UPDATE speaker
SET updated_at = NULL
WHERE updated_at IS NULL;  -- no-op, but keeps it clean

UPDATE speaker
SET deleted_at = NULL
WHERE deleted_at IS NULL;  -- no-op, but keeps it clean


-- 2. Backfill NULL created_at with current timestamp
UPDATE speaker
SET created_at = CURRENT_TIMESTAMP
WHERE created_at IS NULL;

-- 3. Convert created_at
ALTER TABLE speaker
    ALTER COLUMN created_at DROP DEFAULT,
    ALTER COLUMN created_at TYPE timestamptz USING created_at::timestamptz,
    ALTER COLUMN created_at SET DEFAULT CURRENT_TIMESTAMP,
    ALTER COLUMN created_at SET NOT NULL;

-- 4. Convert updated_at
ALTER TABLE speaker
    ALTER COLUMN updated_at DROP DEFAULT,
    ALTER COLUMN updated_at TYPE timestamptz USING updated_at::timestamptz,
    ALTER COLUMN updated_at SET DEFAULT CURRENT_TIMESTAMP,
    ALTER COLUMN updated_at DROP NOT NULL;

-- 5. Convert deleted_at
ALTER TABLE speaker
    ALTER COLUMN deleted_at DROP DEFAULT,
    ALTER COLUMN deleted_at TYPE timestamptz USING deleted_at::timestamptz,
    ALTER COLUMN deleted_at DROP NOT NULL;

COMMIT;
