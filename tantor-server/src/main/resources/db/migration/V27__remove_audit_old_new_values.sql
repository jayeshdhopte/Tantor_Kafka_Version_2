ALTER TABLE audit_logs
    DROP COLUMN IF EXISTS old_value,
    DROP COLUMN IF EXISTS new_value;
