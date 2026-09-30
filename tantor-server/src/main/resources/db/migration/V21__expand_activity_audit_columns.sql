-- Activity audit details were added to the entity after the original V5 table.
-- Keep V5 immutable and evolve the existing table additively.
ALTER TABLE activity_logs
    ADD COLUMN IF NOT EXISTS event_type VARCHAR(80),
    ADD COLUMN IF NOT EXISTS action VARCHAR(100),
    ADD COLUMN IF NOT EXISTS actor VARCHAR(255) NOT NULL DEFAULT 'system',
    ADD COLUMN IF NOT EXISTS resource_type VARCHAR(80),
    ADD COLUMN IF NOT EXISTS resource_id VARCHAR(255),
    ADD COLUMN IF NOT EXISTS old_value TEXT,
    ADD COLUMN IF NOT EXISTS new_value TEXT,
    ADD COLUMN IF NOT EXISTS ip_address VARCHAR(45),
    ADD COLUMN IF NOT EXISTS event_status VARCHAR(40) NOT NULL DEFAULT 'SUCCESS',
    ADD COLUMN IF NOT EXISTS approval_status VARCHAR(40),
    ADD COLUMN IF NOT EXISTS metadata TEXT;

UPDATE activity_logs
SET event_type = COALESCE(event_type, 'LEGACY_ACTIVITY'),
    action = COALESCE(action, 'RECORDED'),
    actor = COALESCE(actor, 'system'),
    event_status = COALESCE(event_status,
        CASE WHEN UPPER(level) IN ('ERROR', 'CRITICAL') THEN 'FAILED' ELSE 'SUCCESS' END)
WHERE event_type IS NULL OR action IS NULL OR actor IS NULL OR event_status IS NULL;

CREATE INDEX IF NOT EXISTS idx_activity_event_created
    ON activity_logs(event_type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_activity_resource_created
    ON activity_logs(resource_type, resource_id, created_at DESC);
