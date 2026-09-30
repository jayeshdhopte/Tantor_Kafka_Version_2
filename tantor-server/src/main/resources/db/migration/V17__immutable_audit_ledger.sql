CREATE EXTENSION IF NOT EXISTS "pgcrypto";

ALTER TABLE audit_logs
    ADD COLUMN IF NOT EXISTS cluster_id UUID,
    ADD COLUMN IF NOT EXISTS actor VARCHAR(255) NOT NULL DEFAULT 'system',
    ADD COLUMN IF NOT EXISTS event_category VARCHAR(80) NOT NULL DEFAULT 'SYSTEM',
    ADD COLUMN IF NOT EXISTS status VARCHAR(40) NOT NULL DEFAULT 'SUCCESS',
    ADD COLUMN IF NOT EXISTS old_value JSONB,
    ADD COLUMN IF NOT EXISTS new_value JSONB,
    ADD COLUMN IF NOT EXISTS approval JSONB,
    ADD COLUMN IF NOT EXISTS source VARCHAR(80) NOT NULL DEFAULT 'MANAGEMENT_SERVER',
    ADD COLUMN IF NOT EXISTS request_id VARCHAR(100),
    ADD COLUMN IF NOT EXISTS previous_hash CHAR(64),
    ADD COLUMN IF NOT EXISTS record_hash CHAR(64);

UPDATE audit_logs
SET source = 'LEGACY',
    record_hash = md5(id::text || created_at::text) || md5(created_at::text || id::text)
WHERE record_hash IS NULL;

INSERT INTO audit_logs (
    id, action, entity_type, entity_id, details, cluster_id, actor,
    event_category, status, source, created_at, record_hash
)
SELECT gen_random_uuid(), 'LEGACY_ACTIVITY', 'CLUSTER', cluster_id::text,
       jsonb_build_object('level', level, 'message', message), cluster_id,
       'system', 'SYSTEM',
       CASE WHEN level IN ('ERROR', 'CRITICAL') THEN 'FAILED' ELSE 'SUCCESS' END,
       'LEGACY', created_at,
       md5(id::text || created_at::text) || md5(created_at::text || id::text)
FROM activity_logs;

ALTER TABLE audit_logs ALTER COLUMN record_hash SET NOT NULL;

CREATE INDEX IF NOT EXISTS idx_audit_category_created ON audit_logs(event_category, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_actor_created ON audit_logs(actor, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_resource ON audit_logs(entity_type, entity_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_audit_status_created ON audit_logs(status, created_at DESC);

CREATE OR REPLACE FUNCTION prevent_audit_log_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'audit_logs is append-only; % is forbidden', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_audit_logs_immutable ON audit_logs;
CREATE TRIGGER trg_audit_logs_immutable
BEFORE UPDATE OR DELETE ON audit_logs
FOR EACH ROW EXECUTE PROCEDURE prevent_audit_log_mutation();
