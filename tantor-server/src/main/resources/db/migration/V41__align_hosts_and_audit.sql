-- V41: Fix artifact audit sync trigger and align host schemas

-- 1. Fix the artifact audit trigger to properly populate host_ip, host_name, artifact_id, and created_by
CREATE OR REPLACE FUNCTION sync_artifact_audit_to_global()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO kf_audit_logs (
        id, action, resource_type, resource_id, user_name, event_category, 
        status, details, ip_address, origin, created_time,
        host_ip, host_name, artifact_id, created_by, resource
    )
    VALUES (
        NEW.id, NEW.action, NEW.resource_type, NEW.artifact_id, NEW.user_name, NEW.event_category, 
        NEW.status, NEW.details, NEW.host_ip, 'ARTIFACT_REPO', NEW.created_at,
        NEW.host_ip, NEW.host_name, CAST(NULLIF(NEW.artifact_id, '') AS UUID), NEW.created_by, COALESCE(NEW.host_name, NEW.host_ip)
    )
    ON CONFLICT (id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Add missing columns to kf_hosts
ALTER TABLE kf_hosts ADD COLUMN IF NOT EXISTS host_ip VARCHAR(255);
ALTER TABLE kf_hosts ADD COLUMN IF NOT EXISTS removed BOOLEAN DEFAULT FALSE;
ALTER TABLE kf_hosts ADD COLUMN IF NOT EXISTS action VARCHAR(100);
ALTER TABLE kf_hosts ADD COLUMN IF NOT EXISTS resource_type VARCHAR(100);
ALTER TABLE kf_hosts ADD COLUMN IF NOT EXISTS "user" VARCHAR(128);

-- 3. Migrate data from kf_host_audit_log to kf_audit_logs, then drop legacy table
DO $$ 
BEGIN
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_host_audit_log') THEN
        INSERT INTO kf_audit_logs (
            id, action, resource_type, resource_id, user_name, event_category, 
            status, origin, created_time,
            host_ip, host_name, host_id, created_by, resource, details
        )
        SELECT 
            id, action, 'HOST', host_id, COALESCE(actor_user, created_by, 'system'), 'AGENT', 
            status, origin, created_at,
            host_ip, host_name, host_id, created_by, COALESCE(resource, 'HOST'), details
        FROM kf_host_audit_log
        ON CONFLICT (id) DO NOTHING;

        DROP TABLE kf_host_audit_log;
    END IF;
END $$;
