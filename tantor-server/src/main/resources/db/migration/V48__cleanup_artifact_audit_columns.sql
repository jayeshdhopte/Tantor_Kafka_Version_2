-- V48: Remove duplicate artifact/audit columns and keep artifact resource display concise.

DROP TRIGGER IF EXISTS trg_artifact_audit_to_global ON kf_artifact_audit_log;

CREATE OR REPLACE FUNCTION sync_artifact_audit_to_global()
RETURNS TRIGGER AS $$
DECLARE
    artifact_uuid UUID;
    artifact_text TEXT;
BEGIN
    artifact_text := NULLIF(NEW.artifact_id::text, '');
    artifact_uuid := CASE
        WHEN artifact_text ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
        THEN artifact_text::UUID
        ELSE NULL
    END;

    INSERT INTO kf_audit_logs (
        id, action, event, resource_type, resource_id, resource, user_name, event_category,
        status, details, origin, created_time,
        host_ip, host_name, artifact_id, created_by
    )
    VALUES (
        NEW.id,
        NEW.action,
        NEW.action,
        COALESCE(NULLIF(NEW.resource_type, ''), 'ARTIFACT'),
        artifact_text,
        'Artifact',
        COALESCE(NULLIF(NEW.user_name, ''), NULLIF(NEW.created_by, ''), 'system'),
        COALESCE(NULLIF(NEW.event_category, ''), 'PACKAGE'),
        COALESCE(NULLIF(NEW.status, ''), 'SUCCESS'),
        NEW.details::jsonb,
        'ARTIFACT_REPO',
        NEW.created_at,
        NULLIF(NEW.host_ip, ''),
        NULLIF(NEW.host_name, ''),
        artifact_uuid,
        COALESCE(NULLIF(NEW.created_by, ''), NULLIF(NEW.user_name, ''), 'system')
    )
    ON CONFLICT (id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_artifact_audit_to_global
AFTER INSERT ON kf_artifact_audit_log
FOR EACH ROW EXECUTE PROCEDURE sync_artifact_audit_to_global();

ALTER TABLE kf_artifact
    DROP COLUMN IF EXISTS artifact_id,
    DROP COLUMN IF EXISTS resource_type;

ALTER TABLE kf_artifact_audit_log
    DROP COLUMN IF EXISTS actor_user,
    DROP COLUMN IF EXISTS version,
    DROP COLUMN IF EXISTS path,
    DROP COLUMN IF EXISTS version_no,
    DROP COLUMN IF EXISTS path_of_tar,
    DROP COLUMN IF EXISTS full_file_path;

ALTER TABLE kf_audit_logs
    DROP COLUMN IF EXISTS actor_user,
    DROP COLUMN IF EXISTS approval,
    DROP COLUMN IF EXISTS ip_address,
    DROP COLUMN IF EXISTS request_id;
