-- V45: Fix artifact audit trigger JSONB casting and backfill rows missed when V44 failed post-upload audit sync.

ALTER TABLE kf_audit_logs
    ALTER COLUMN resource TYPE VARCHAR(1024);

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
        status, details, ip_address, origin, created_time,
        host_ip, host_name, artifact_id, created_by
    )
    VALUES (
        NEW.id,
        NEW.action,
        NEW.action,
        COALESCE(NULLIF(NEW.resource_type, ''), 'ARTIFACT'),
        artifact_text,
        COALESCE(NULLIF(NEW.full_file_path, ''), NULLIF(NEW.path_of_tar, ''), NULLIF(NEW.host_name, ''), artifact_text),
        COALESCE(NULLIF(NEW.user_name, ''), NULLIF(NEW.created_by, ''), 'system'),
        COALESCE(NULLIF(NEW.event_category, ''), 'PACKAGE'),
        COALESCE(NULLIF(NEW.status, ''), 'SUCCESS'),
        NEW.details::jsonb,
        NULLIF(NEW.host_ip, ''),
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

DROP TRIGGER IF EXISTS trg_artifact_audit_to_global ON kf_artifact_audit_log;
CREATE TRIGGER trg_artifact_audit_to_global
AFTER INSERT ON kf_artifact_audit_log
FOR EACH ROW EXECUTE PROCEDURE sync_artifact_audit_to_global();

INSERT INTO kf_artifact_audit_log (
    id, user_name, event_category, action, resource_type, artifact_id, status, details,
    created_at, host_ip, host_name, created_by, version_no, path_of_tar, full_file_path, checksum
)
SELECT
    gen_random_uuid(),
    COALESCE(NULLIF(a.user_name, ''), NULLIF(a.created_by, ''), 'system'),
    'PACKAGE',
    CASE
        WHEN upper(COALESCE(a.action, '')) = 'DELETE' THEN 'PACKAGE_REMOVED'
        WHEN upper(COALESCE(a.action, '')) = 'DOWNLOAD' THEN 'PACKAGE_DOWNLOADED'
        WHEN upper(COALESCE(a.action, '')) LIKE 'VERIFY%' THEN 'PACKAGE_VERIFIED'
        ELSE 'PACKAGE_UPLOADED'
    END,
    'ARTIFACT',
    a.id::text,
    CASE
        WHEN upper(COALESCE(a.status, '')) IN ('AVAILABLE', 'DELETED') THEN 'SUCCESS'
        ELSE 'FAILED'
    END,
    jsonb_build_object(
        'validationStatus', a.status,
        'serviceType', a.service_type,
        'version', a.version_no,
        'fileName', a.binary_file_name,
        'sha256', a.checksum,
        'size', a.file_size_bytes
    ),
    a.created_time,
    NULLIF(a.host_ip, ''),
    NULLIF(a.hostname, ''),
    COALESCE(NULLIF(a.created_by, ''), NULLIF(a.user_name, ''), 'system'),
    a.version_no,
    a.path_of_tar,
    a.full_file_path,
    a.checksum
FROM kf_artifact a
WHERE NOT EXISTS (
    SELECT 1
    FROM kf_artifact_audit_log aal
    WHERE aal.artifact_id::text = a.id::text
      AND aal.action = CASE
          WHEN upper(COALESCE(a.action, '')) = 'DELETE' THEN 'PACKAGE_REMOVED'
          WHEN upper(COALESCE(a.action, '')) = 'DOWNLOAD' THEN 'PACKAGE_DOWNLOADED'
          WHEN upper(COALESCE(a.action, '')) LIKE 'VERIFY%' THEN 'PACKAGE_VERIFIED'
          ELSE 'PACKAGE_UPLOADED'
      END
);
