-- 1. Re-create the trigger function without agent_status
CREATE OR REPLACE FUNCTION sync_host_to_global_audit()
RETURNS TRIGGER AS $$
DECLARE
    host_action VARCHAR(100);
    host_status VARCHAR(50);
BEGIN
    IF TG_OP = 'INSERT' THEN
        host_action := COALESCE(NULLIF(NEW.action, ''), 'HOST_REGISTERED');
    ELSE
        host_action := COALESCE(NULLIF(NEW.action, ''), 'HOST_UPDATED');
    END IF;

    host_status := CASE
        WHEN COALESCE(NEW.removed, FALSE) THEN 'REMOVED'
        ELSE COALESCE(NULLIF(NEW.status, ''), 'UNKNOWN')
    END;

    INSERT INTO kf_audit_logs (
        action, event, resource_type, resource_id, resource, user_name, event_category,
        status, origin, created_time, host_id, host_ip, host_name, cluster_id, created_by, details
    )
    VALUES (
        host_action,
        host_action,
        'HOST',
        NEW.id,
        COALESCE(NULLIF(NEW.hostname, ''), NULLIF(NEW.host_ip, ''), NEW.id),
        COALESCE(NULLIF(NEW.user_name, ''), 'system'),
        'AGENT',
        host_status,
        'AGENT',
        COALESCE(NEW.updated_at, NEW.created_at, now()),
        NEW.id,
        NULLIF(NEW.host_ip, ''),
        NULLIF(NEW.hostname, ''),
        NEW.cluster_id,
        COALESCE(NULLIF(NEW.user_name, ''), 'system'),
        jsonb_build_object(
            'availability', CASE WHEN COALESCE(NEW.removed, FALSE) OR upper(host_status) IN ('OCCUPIED', 'PENDING', 'OFFLINE', 'REMOVED') THEN 'unavailable' ELSE 'available' END,
            'agentVersion', NEW.agent_version,
            'javaVersion', NEW.java_version
        )
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 2. Drop the old trigger that watches agent_status
DROP TRIGGER IF EXISTS trg_host_to_global_audit ON kf_hosts;

-- 3. Re-create the trigger without agent_status
CREATE TRIGGER trg_host_to_global_audit
AFTER INSERT OR UPDATE OF status, host_ip, hostname, cluster_id, removed, action ON kf_hosts
FOR EACH ROW EXECUTE PROCEDURE sync_host_to_global_audit();

-- 4. Now we can drop the columns safely
-- Drop userId from AuditLogs
ALTER TABLE kf_audit_logs DROP COLUMN IF EXISTS user_id;

-- Drop ipAddresses and agentStatus from Hosts
ALTER TABLE kf_hosts DROP COLUMN IF EXISTS ip_addresses;
ALTER TABLE kf_hosts DROP COLUMN IF EXISTS agent_status;

-- Drop failedReason from Tasks
ALTER TABLE kf_tasks DROP COLUMN IF EXISTS failed_reason;
