-- V47: Avoid noisy host audit rows from unchanged heartbeat-style updates.

CREATE OR REPLACE FUNCTION sync_host_to_global_audit()
RETURNS TRIGGER AS $$
DECLARE
    host_action VARCHAR(100);
    host_status VARCHAR(50);
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.status IS NOT DISTINCT FROM NEW.status
       AND OLD.agent_status IS NOT DISTINCT FROM NEW.agent_status
       AND OLD.host_ip IS NOT DISTINCT FROM NEW.host_ip
       AND OLD.hostname IS NOT DISTINCT FROM NEW.hostname
       AND OLD.cluster_id IS NOT DISTINCT FROM NEW.cluster_id
       AND OLD.removed IS NOT DISTINCT FROM NEW.removed
       AND OLD.action IS NOT DISTINCT FROM NEW.action THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'INSERT' THEN
        host_action := COALESCE(NULLIF(NEW.action, ''), 'HOST_REGISTERED');
    ELSE
        host_action := COALESCE(NULLIF(NEW.action, ''), 'HOST_UPDATED');
    END IF;

    host_status := CASE
        WHEN COALESCE(NEW.removed, FALSE) THEN 'REMOVED'
        ELSE COALESCE(NULLIF(NEW.status, ''), NULLIF(NEW.agent_status, ''), 'UNKNOWN')
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
        COALESCE(NULLIF(NEW.hostname, ''), NEW.id),
        COALESCE(NULLIF(NEW.user_name, ''), 'system'),
        'AGENT',
        host_status,
        'AGENT',
        now(),
        NEW.id,
        NULLIF(NEW.host_ip, ''),
        NULLIF(NEW.hostname, ''),
        NEW.cluster_id,
        COALESCE(NULLIF(NEW.user_name, ''), 'system'),
        jsonb_build_object(
            'availability', CASE WHEN COALESCE(NEW.removed, FALSE) OR upper(host_status) IN ('OCCUPIED', 'PENDING', 'OFFLINE', 'REMOVED') THEN 'unavailable' ELSE 'available' END,
            'agentName', NEW.agent_name,
            'agentVersion', NEW.agent_version,
            'agentPath', NEW.agent_path,
            'javaVersion', NEW.java_version
        )
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
