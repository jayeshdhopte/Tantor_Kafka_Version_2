-- V66: Avoid noisy host audit rows from heartbeat/metrics-only updates.
--
-- V64 repaired the host audit trigger after redundant username columns were
-- removed, but it also dropped the no-op update guard from V47. Hibernate may
-- include audited columns in an UPDATE even when only heartbeat/metric fields
-- changed, so the trigger must compare OLD and NEW values before inserting an
-- audit event.

CREATE OR REPLACE FUNCTION sync_host_to_global_audit()
RETURNS TRIGGER AS $$
DECLARE
    host_action VARCHAR(100);
    host_status VARCHAR(50);
    host_actor VARCHAR(255);
BEGIN
    IF TG_OP = 'UPDATE'
       AND OLD.status IS NOT DISTINCT FROM NEW.status
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
        ELSE COALESCE(NULLIF(NEW.status, ''), 'UNKNOWN')
    END;

    host_actor := COALESCE(NULLIF(NEW.agent_name, ''), NULLIF(NEW.hostname, ''), NEW.id, 'system');
    IF NULLIF(NEW.host_ip, '') IS NOT NULL AND host_actor NOT LIKE '%(%' THEN
        host_actor := host_actor || ' (' || NEW.host_ip || ')';
    END IF;

    INSERT INTO kf_audit_logs (
        action,
        event_category,
        resource_type,
        resource_id,
        resource,
        status,
        origin,
        created_time,
        host_id,
        host_ip,
        host_name,
        cluster_id,
        created_by,
        details
    )
    VALUES (
        host_action,
        'AGENT',
        'HOST',
        NEW.id,
        COALESCE(NULLIF(NEW.hostname, ''), NULLIF(NEW.host_ip, ''), NEW.id),
        host_status,
        'AGENT',
        COALESCE(NEW.updated_at, NEW.created_at, now()),
        NEW.id,
        NULLIF(NEW.host_ip, ''),
        NULLIF(NEW.hostname, ''),
        NEW.cluster_id,
        host_actor,
        jsonb_build_object(
            'availability', CASE
                WHEN COALESCE(NEW.removed, FALSE)
                    OR upper(host_status) IN ('OCCUPIED', 'PENDING', 'OFFLINE', 'REMOVED')
                    THEN 'unavailable'
                ELSE 'available'
            END,
            'agentName', NEW.agent_name,
            'agentVersion', NEW.agent_version,
            'agentPath', NEW.agent_path,
            'javaVersion', NEW.java_version
        )
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
