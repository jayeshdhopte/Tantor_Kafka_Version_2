-- V64 restored inserts into legacy display columns no longer present in the
-- cluster audit table. Keep the current schema and preserve the audit event.
CREATE OR REPLACE FUNCTION sync_cluster_to_cluster_audit()
RETURNS TRIGGER AS $$
DECLARE
    cluster_action VARCHAR(100);
    bootstrap_ip VARCHAR(100);
    cluster_actor VARCHAR(128);
BEGIN
    IF TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;

    IF NEW.status IS DISTINCT FROM OLD.status AND upper(COALESCE(NEW.status, '')) = 'SUCCESS' THEN
        cluster_action := 'CLUSTER_DEPLOYED';
    ELSIF NEW.status IS DISTINCT FROM OLD.status AND upper(COALESCE(NEW.status, '')) = 'FAILED' THEN
        cluster_action := 'CLUSTER_DEPLOYMENT_FAILED';
    ELSE
        RETURN NEW;
    END IF;

    bootstrap_ip := split_part(split_part(COALESCE(NEW.bootstrap_servers, ''), ',', 1), ':', 1);
    cluster_actor := COALESCE(NULLIF(NEW.updated_by, ''), NULLIF(NEW.created_by, ''), 'system');

    INSERT INTO kf_cluster_audit_log (
        cluster_id, action, event, status, origin, resource, resource_type,
        cluster_name, bootstrap_ip, env, kafka_version, mode, created_by,
        created_at, details
    ) VALUES (
        NEW.id, cluster_action, cluster_action,
        CASE WHEN upper(COALESCE(NEW.status, '')) = 'FAILED' THEN 'FAILED' ELSE 'SUCCESS' END,
        'MANAGEMENT_SERVER', COALESCE(NULLIF(NEW.cluster_name, ''), NEW.id::text), 'CLUSTER',
        NEW.cluster_name, NULLIF(bootstrap_ip, ''), NEW.environment, NEW.kafka_version,
        NEW.mode, cluster_actor, now(),
        jsonb_build_object(
            'status', NEW.status, 'kafkaVersion', NEW.kafka_version,
            'mode', NEW.mode, 'nodeIds', NEW.node_ids,
            'bootstrapServers', NEW.bootstrap_servers,
            'installDirectory', NEW.install_directory, 'configPath', NEW.config_path
        )
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Also remove the obsolete actor_user reference from the global audit mirror.
CREATE OR REPLACE FUNCTION sync_cluster_audit_to_global()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO kf_audit_logs (
        id,
        action,
        resource_type,
        resource_id,
        resource,
        event_category,
        status,
        details,
        origin,
        created_time,
        cluster_id,
        host_ip,
        created_by
    )
    VALUES (
        NEW.id,
        NEW.action,
        COALESCE(NULLIF(NEW.resource_type, ''), 'CLUSTER'),
        NEW.cluster_id::text,
        COALESCE(NULLIF(NEW.cluster_name, ''), NEW.cluster_id::text),
        'CLUSTER',
        CASE
            WHEN upper(COALESCE(NEW.status, '')) = 'FAILED'
                THEN 'FAILED'
            ELSE 'SUCCESS'
        END,
        NEW.details,
        COALESCE(NULLIF(NEW.origin, ''), 'MANAGEMENT_SERVER'),
        NEW.created_at,
        NEW.cluster_id,
        NULLIF(NEW.bootstrap_ip, ''),
        COALESCE(NULLIF(NEW.created_by, ''), 'system')
    )
    ON CONFLICT (id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
