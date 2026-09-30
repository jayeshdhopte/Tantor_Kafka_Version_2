-- 1. Backfill data to ensure no data loss (Disable immutable triggers first)
ALTER TABLE kf_audit_logs DISABLE TRIGGER ALL;
ALTER TABLE kf_cluster_audit_log DISABLE TRIGGER ALL;

UPDATE kf_audit_logs SET created_by = user_name WHERE created_by IS NULL OR created_by = '';

DO $$
BEGIN
    IF EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='kf_cluster_audit_log' AND column_name='user_name') THEN
        EXECUTE 'UPDATE kf_cluster_audit_log SET created_by = user_name WHERE created_by IS NULL OR created_by = ''''';
    END IF;
END $$;

ALTER TABLE kf_audit_logs ENABLE TRIGGER ALL;
ALTER TABLE kf_cluster_audit_log ENABLE TRIGGER ALL;

-- 2. Update functions to remove user_name references
CREATE OR REPLACE FUNCTION sync_cluster_to_cluster_audit()
RETURNS TRIGGER AS $$
DECLARE
    cluster_action VARCHAR(100);
    bootstrap_ip VARCHAR(100);
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

    INSERT INTO kf_cluster_audit_log (
        cluster_id, action, event, severity, title, description, status, origin, resource,
        resource_type, cluster_name, bootstrap_ip, env, kafka_version, mode, actor_user,
        created_by, created_at, log_path, details
    )
    VALUES (
        NEW.id,
        cluster_action,
        cluster_action,
        CASE WHEN upper(COALESCE(NEW.status, '')) = 'FAILED' THEN 'ERROR' ELSE 'INFO' END,
        replace(initcap(replace(lower(cluster_action), '_', ' ')), ' ', ' '),
        'Cluster ' || COALESCE(NEW.cluster_name, NEW.id::text) || ' status is ' || COALESCE(NEW.status, 'UNKNOWN'),
        CASE WHEN upper(COALESCE(NEW.status, '')) = 'FAILED' THEN 'FAILED' ELSE 'SUCCESS' END,
        'MANAGEMENT_SERVER',
        COALESCE(NULLIF(NEW.cluster_name, ''), NEW.id::text),
        'CLUSTER',
        NEW.cluster_name,
        NULLIF(bootstrap_ip, ''),
        NEW.environment,
        NEW.kafka_version,
        NEW.mode,
        COALESCE(NULLIF(NEW."user", ''), NULLIF(NEW.created_by, ''), 'system'),
        COALESCE(NULLIF(NEW.created_by, ''), 'system'),
        now(),
        COALESCE(NULLIF(NEW.log_directory, ''), NULLIF(NEW.config_path, '')),
        jsonb_build_object(
            'status', NEW.status,
            'kafkaVersion', NEW.kafka_version,
            'mode', NEW.mode,
            'nodeIds', NEW.node_ids,
            'bootstrapServers', NEW.bootstrap_servers,
            'installDirectory', NEW.install_directory,
            'configPath', NEW.config_path
        )
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sync_cluster_audit_to_global()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO kf_audit_logs (
        id, action, resource_type, resource_id, resource, event_category,
        status, details, origin, created_time, cluster_id, host_ip, created_by
    )
    VALUES (
        NEW.id,
        NEW.action,
        COALESCE(NULLIF(NEW.resource_type, ''), 'CLUSTER'),
        NEW.cluster_id::text,
        COALESCE(NULLIF(NEW.cluster_name, ''), NEW.cluster_id::text),
        'CLUSTER',
        CASE WHEN upper(COALESCE(NEW.status, '')) = 'FAILED' THEN 'FAILED' ELSE 'SUCCESS' END,
        NEW.details,
        COALESCE(NULLIF(NEW.origin, ''), 'MANAGEMENT_SERVER'),
        NEW.created_at,
        NEW.cluster_id,
        NULLIF(NEW.bootstrap_ip, ''),
        COALESCE(NULLIF(NEW.created_by, ''), NULLIF(NEW.actor_user, ''), 'system')
    )
    ON CONFLICT (id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- 3. Drop the redundant user_name columns
ALTER TABLE kf_audit_logs DROP COLUMN IF EXISTS user_name;
ALTER TABLE kf_cluster_audit_log DROP COLUMN IF EXISTS user_name;
