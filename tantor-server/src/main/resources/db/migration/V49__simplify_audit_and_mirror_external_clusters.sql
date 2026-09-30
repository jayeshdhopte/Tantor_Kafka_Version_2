-- V49: Keep the global audit table action-only, reduce deployment noise, and mirror external clusters/nodes.

ALTER TABLE kf_clusters
    ADD COLUMN IF NOT EXISTS "user" VARCHAR(128),
    ADD COLUMN IF NOT EXISTS role VARCHAR(50),
    ADD COLUMN IF NOT EXISTS config_path VARCHAR(1024);

DROP TRIGGER IF EXISTS trg_artifact_audit_to_global ON kf_artifact_audit_log;
CREATE OR REPLACE FUNCTION sync_artifact_audit_to_global()
RETURNS TRIGGER AS $$
DECLARE
    artifact_text TEXT;
BEGIN
    artifact_text := NULLIF(NEW.artifact_id::text, '');

    INSERT INTO kf_audit_logs (
        id, action, resource_type, resource_id, resource, user_name, event_category,
        status, details, origin, created_time, host_ip, host_name, created_by
    )
    VALUES (
        NEW.id,
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
        COALESCE(NULLIF(NEW.created_by, ''), NULLIF(NEW.user_name, ''), 'system')
    )
    ON CONFLICT (id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_artifact_audit_to_global
AFTER INSERT ON kf_artifact_audit_log
FOR EACH ROW EXECUTE PROCEDURE sync_artifact_audit_to_global();

CREATE OR REPLACE FUNCTION sync_host_to_global_audit()
RETURNS TRIGGER AS $$
DECLARE
    host_action VARCHAR(100);
    host_status VARCHAR(50);
    host_actor VARCHAR(255);
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
    host_actor := COALESCE(NULLIF(NEW.agent_name, ''), NULLIF(NEW.user_name, ''), NULLIF(NEW.hostname, ''), 'system');
    IF NULLIF(NEW.host_ip, '') IS NOT NULL AND host_actor NOT LIKE '%(%' THEN
        host_actor := host_actor || ' (' || NEW.host_ip || ')';
    END IF;

    INSERT INTO kf_audit_logs (
        action, resource_type, resource_id, resource, user_name, event_category,
        status, origin, created_time, host_id, host_ip, host_name, cluster_id, created_by, details
    )
    VALUES (
        host_action,
        'HOST',
        NEW.id,
        COALESCE(NULLIF(NEW.hostname, ''), NEW.id),
        host_actor,
        'AGENT',
        host_status,
        'AGENT',
        now(),
        NEW.id,
        NULLIF(NEW.host_ip, ''),
        NULLIF(NEW.hostname, ''),
        NEW.cluster_id,
        host_actor,
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
        user_name, created_by, created_at, log_path, details
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

DROP TRIGGER IF EXISTS trg_cluster_to_cluster_audit ON kf_clusters;
CREATE TRIGGER trg_cluster_to_cluster_audit
AFTER INSERT OR UPDATE OF status, cluster_name, bootstrap_servers, node_ids ON kf_clusters
FOR EACH ROW EXECUTE PROCEDURE sync_cluster_to_cluster_audit();

DROP TRIGGER IF EXISTS trg_cluster_audit_to_global ON kf_cluster_audit_log;
CREATE OR REPLACE FUNCTION sync_cluster_audit_to_global()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO kf_audit_logs (
        id, action, resource_type, resource_id, resource, user_name, event_category,
        status, details, origin, created_time, cluster_id, host_ip, created_by
    )
    VALUES (
        NEW.id,
        NEW.action,
        COALESCE(NULLIF(NEW.resource_type, ''), 'CLUSTER'),
        NEW.cluster_id::text,
        COALESCE(NULLIF(NEW.cluster_name, ''), NEW.cluster_id::text),
        COALESCE(NULLIF(NEW.user_name, ''), NULLIF(NEW.actor_user, ''), NULLIF(NEW.created_by, ''), 'system'),
        'CLUSTER',
        CASE WHEN upper(COALESCE(NEW.status, '')) = 'FAILED' THEN 'FAILED' ELSE 'SUCCESS' END,
        NEW.details,
        COALESCE(NULLIF(NEW.origin, ''), 'MANAGEMENT_SERVER'),
        NEW.created_at,
        NEW.cluster_id,
        NULLIF(NEW.bootstrap_ip, ''),
        COALESCE(NULLIF(NEW.created_by, ''), NULLIF(NEW.user_name, ''), 'system')
    )
    ON CONFLICT (id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_cluster_audit_to_global
AFTER INSERT ON kf_cluster_audit_log
FOR EACH ROW EXECUTE PROCEDURE sync_cluster_audit_to_global();

INSERT INTO kf_clusters (
    id, cluster_name, origin_type, kafka_cluster_id, install_directory, log_directory,
    node_ids, created_by, updated_by, "user", kafka_version, mode, environment,
    bootstrap_servers, external_broker_hosts_json, config_json, created_at, updated_at, status
)
SELECT
    ec.id,
    COALESCE(NULLIF(ec.cluster_name, ''), 'External cluster ' || left(ec.id::text, 8)),
    'EXTERNAL',
    NULLIF(ec.kafka_cluster_id, ''),
    NULLIF(ec.install_path, ''),
    NULLIF(ec.log_dirs, ''),
    COALESCE((
        SELECT jsonb_agg(node_id ORDER BY node_id)
        FROM (
            SELECT DISTINCT ecn.node_id
            FROM kf_external_cluster_nodes ecn
            WHERE ecn.cluster_id = ec.id AND ecn.node_id IS NOT NULL
        ) nodes
    ), '[]'::jsonb),
    COALESCE(NULLIF(ec.created_by, ''), 'system'),
    COALESCE(NULLIF(ec.updated_by, ''), NULLIF(ec.created_by, ''), 'system'),
    COALESCE(NULLIF(ec.created_by, ''), 'system'),
    COALESCE(NULLIF(NULLIF(ec.kafka_version, ''), 'null'), 'Unknown'),
    'EXTERNAL',
    COALESCE(NULLIF(ec.environment, ''), 'unknown'),
    ec.bootstrap_servers,
    COALESCE((
        SELECT jsonb_agg(jsonb_build_object(
            'host', ecn.host,
            'nodeId', ecn.node_id,
            'port', ecn.port,
            'isBroker', ecn.is_broker,
            'isController', ecn.is_controller
        ))::text
        FROM kf_external_cluster_nodes ecn
        WHERE ecn.cluster_id = ec.id
    ), '[]'),
    jsonb_build_object(
        'managementMode', 'BOOTSTRAP_ONLY',
        'kafkaMode', ec.kafka_mode,
        'security', ec.security,
        'listeners', ec.listeners,
        'advertisedListeners', ec.advertised_listeners,
        'processRoles', ec.process_roles,
        'brokerCount', ec.broker_count,
        'memoryUsedMb', ec.memory_used_mb,
        'memoryTotalMb', ec.memory_total_mb,
        'diskUsedGb', ec.disk_used_gb,
        'diskTotalGb', ec.disk_total_gb,
        'cpuUsagePct', ec.cpu_usage_pct,
        'running', ec.is_running
    )::text,
    COALESCE(ec.created_at, now()),
    COALESCE(ec.updated_at, now()),
    COALESCE(NULLIF(ec.status, ''), 'SUCCESS')
FROM kf_external_clusters ec
WHERE COALESCE(ec.status, '') <> 'DELETED'
ON CONFLICT (id) DO UPDATE SET
    cluster_name = EXCLUDED.cluster_name,
    origin_type = 'EXTERNAL',
    kafka_cluster_id = EXCLUDED.kafka_cluster_id,
    install_directory = EXCLUDED.install_directory,
    log_directory = EXCLUDED.log_directory,
    node_ids = EXCLUDED.node_ids,
    kafka_version = EXCLUDED.kafka_version,
    mode = 'EXTERNAL',
    environment = EXCLUDED.environment,
    bootstrap_servers = EXCLUDED.bootstrap_servers,
    external_broker_hosts_json = EXCLUDED.external_broker_hosts_json,
    config_json = EXCLUDED.config_json,
    updated_at = now(),
    status = EXCLUDED.status;

INSERT INTO kf_nodes (
    cluster_id, cluster_unique_id, node_id, host_id, host_name, host_ip, role, status,
    mode, env, kafka_version, installation_dir_path, config_path, data_path, log_path,
    service_config_json, created_at, updated_at
)
SELECT
    ec.id,
    ec.kafka_cluster_id,
    ecn.node_id,
    COALESCE(NULLIF(ecn.host, ''), 'external-' || COALESCE(ecn.node_id::text, ecn.id::text)),
    ecn.host,
    ecn.host,
    CASE
        WHEN COALESCE(ecn.is_broker, FALSE) AND COALESCE(ecn.is_controller, FALSE) THEN 'broker_controller'
        WHEN COALESCE(ecn.is_broker, FALSE) THEN 'broker'
        WHEN COALESCE(ecn.is_controller, FALSE) THEN 'controller'
        ELSE 'external'
    END,
    CASE WHEN ecn.last_seen IS NULL THEN 'BOOTSTRAP_CONNECTED' ELSE 'ONLINE' END,
    'EXTERNAL',
    ec.environment,
    COALESCE(NULLIF(NULLIF(ec.kafka_version, ''), 'null'), 'Unknown'),
    ecn.install_dir,
    ecn.config_file,
    ecn.data_dirs,
    COALESCE(ecn.log_dirs, ec.log_dirs),
    jsonb_build_object(
        'port', ecn.port,
        'isBroker', ecn.is_broker,
        'isController', ecn.is_controller,
        'memoryUsedMb', ecn.memory_used_mb,
        'memoryTotalMb', ecn.memory_total_mb,
        'diskUsedGb', ecn.disk_used_gb,
        'diskTotalGb', ecn.disk_total_gb,
        'lastSeen', ecn.last_seen
    )::text,
    now(),
    now()
FROM kf_external_cluster_nodes ecn
JOIN kf_external_clusters ec ON ec.id = ecn.cluster_id
WHERE COALESCE(ec.status, '') <> 'DELETED'
ON CONFLICT (cluster_id, host_id, role) DO UPDATE SET
    cluster_unique_id = EXCLUDED.cluster_unique_id,
    node_id = EXCLUDED.node_id,
    host_name = EXCLUDED.host_name,
    host_ip = EXCLUDED.host_ip,
    status = EXCLUDED.status,
    mode = EXCLUDED.mode,
    env = EXCLUDED.env,
    kafka_version = EXCLUDED.kafka_version,
    installation_dir_path = EXCLUDED.installation_dir_path,
    config_path = EXCLUDED.config_path,
    data_path = EXCLUDED.data_path,
    log_path = EXCLUDED.log_path,
    service_config_json = EXCLUDED.service_config_json,
    updated_at = now();

CREATE OR REPLACE FUNCTION sync_external_cluster_to_cluster()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        UPDATE kf_clusters SET status = 'DELETED', deleted_at = now() WHERE id = OLD.id;
        RETURN OLD;
    END IF;

    INSERT INTO kf_clusters (
        id, cluster_name, origin_type, kafka_cluster_id, install_directory, log_directory,
        node_ids, created_by, updated_by, "user", kafka_version, mode, environment,
        bootstrap_servers, config_json, created_at, updated_at, status
    )
    VALUES (
        NEW.id,
        COALESCE(NULLIF(NEW.cluster_name, ''), 'External cluster ' || left(NEW.id::text, 8)),
        'EXTERNAL',
        NULLIF(NEW.kafka_cluster_id, ''),
        NULLIF(NEW.install_path, ''),
        NULLIF(NEW.log_dirs, ''),
        '[]'::jsonb,
        COALESCE(NULLIF(NEW.created_by, ''), 'system'),
        COALESCE(NULLIF(NEW.updated_by, ''), NULLIF(NEW.created_by, ''), 'system'),
        COALESCE(NULLIF(NEW.created_by, ''), 'system'),
        COALESCE(NULLIF(NULLIF(NEW.kafka_version, ''), 'null'), 'Unknown'),
        'EXTERNAL',
        COALESCE(NULLIF(NEW.environment, ''), 'unknown'),
        NEW.bootstrap_servers,
        jsonb_build_object(
            'managementMode', 'BOOTSTRAP_ONLY',
            'kafkaMode', NEW.kafka_mode,
            'security', NEW.security,
            'listeners', NEW.listeners,
            'advertisedListeners', NEW.advertised_listeners,
            'processRoles', NEW.process_roles,
            'brokerCount', NEW.broker_count,
            'memoryUsedMb', NEW.memory_used_mb,
            'memoryTotalMb', NEW.memory_total_mb,
            'diskUsedGb', NEW.disk_used_gb,
            'diskTotalGb', NEW.disk_total_gb,
            'cpuUsagePct', NEW.cpu_usage_pct,
            'running', NEW.is_running
        )::text,
        COALESCE(NEW.created_at, now()),
        COALESCE(NEW.updated_at, now()),
        COALESCE(NULLIF(NEW.status, ''), 'SUCCESS')
    )
    ON CONFLICT (id) DO UPDATE SET
        cluster_name = EXCLUDED.cluster_name,
        origin_type = 'EXTERNAL',
        kafka_cluster_id = EXCLUDED.kafka_cluster_id,
        install_directory = EXCLUDED.install_directory,
        log_directory = EXCLUDED.log_directory,
        kafka_version = EXCLUDED.kafka_version,
        mode = 'EXTERNAL',
        environment = EXCLUDED.environment,
        bootstrap_servers = EXCLUDED.bootstrap_servers,
        config_json = EXCLUDED.config_json,
        updated_at = now(),
        status = EXCLUDED.status;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_external_cluster_to_cluster ON kf_external_clusters;
CREATE TRIGGER trg_external_cluster_to_cluster
AFTER INSERT OR UPDATE OR DELETE ON kf_external_clusters
FOR EACH ROW EXECUTE PROCEDURE sync_external_cluster_to_cluster();

CREATE OR REPLACE FUNCTION sync_external_node_to_node()
RETURNS TRIGGER AS $$
DECLARE
    ec RECORD;
    node_role VARCHAR(100);
BEGIN
    IF TG_OP = 'DELETE' THEN
        DELETE FROM kf_nodes
        WHERE cluster_id = OLD.cluster_id
          AND host_id = COALESCE(NULLIF(OLD.host, ''), 'external-' || COALESCE(OLD.node_id::text, OLD.id::text));
        RETURN OLD;
    END IF;

    SELECT * INTO ec FROM kf_external_clusters WHERE id = NEW.cluster_id;
    node_role := CASE
        WHEN COALESCE(NEW.is_broker, FALSE) AND COALESCE(NEW.is_controller, FALSE) THEN 'broker_controller'
        WHEN COALESCE(NEW.is_broker, FALSE) THEN 'broker'
        WHEN COALESCE(NEW.is_controller, FALSE) THEN 'controller'
        ELSE 'external'
    END;

    INSERT INTO kf_nodes (
        cluster_id, cluster_unique_id, node_id, host_id, host_name, host_ip, role, status,
        mode, env, kafka_version, installation_dir_path, config_path, data_path, log_path,
        service_config_json, created_at, updated_at
    )
    VALUES (
        NEW.cluster_id,
        ec.kafka_cluster_id,
        NEW.node_id,
        COALESCE(NULLIF(NEW.host, ''), 'external-' || COALESCE(NEW.node_id::text, NEW.id::text)),
        NEW.host,
        NEW.host,
        node_role,
        CASE WHEN NEW.last_seen IS NULL THEN 'BOOTSTRAP_CONNECTED' ELSE 'ONLINE' END,
        'EXTERNAL',
        ec.environment,
        COALESCE(NULLIF(NULLIF(ec.kafka_version, ''), 'null'), 'Unknown'),
        NEW.install_dir,
        NEW.config_file,
        NEW.data_dirs,
        COALESCE(NEW.log_dirs, ec.log_dirs),
        jsonb_build_object(
            'port', NEW.port,
            'isBroker', NEW.is_broker,
            'isController', NEW.is_controller,
            'memoryUsedMb', NEW.memory_used_mb,
            'memoryTotalMb', NEW.memory_total_mb,
            'diskUsedGb', NEW.disk_used_gb,
            'diskTotalGb', NEW.disk_total_gb,
            'lastSeen', NEW.last_seen
        )::text,
        now(),
        now()
    )
    ON CONFLICT (cluster_id, host_id, role) DO UPDATE SET
        cluster_unique_id = EXCLUDED.cluster_unique_id,
        node_id = EXCLUDED.node_id,
        host_name = EXCLUDED.host_name,
        host_ip = EXCLUDED.host_ip,
        status = EXCLUDED.status,
        mode = EXCLUDED.mode,
        env = EXCLUDED.env,
        kafka_version = EXCLUDED.kafka_version,
        installation_dir_path = EXCLUDED.installation_dir_path,
        config_path = EXCLUDED.config_path,
        data_path = EXCLUDED.data_path,
        log_path = EXCLUDED.log_path,
        service_config_json = EXCLUDED.service_config_json,
        updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_external_node_to_node ON kf_external_cluster_nodes;
CREATE TRIGGER trg_external_node_to_node
AFTER INSERT OR UPDATE OR DELETE ON kf_external_cluster_nodes
FOR EACH ROW EXECUTE PROCEDURE sync_external_node_to_node();

ALTER TABLE kf_audit_logs
    DROP COLUMN IF EXISTS event,
    DROP COLUMN IF EXISTS artifact_id;
