-- V44: Wire artifact, host, cluster, and node data into the universal audit model.

CREATE EXTENSION IF NOT EXISTS pgcrypto;

ALTER TABLE kf_audit_logs
    ADD COLUMN IF NOT EXISTS event VARCHAR(255),
    ADD COLUMN IF NOT EXISTS resource VARCHAR(255),
    ADD COLUMN IF NOT EXISTS host_id VARCHAR(255),
    ADD COLUMN IF NOT EXISTS host_ip VARCHAR(100),
    ADD COLUMN IF NOT EXISTS host_name VARCHAR(255),
    ADD COLUMN IF NOT EXISTS artifact_id UUID,
    ADD COLUMN IF NOT EXISTS user_id VARCHAR(255),
    ADD COLUMN IF NOT EXISTS created_by VARCHAR(128);

ALTER TABLE kf_audit_logs
    ALTER COLUMN id SET DEFAULT gen_random_uuid();

ALTER TABLE kf_artifact
    ADD COLUMN IF NOT EXISTS full_file_path VARCHAR(2048);

ALTER TABLE kf_hosts
    ADD COLUMN IF NOT EXISTS user_name VARCHAR(128);

ALTER TABLE kf_clusters
    ADD COLUMN IF NOT EXISTS config_path VARCHAR(1024);

ALTER TABLE kf_artifact_audit_log
    ADD COLUMN IF NOT EXISTS full_file_path VARCHAR(2048),
    ADD COLUMN IF NOT EXISTS path_of_tar VARCHAR(1024),
    ADD COLUMN IF NOT EXISTS version_no VARCHAR(80),
    ADD COLUMN IF NOT EXISTS checksum VARCHAR(128),
    ADD COLUMN IF NOT EXISTS host_ip VARCHAR(100),
    ADD COLUMN IF NOT EXISTS host_name VARCHAR(255);

ALTER TABLE kf_artifact_audit_log
    ALTER COLUMN artifact_id TYPE VARCHAR(255) USING artifact_id::text;

ALTER TABLE kf_artifact_audit_log DISABLE TRIGGER USER;

UPDATE kf_artifact_audit_log aal
SET
    full_file_path = COALESCE(aal.full_file_path, a.full_file_path),
    path_of_tar = COALESCE(aal.path_of_tar, a.path_of_tar),
    version_no = COALESCE(aal.version_no, a.version_no),
    checksum = COALESCE(aal.checksum, a.checksum),
    host_ip = COALESCE(NULLIF(aal.host_ip, ''), NULLIF(a.host_ip, '')),
    host_name = COALESCE(NULLIF(aal.host_name, ''), NULLIF(a.hostname, '')),
    user_name = COALESCE(NULLIF(aal.user_name, ''), NULLIF(a.created_by, ''), aal.user_name)
FROM kf_artifact a
WHERE aal.artifact_id::text = a.id::text;

ALTER TABLE kf_artifact_audit_log ENABLE TRIGGER USER;

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
        COALESCE(NULLIF(NEW.full_file_path, ''), NULLIF(NEW.path_of_tar, ''), NULLIF(NEW.host_name, ''), NEW.artifact_id::text),
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

DROP TRIGGER IF EXISTS trg_host_to_global_audit ON kf_hosts;
CREATE TRIGGER trg_host_to_global_audit
AFTER INSERT OR UPDATE OF status, agent_status, host_ip, hostname, cluster_id, removed, action ON kf_hosts
FOR EACH ROW EXECUTE PROCEDURE sync_host_to_global_audit();

CREATE TABLE IF NOT EXISTS kf_cluster_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_id UUID NOT NULL,
    action VARCHAR(100) NOT NULL,
    event VARCHAR(255),
    status VARCHAR(50),
    origin VARCHAR(100),
    resource VARCHAR(100),
    resource_type VARCHAR(50) DEFAULT 'CLUSTER',
    cluster_name VARCHAR(255),
    bootstrap_ip VARCHAR(100),
    env VARCHAR(50),
    kafka_version VARCHAR(100),
    mode VARCHAR(100),
    user_id VARCHAR(255),
    actor_user VARCHAR(128),
    created_by VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    details JSONB
);

ALTER TABLE kf_cluster_audit_log
    ADD COLUMN IF NOT EXISTS severity VARCHAR(50),
    ADD COLUMN IF NOT EXISTS title VARCHAR(255),
    ADD COLUMN IF NOT EXISTS description TEXT,
    ADD COLUMN IF NOT EXISTS log_path VARCHAR(1024),
    ADD COLUMN IF NOT EXISTS env VARCHAR(50),
    ADD COLUMN IF NOT EXISTS user_name VARCHAR(128),
    ADD COLUMN IF NOT EXISTS created_by VARCHAR(128),
    ADD COLUMN IF NOT EXISTS resource_type VARCHAR(50) DEFAULT 'CLUSTER';

CREATE OR REPLACE FUNCTION sync_cluster_to_cluster_audit()
RETURNS TRIGGER AS $$
DECLARE
    cluster_action VARCHAR(100);
    bootstrap_ip VARCHAR(100);
BEGIN
    IF TG_OP = 'INSERT' THEN
        cluster_action := 'CLUSTER_CREATED';
    ELSIF NEW.status IS DISTINCT FROM OLD.status AND upper(COALESCE(NEW.status, '')) = 'SUCCESS' THEN
        cluster_action := 'CLUSTER_DEPLOYED';
    ELSIF NEW.status IS DISTINCT FROM OLD.status THEN
        cluster_action := 'CLUSTER_STATUS_CHANGED';
    ELSE
        cluster_action := 'CLUSTER_UPDATED';
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
        COALESCE(NULLIF(NEW.status, ''), 'UNKNOWN'),
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
        COALESCE(NEW.updated_at, NEW.created_at, now()),
        COALESCE(NULLIF(NEW.log_directory, ''), NULLIF(NEW.config_path, '')),
        jsonb_build_object(
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

CREATE OR REPLACE FUNCTION sync_cluster_audit_to_global()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO kf_audit_logs (
        id, action, event, resource_type, resource_id, resource, user_name, event_category,
        status, details, origin, created_time, cluster_id, host_ip, created_by
    )
    VALUES (
        NEW.id,
        NEW.action,
        COALESCE(NULLIF(NEW.event, ''), NULLIF(NEW.title, ''), NEW.action),
        COALESCE(NULLIF(NEW.resource_type, ''), 'CLUSTER'),
        NEW.cluster_id::text,
        COALESCE(NULLIF(NEW.cluster_name, ''), NEW.cluster_id::text),
        COALESCE(NULLIF(NEW.user_name, ''), NULLIF(NEW.actor_user, ''), NULLIF(NEW.created_by, ''), 'system'),
        'CLUSTER',
        COALESCE(NULLIF(NEW.status, ''), 'UNKNOWN'),
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

DROP TRIGGER IF EXISTS trg_cluster_audit_to_global ON kf_cluster_audit_log;
CREATE TRIGGER trg_cluster_audit_to_global
AFTER INSERT ON kf_cluster_audit_log
FOR EACH ROW EXECUTE PROCEDURE sync_cluster_audit_to_global();

CREATE TABLE IF NOT EXISTS kf_nodes (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_id UUID NOT NULL,
    cluster_unique_id VARCHAR(255),
    node_id INTEGER,
    host_id VARCHAR(255) NOT NULL,
    host_name VARCHAR(255),
    host_ip VARCHAR(100),
    role VARCHAR(100) NOT NULL,
    status VARCHAR(50),
    mode VARCHAR(100),
    env VARCHAR(50),
    kafka_version VARCHAR(100),
    installation_dir_path VARCHAR(1024),
    config_path VARCHAR(1024),
    data_path VARCHAR(1024),
    log_path VARCHAR(1024),
    service_config_json TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_kf_nodes_cluster_host_role UNIQUE (cluster_id, host_id, role)
);

CREATE OR REPLACE FUNCTION sync_cluster_service_to_node()
RETURNS TRIGGER AS $$
DECLARE
    c RECORD;
    h RECORD;
BEGIN
    IF TG_OP = 'DELETE' THEN
        DELETE FROM kf_nodes
        WHERE cluster_id = OLD.cluster_id AND host_id = OLD.host_id AND role = OLD.role;
        RETURN OLD;
    END IF;

    SELECT * INTO c FROM kf_clusters WHERE id = NEW.cluster_id;
    SELECT * INTO h FROM kf_hosts WHERE id = NEW.host_id;

    INSERT INTO kf_nodes (
        cluster_id, cluster_unique_id, node_id, host_id, host_name, host_ip, role, status,
        mode, env, kafka_version, installation_dir_path, config_path, data_path, log_path,
        service_config_json, created_at, updated_at
    )
    VALUES (
        NEW.cluster_id,
        c.kafka_cluster_id,
        NEW.node_id,
        NEW.host_id,
        COALESCE(NULLIF(h.hostname, ''), NEW.host_id),
        NULLIF(h.host_ip, ''),
        NEW.role,
        COALESCE(NULLIF(h.status, ''), NULLIF(c.status, ''), 'UNKNOWN'),
        c.mode,
        c.environment,
        c.kafka_version,
        c.install_directory,
        c.config_path,
        c.data_directory,
        c.log_directory,
        NEW.config_json,
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

DROP TRIGGER IF EXISTS trg_cluster_service_to_node ON kf_cluster_services;
CREATE TRIGGER trg_cluster_service_to_node
AFTER INSERT OR UPDATE OR DELETE ON kf_cluster_services
FOR EACH ROW EXECUTE PROCEDURE sync_cluster_service_to_node();

INSERT INTO kf_nodes (
    cluster_id, cluster_unique_id, node_id, host_id, host_name, host_ip, role, status,
    mode, env, kafka_version, installation_dir_path, config_path, data_path, log_path,
    service_config_json, created_at, updated_at
)
SELECT
    cs.cluster_id,
    c.kafka_cluster_id,
    cs.node_id,
    cs.host_id,
    COALESCE(NULLIF(h.hostname, ''), cs.host_id),
    NULLIF(h.host_ip, ''),
    cs.role,
    COALESCE(NULLIF(h.status, ''), NULLIF(c.status, ''), 'UNKNOWN'),
    c.mode,
    c.environment,
    c.kafka_version,
    c.install_directory,
    c.config_path,
    c.data_directory,
    c.log_directory,
    cs.config_json,
    now(),
    now()
FROM kf_cluster_services cs
JOIN kf_clusters c ON c.id = cs.cluster_id
LEFT JOIN kf_hosts h ON h.id = cs.host_id
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
