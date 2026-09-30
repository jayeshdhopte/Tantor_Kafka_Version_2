-- Repair databases where an earlier form of V44 was recorded without creating kf_nodes.
-- This version deliberately runs after V48 and before V49, which mirrors external nodes.

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
