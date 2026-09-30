CREATE TABLE IF NOT EXISTS kf_external_cluster_nodes (
    id UUID PRIMARY KEY,
    cluster_id UUID NOT NULL REFERENCES kf_external_clusters(id) ON DELETE CASCADE,
    host VARCHAR(255) NOT NULL,
    node_id INT,
    is_broker BOOLEAN,
    is_controller BOOLEAN,
    cpu_usage_pct DOUBLE PRECISION,
    memory_used_mb BIGINT,
    memory_total_mb BIGINT,
    disk_used_gb BIGINT,
    disk_total_gb BIGINT,
    last_seen TIMESTAMPTZ,
    UNIQUE(cluster_id, node_id)
);

ALTER TABLE kf_external_cluster_nodes ADD COLUMN IF NOT EXISTS port INTEGER;
ALTER TABLE kf_external_cluster_nodes ADD COLUMN IF NOT EXISTS install_dir VARCHAR(512);
ALTER TABLE kf_external_cluster_nodes ADD COLUMN IF NOT EXISTS config_file VARCHAR(512);
ALTER TABLE kf_external_cluster_nodes ADD COLUMN IF NOT EXISTS data_dirs TEXT;
ALTER TABLE kf_external_cluster_nodes ADD COLUMN IF NOT EXISTS log_dirs TEXT;
