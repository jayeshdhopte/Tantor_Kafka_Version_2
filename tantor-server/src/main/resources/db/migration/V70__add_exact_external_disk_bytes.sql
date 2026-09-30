ALTER TABLE kf_external_cluster_nodes
    ADD COLUMN IF NOT EXISTS disk_used_bytes BIGINT,
    ADD COLUMN IF NOT EXISTS disk_total_bytes BIGINT;

COMMENT ON COLUMN kf_external_cluster_nodes.disk_used_bytes IS
    'Exact OS filesystem bytes used on the discovery-agent node.';
COMMENT ON COLUMN kf_external_cluster_nodes.disk_total_bytes IS
    'Exact OS filesystem capacity in bytes on the discovery-agent node.';
