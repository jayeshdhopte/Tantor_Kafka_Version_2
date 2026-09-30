ALTER TABLE kf_external_cluster_nodes
    ADD COLUMN IF NOT EXISTS messages_in_per_sec DOUBLE PRECISION,
    ADD COLUMN IF NOT EXISTS bytes_in_per_sec DOUBLE PRECISION;

COMMENT ON COLUMN kf_external_cluster_nodes.messages_in_per_sec IS
    'Latest broker message ingestion rate reported by the external discovery agent.';
COMMENT ON COLUMN kf_external_cluster_nodes.bytes_in_per_sec IS
    'Latest broker byte ingestion rate reported by the external discovery agent.';
