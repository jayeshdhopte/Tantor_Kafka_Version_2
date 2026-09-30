ALTER TABLE kf_clusters
    ADD COLUMN IF NOT EXISTS monitoring_enabled BOOLEAN DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS kafka_exporter_host VARCHAR(255),
    ADD COLUMN IF NOT EXISTS kafka_exporter_port INTEGER,
    ADD COLUMN IF NOT EXISTS jmx_enabled BOOLEAN DEFAULT TRUE,
    ADD COLUMN IF NOT EXISTS jmx_exporter_port INTEGER DEFAULT 9404,
    ADD COLUMN IF NOT EXISTS node_exporter_enabled BOOLEAN DEFAULT FALSE,
    ADD COLUMN IF NOT EXISTS node_exporter_port INTEGER DEFAULT 9100;

ALTER TABLE kf_cluster_services
    ADD COLUMN IF NOT EXISTS jmx_exporter_port INTEGER,
    ADD COLUMN IF NOT EXISTS node_exporter_port INTEGER;

ALTER TABLE kf_external_cluster_nodes
    ADD COLUMN IF NOT EXISTS jmx_exporter_port INTEGER;
