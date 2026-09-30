-- V46: Add is_default column to kf_data_service_connections
-- Allows one default Schema Registry and one default Kafka Connect connection per cluster.
-- Partial unique index ensures only one default per (cluster, service_type) at a time.

ALTER TABLE kf_data_service_connections
    ADD COLUMN IF NOT EXISTS is_default BOOLEAN NOT NULL DEFAULT FALSE;

-- Only one default per (cluster_id, service_type) may exist while active
CREATE UNIQUE INDEX IF NOT EXISTS uidx_dsc_default
    ON kf_data_service_connections (cluster_id, service_type)
    WHERE is_default = TRUE AND is_active = TRUE;

COMMENT ON COLUMN kf_data_service_connections.is_default IS
    'When true, this connection is auto-selected when no connectionId is provided in API requests.';
