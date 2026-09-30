ALTER TABLE kf_cluster_services
    ADD COLUMN IF NOT EXISTS status VARCHAR(32),
    ADD COLUMN IF NOT EXISTS last_error TEXT;

COMMENT ON COLUMN kf_cluster_services.status IS
    'Optional lifecycle status for independently deployed services such as Schema Registry.';

COMMENT ON COLUMN kf_cluster_services.last_error IS
    'Most recent deployment or health-check error for the assigned service.';
