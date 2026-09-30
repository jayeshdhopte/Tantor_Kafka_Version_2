-- Recreate clusters view without deprecated user column.

DROP VIEW IF EXISTS clusters;

ALTER TABLE kf_clusters
    DROP COLUMN IF EXISTS "user";

CREATE VIEW clusters AS
SELECT
    kf_clusters.id,
    kf_clusters.cluster_name,
    kf_clusters.created_at,
    kf_clusters.updated_at,
    kf_clusters.kafka_version,
    kf_clusters.mode,
    kf_clusters.environment,
    kf_clusters.config_json,
    kf_clusters.bootstrap_servers,
    kf_clusters.external_broker_hosts_json,
    kf_clusters.status,
    kf_clusters.deleted_at,
    kf_clusters.origin_type,
    kf_clusters.kafka_cluster_id,
    kf_clusters.install_directory,
    kf_clusters.config_directory,
    kf_clusters.data_directory,
    kf_clusters.log_directory,
    kf_clusters.node_ids,
    kf_clusters.created_by,
    kf_clusters.updated_by,
    kf_clusters.role,
    kf_clusters.config_path
FROM kf_clusters;
