ALTER TABLE kf_clusters
    ALTER COLUMN jmx_exporter_port SET DEFAULT 7071;

UPDATE kf_clusters
SET jmx_exporter_port = 7071
WHERE jmx_exporter_port IS NULL
   OR jmx_exporter_port = 9404;

UPDATE kf_cluster_services
SET jmx_exporter_port = 7071
WHERE jmx_exporter_port IS NULL
   OR jmx_exporter_port = 9404;
