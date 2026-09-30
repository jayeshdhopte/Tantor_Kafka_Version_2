UPDATE kf_clusters
SET jmx_exporter_port = 7071
WHERE jmx_exporter_port IS NULL
   OR jmx_exporter_port < 1024
   OR jmx_exporter_port > 65535;

UPDATE kf_cluster_services
SET jmx_exporter_port = 7071
WHERE jmx_exporter_port IS NULL
   OR jmx_exporter_port < 1024
   OR jmx_exporter_port > 65535;

UPDATE kf_external_cluster_nodes
SET jmx_exporter_port = NULL
WHERE jmx_exporter_port < 1024
   OR jmx_exporter_port > 65535;
