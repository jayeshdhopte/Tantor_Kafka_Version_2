-- Controller-only Kafka JVMs use a dedicated Prometheus JMX exporter endpoint.
-- Combined broker_controller JVMs intentionally retain the broker endpoint (7071).
UPDATE kf_cluster_services
SET jmx_exporter_port = 7072
WHERE LOWER(role) = 'controller'
  AND (jmx_exporter_port IS NULL OR jmx_exporter_port = 7071);
