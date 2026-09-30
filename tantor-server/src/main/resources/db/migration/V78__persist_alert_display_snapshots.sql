-- Keep alert history understandable after its cluster or host is deleted.
ALTER TABLE kf_alerts
    ADD COLUMN IF NOT EXISTS cluster_name_snapshot VARCHAR(255),
    ADD COLUMN IF NOT EXISTS kafka_cluster_id_snapshot VARCHAR(255),
    ADD COLUMN IF NOT EXISTS host_ip_snapshot TEXT;

UPDATE kf_alerts alert
SET cluster_name_snapshot = COALESCE(alert.cluster_name_snapshot, NULLIF(c.cluster_name, '')),
    kafka_cluster_id_snapshot = COALESCE(alert.kafka_cluster_id_snapshot, NULLIF(c.kafka_cluster_id, ''))
FROM kf_clusters c
WHERE alert.cluster_id = c.id
  AND (alert.cluster_name_snapshot IS NULL OR alert.kafka_cluster_id_snapshot IS NULL);

UPDATE kf_alerts alert
SET host_ip_snapshot = COALESCE(alert.host_ip_snapshot, NULLIF(h.host_ip, ''))
FROM kf_hosts h
WHERE alert.host_id = h.id
  AND alert.host_ip_snapshot IS NULL;

UPDATE kf_alerts
SET host_ip_snapshot = affected_ips
WHERE host_ip_snapshot IS NULL
  AND NULLIF(affected_ips, '') IS NOT NULL;
