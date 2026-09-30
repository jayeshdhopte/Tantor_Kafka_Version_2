-- ZooKeeper's active controller is a broker leadership designation, not a
-- KRaft controller process role. Correct previously persisted combined rows.
UPDATE kf_external_cluster_nodes node
SET is_controller = FALSE
FROM kf_external_clusters cluster
WHERE node.cluster_id = cluster.id
  AND node.is_broker = TRUE
  AND node.is_controller = TRUE
  AND UPPER(REPLACE(COALESCE(cluster.kafka_mode, ''), ' ', '')) IN ('ZOOKEEPER', 'ZK');
