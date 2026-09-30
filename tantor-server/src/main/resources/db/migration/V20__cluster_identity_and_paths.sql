ALTER TABLE clusters RENAME COLUMN name TO cluster_name;
ALTER TABLE clusters ADD COLUMN origin_type VARCHAR(20) NOT NULL DEFAULT 'INTERNAL';
ALTER TABLE clusters ADD COLUMN kafka_cluster_id VARCHAR(255);
ALTER TABLE clusters ADD COLUMN install_directory VARCHAR(1024);
ALTER TABLE clusters ADD COLUMN config_directory VARCHAR(1024);
ALTER TABLE clusters ADD COLUMN data_directory VARCHAR(1024);
ALTER TABLE clusters ADD COLUMN log_directory VARCHAR(1024);

UPDATE clusters
SET origin_type = CASE WHEN UPPER(COALESCE(mode, '')) = 'EXTERNAL' THEN 'EXTERNAL' ELSE 'INTERNAL' END;

UPDATE clusters
SET kafka_cluster_id = NULLIF(config_json::jsonb ->> 'kafkaClusterId', '')
WHERE UPPER(COALESCE(mode, '')) = 'EXTERNAL'
  AND config_json IS NOT NULL
  AND config_json ~ '^\\s*\\{';

CREATE INDEX idx_clusters_origin_type ON clusters(origin_type);
CREATE UNIQUE INDEX uq_clusters_kafka_cluster_id
    ON clusters(kafka_cluster_id) WHERE kafka_cluster_id IS NOT NULL AND kafka_cluster_id <> '';

