-- V2__update_clusters_schema.sql

-- Update clusters table to match Cluster.java
ALTER TABLE clusters DROP COLUMN IF EXISTS cluster_type;
ALTER TABLE clusters DROP COLUMN IF EXISTS status;
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS kafka_version VARCHAR(50) NOT NULL DEFAULT 'unknown';
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS mode VARCHAR(50);
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS environment VARCHAR(50);
ALTER TABLE clusters ADD COLUMN IF NOT EXISTS config_json TEXT;

-- Rename services to cluster_services and update columns to match ClusterServiceAssignment.java
ALTER TABLE IF EXISTS services RENAME TO cluster_services;
ALTER TABLE cluster_services DROP COLUMN IF EXISTS service_type;
ALTER TABLE cluster_services DROP COLUMN IF EXISTS status;
ALTER TABLE cluster_services DROP COLUMN IF EXISTS config_overrides;
ALTER TABLE cluster_services DROP COLUMN IF EXISTS created_at;
ALTER TABLE cluster_services DROP COLUMN IF EXISTS updated_at;
ALTER TABLE cluster_services ADD COLUMN IF NOT EXISTS role VARCHAR(50) NOT NULL DEFAULT 'broker';

-- Change cluster_services id from UUID to VARCHAR(36)
ALTER TABLE cluster_services ALTER COLUMN id TYPE VARCHAR(36) USING id::VARCHAR;
