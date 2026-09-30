-- V32__create_discovery_agents_table.sql
CREATE TABLE IF NOT EXISTS kf_discovery_agents (
    id VARCHAR(255) PRIMARY KEY,
    agent_name VARCHAR(255),
    hostname VARCHAR(255),
    ip_addresses JSONB,
    status VARCHAR(50),
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    version VARCHAR(50),
    cluster_id UUID,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_discovery_agents_cluster ON kf_discovery_agents(cluster_id);
