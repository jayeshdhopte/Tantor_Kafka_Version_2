CREATE TABLE IF NOT EXISTS kf_external_cluster_nodes (
    id UUID PRIMARY KEY,
    cluster_id UUID NOT NULL REFERENCES kf_external_clusters(id) ON DELETE CASCADE,
    host VARCHAR(255) NOT NULL,
    node_id INT,
    is_broker BOOLEAN,
    is_controller BOOLEAN,
    cpu_usage_pct DOUBLE PRECISION,
    memory_used_mb BIGINT,
    memory_total_mb BIGINT,
    disk_used_gb BIGINT,
    disk_total_gb BIGINT,
    last_seen TIMESTAMPTZ,
    UNIQUE(cluster_id, host)
);

-- Backfill from external_broker_hosts_json
INSERT INTO kf_external_cluster_nodes (
    id, cluster_id, host, node_id, is_broker, is_controller, 
    cpu_usage_pct, memory_used_mb, memory_total_mb, disk_used_gb, disk_total_gb, last_seen
)
SELECT 
    gen_random_uuid(),
    c.id as cluster_id,
    COALESCE(
        p.node->>'hostname', 
        p.node->>'host'
    ) as host,
    NULLIF(p.node->>'nodeId', 'null')::INT as node_id,
    CASE 
        WHEN p.node->>'role' = 'broker' THEN true
        WHEN p.node->>'role' = 'broker_controller' THEN true
        WHEN p.node->>'isBroker' IS NOT NULL THEN (p.node->>'isBroker')::BOOLEAN
        ELSE false
    END as is_broker,
    CASE 
        WHEN p.node->>'role' = 'controller' THEN true
        WHEN p.node->>'role' = 'broker_controller' THEN true
        WHEN p.node->>'isController' IS NOT NULL THEN (p.node->>'isController')::BOOLEAN
        ELSE false
    END as is_controller,
    NULLIF(p.node->>'cpuUsagePct', 'null')::DOUBLE PRECISION as cpu_usage_pct,
    NULLIF(p.node->>'memoryUsedMb', 'null')::BIGINT as memory_used_mb,
    NULLIF(p.node->>'memoryTotalMb', 'null')::BIGINT as memory_total_mb,
    NULLIF(p.node->>'diskUsedGb', 'null')::BIGINT as disk_used_gb,
    NULLIF(p.node->>'diskTotalGb', 'null')::BIGINT as disk_total_gb,
    (NULLIF(p.node->>'lastSeen', 'null'))::TIMESTAMPTZ as last_seen
FROM 
    kf_external_clusters c,
    jsonb_array_elements(
        CASE 
            WHEN c.external_broker_hosts_json IS NULL OR c.external_broker_hosts_json = '' THEN '[]'::jsonb
            ELSE c.external_broker_hosts_json::jsonb 
        END
    ) as p(node)
WHERE COALESCE(p.node->>'hostname', p.node->>'host') IS NOT NULL
ON CONFLICT (cluster_id, host) DO NOTHING;

-- Drop the old column
ALTER TABLE kf_external_clusters DROP COLUMN external_broker_hosts_json;
