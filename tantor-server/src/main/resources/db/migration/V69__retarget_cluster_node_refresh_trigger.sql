-- V31 renamed the cluster inventory tables with the kf_ prefix, but PostgreSQL
-- retained the original table names inside this PL/pgSQL function body. Any
-- cluster-service insert therefore failed when the trigger queried the removed
-- legacy tables. Recreate the function against the canonical inventory tables.
CREATE OR REPLACE FUNCTION refresh_cluster_node_ids()
RETURNS trigger AS $$
DECLARE
    affected_cluster_id UUID;
BEGIN
    affected_cluster_id := CASE
        WHEN TG_OP = 'DELETE' THEN OLD.cluster_id
        ELSE NEW.cluster_id
    END;

    UPDATE kf_clusters c
    SET node_ids = COALESCE((
            SELECT jsonb_agg(nodes.node_id ORDER BY nodes.node_id)
            FROM (
                SELECT DISTINCT cs.node_id
                FROM kf_cluster_services cs
                WHERE cs.cluster_id = affected_cluster_id
                  AND cs.node_id IS NOT NULL
            ) nodes
        ), '[]'::jsonb),
        updated_by = 'system',
        updated_at = CURRENT_TIMESTAMP
    WHERE c.id = affected_cluster_id;

    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$ LANGUAGE plpgsql;
