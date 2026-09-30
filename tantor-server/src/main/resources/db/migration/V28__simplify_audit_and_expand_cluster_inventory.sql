ALTER TABLE audit_logs
    DROP COLUMN IF EXISTS user_id,
    DROP COLUMN IF EXISTS previous_hash,
    DROP COLUMN IF EXISTS record_hash;

ALTER TABLE clusters
    ADD COLUMN IF NOT EXISTS node_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS created_by VARCHAR(128) NOT NULL DEFAULT 'system',
    ADD COLUMN IF NOT EXISTS updated_by VARCHAR(128) NOT NULL DEFAULT 'system';

UPDATE clusters c
SET node_ids = COALESCE((
    SELECT jsonb_agg(nodes.node_id ORDER BY nodes.node_id)
    FROM (
        SELECT DISTINCT cs.node_id
        FROM cluster_services cs
        WHERE cs.cluster_id = c.id AND cs.node_id IS NOT NULL
    ) nodes
), '[]'::jsonb);

CREATE OR REPLACE FUNCTION refresh_cluster_node_ids()
RETURNS trigger AS $$
DECLARE
    affected_cluster_id UUID;
BEGIN
    affected_cluster_id := CASE WHEN TG_OP = 'DELETE' THEN OLD.cluster_id ELSE NEW.cluster_id END;
    UPDATE clusters c
    SET node_ids = COALESCE((
            SELECT jsonb_agg(nodes.node_id ORDER BY nodes.node_id)
            FROM (
                SELECT DISTINCT cs.node_id
                FROM cluster_services cs
                WHERE cs.cluster_id = affected_cluster_id AND cs.node_id IS NOT NULL
            ) nodes
        ), '[]'::jsonb),
        updated_by = 'system',
        updated_at = CURRENT_TIMESTAMP
    WHERE c.id = affected_cluster_id;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_refresh_cluster_node_ids ON cluster_services;
CREATE TRIGGER trg_refresh_cluster_node_ids
AFTER INSERT OR UPDATE OR DELETE ON cluster_services
FOR EACH ROW EXECUTE PROCEDURE refresh_cluster_node_ids();

UPDATE hosts SET status = 'OCCUPIED' WHERE status = 'UNAVAILABLE';
