-- Phase 2: add an immutable canonical cluster UUID to the existing cluster
-- inventory and external-node inventory. This migration is intentionally
-- additive; it does not restructure or delete either legacy table.

ALTER TABLE kf_clusters
    ADD COLUMN IF NOT EXISTS canonical_cluster_uuid UUID;

ALTER TABLE kf_external_cluster_nodes
    ADD COLUMN IF NOT EXISTS canonical_cluster_uuid UUID;

-- Existing kf_clusters.id values are already stable UUIDs. Reuse them rather
-- than generating replacement identities.
UPDATE kf_clusters
SET canonical_cluster_uuid = id
WHERE canonical_cluster_uuid IS NULL;

-- External clusters are mirrored into kf_clusters with the same UUID. Bind a
-- node through its existing cluster_id relationship only. Do not match by
-- host, IP address, hostname, display name or bootstrap server.
UPDATE kf_external_cluster_nodes AS external_node
SET canonical_cluster_uuid = cluster.canonical_cluster_uuid
FROM kf_clusters AS cluster
WHERE cluster.id = external_node.cluster_id
  AND external_node.canonical_cluster_uuid IS NULL;

-- Fail closed before constraints are enabled. A missing parent or conflicting
-- UUID must be corrected explicitly; the migration must never guess identity.
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM kf_clusters
        WHERE canonical_cluster_uuid IS NULL
           OR canonical_cluster_uuid <> id
    ) THEN
        RAISE EXCEPTION 'Canonical UUID backfill left a kf_clusters row unbound or mismatched';
    END IF;

    IF EXISTS (
        SELECT canonical_cluster_uuid
        FROM kf_clusters
        GROUP BY canonical_cluster_uuid
        HAVING COUNT(*) > 1
    ) THEN
        RAISE EXCEPTION 'Canonical UUID backfill produced duplicate kf_clusters identities';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM kf_external_cluster_nodes AS external_node
        LEFT JOIN kf_clusters AS cluster
          ON cluster.id = external_node.cluster_id
        WHERE external_node.canonical_cluster_uuid IS NULL
           OR cluster.canonical_cluster_uuid IS NULL
           OR external_node.canonical_cluster_uuid <> cluster.canonical_cluster_uuid
    ) THEN
        RAISE EXCEPTION 'Canonical UUID backfill left an external node unbound or mismatched';
    END IF;
END;
$$;

ALTER TABLE kf_clusters
    ALTER COLUMN canonical_cluster_uuid SET NOT NULL;

ALTER TABLE kf_external_cluster_nodes
    ALTER COLUMN canonical_cluster_uuid SET NOT NULL;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'uq_kf_clusters_canonical_cluster_uuid'
          AND conrelid = 'kf_clusters'::regclass
    ) THEN
        ALTER TABLE kf_clusters
            ADD CONSTRAINT uq_kf_clusters_canonical_cluster_uuid
            UNIQUE (canonical_cluster_uuid);
    END IF;
END;
$$;

CREATE INDEX IF NOT EXISTS ix_external_nodes_canonical_cluster_uuid
    ON kf_external_cluster_nodes (canonical_cluster_uuid);

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = 'fk_external_nodes_canonical_cluster_uuid'
          AND conrelid = 'kf_external_cluster_nodes'::regclass
    ) THEN
        ALTER TABLE kf_external_cluster_nodes
            ADD CONSTRAINT fk_external_nodes_canonical_cluster_uuid
            FOREIGN KEY (canonical_cluster_uuid)
            REFERENCES kf_clusters (canonical_cluster_uuid)
            ON DELETE RESTRICT;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION assign_kf_cluster_canonical_uuid()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.canonical_cluster_uuid IS NULL THEN
            NEW.canonical_cluster_uuid := NEW.id;
        ELSIF NEW.canonical_cluster_uuid IS DISTINCT FROM NEW.id THEN
            RAISE EXCEPTION 'canonical_cluster_uuid must equal the immutable kf_clusters.id';
        END IF;
    ELSIF NEW.canonical_cluster_uuid IS DISTINCT FROM OLD.canonical_cluster_uuid THEN
        RAISE EXCEPTION 'canonical_cluster_uuid is immutable for kf_clusters';
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_assign_kf_cluster_canonical_uuid ON kf_clusters;
CREATE TRIGGER trg_assign_kf_cluster_canonical_uuid
BEFORE INSERT OR UPDATE OF canonical_cluster_uuid ON kf_clusters
FOR EACH ROW EXECUTE PROCEDURE assign_kf_cluster_canonical_uuid();

CREATE OR REPLACE FUNCTION assign_external_node_canonical_uuid()
RETURNS TRIGGER AS $$
DECLARE
    expected_cluster_uuid UUID;
BEGIN
    SELECT cluster.canonical_cluster_uuid
    INTO expected_cluster_uuid
    FROM kf_clusters AS cluster
    WHERE cluster.id = NEW.cluster_id;

    IF expected_cluster_uuid IS NULL THEN
        RAISE EXCEPTION 'No canonical kf_clusters parent exists for external cluster_id %', NEW.cluster_id;
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.canonical_cluster_uuid IS NULL THEN
            NEW.canonical_cluster_uuid := expected_cluster_uuid;
        ELSIF NEW.canonical_cluster_uuid IS DISTINCT FROM expected_cluster_uuid THEN
            RAISE EXCEPTION 'External node canonical UUID does not match its cluster_id parent';
        END IF;
    ELSE
        IF NEW.cluster_id IS DISTINCT FROM OLD.cluster_id THEN
            RAISE EXCEPTION 'cluster_id is immutable for an existing external node';
        END IF;
        IF NEW.canonical_cluster_uuid IS DISTINCT FROM OLD.canonical_cluster_uuid THEN
            RAISE EXCEPTION 'canonical_cluster_uuid is immutable for an existing external node';
        END IF;
    END IF;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_assign_external_node_canonical_uuid ON kf_external_cluster_nodes;
CREATE TRIGGER trg_assign_external_node_canonical_uuid
BEFORE INSERT OR UPDATE OF cluster_id, canonical_cluster_uuid ON kf_external_cluster_nodes
FOR EACH ROW EXECUTE PROCEDURE assign_external_node_canonical_uuid();

-- Operational rollback (maintenance window only): roll the application back
-- first, then drop the two triggers/functions, the external-node FK/index, the
-- cluster unique constraint, and finally the two new columns. Existing business
-- columns and primary keys have not been changed by this migration.
