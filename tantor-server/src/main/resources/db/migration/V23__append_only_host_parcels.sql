ALTER TABLE host_parcels
    DROP CONSTRAINT IF EXISTS uq_host_parcel_artifact,
    DROP COLUMN IF EXISTS artifact_url,
    ADD COLUMN IF NOT EXISTS action VARCHAR(40) NOT NULL DEFAULT 'DISTRIBUTE',
    ADD COLUMN IF NOT EXISTS created_by VARCHAR(128) NOT NULL DEFAULT 'system',
    ADD COLUMN IF NOT EXISTS updated_by VARCHAR(128) NOT NULL DEFAULT 'system';

UPDATE host_parcels
SET action = CASE
    WHEN status IN ('ACTIVE', 'ACTIVATING') THEN 'ACTIVATE'
    WHEN status IN ('DEACTIVATED', 'DEACTIVATING') THEN 'DEACTIVATE'
    WHEN status IN ('REMOVED', 'REMOVING') THEN 'REMOVE'
    ELSE 'DISTRIBUTE'
END;

CREATE INDEX IF NOT EXISTS idx_host_parcels_latest
    ON host_parcels(host_id, artifact_id, created_at DESC, id DESC);

