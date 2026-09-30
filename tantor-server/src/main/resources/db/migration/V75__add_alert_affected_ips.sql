-- Preserve the exact node IP snapshot for cluster-level alerts. A cluster
-- alert can affect multiple discovery agents, so host_id alone is insufficient.
ALTER TABLE kf_alerts
    ADD COLUMN IF NOT EXISTS affected_ips TEXT;

-- Rollback (after rolling application code back):
-- ALTER TABLE kf_alerts DROP COLUMN IF EXISTS affected_ips;
