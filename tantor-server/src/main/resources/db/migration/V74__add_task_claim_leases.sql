ALTER TABLE kf_tasks
    ADD COLUMN IF NOT EXISTS claim_token VARCHAR(64),
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS lease_expires_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_kf_tasks_claimable
    ON kf_tasks (host_id, status, created_at);

CREATE INDEX IF NOT EXISTS idx_kf_tasks_lease_expiry
    ON kf_tasks (lease_expires_at)
    WHERE status = 'IN_PROGRESS';
