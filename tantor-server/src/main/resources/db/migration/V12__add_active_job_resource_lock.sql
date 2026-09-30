ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS resource_key VARCHAR(255);

CREATE UNIQUE INDEX uq_jobs_active_resource
    ON jobs(resource_key)
    WHERE resource_key IS NOT NULL
      AND status IN ('PENDING', 'IN_PROGRESS', 'ROLLBACK_PENDING', 'ROLLING_BACK');
