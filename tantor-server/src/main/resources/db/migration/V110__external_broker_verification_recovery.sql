ALTER TABLE kf_external_broker_additions DROP CONSTRAINT kf_external_broker_additions_state_check;
ALTER TABLE kf_external_broker_additions ADD CONSTRAINT kf_external_broker_additions_state_check CHECK(state IN ('ACCEPTED','VALIDATING','PREPARING','PREPARED','FORMATTING','FORMATTED','STARTING','STARTED_PENDING_VERIFICATION','RECONCILING','VERIFYING','PUBLISHING','RECOVERY_REQUIRED','SUCCEEDED','CANCELLED_SAFE'));
ALTER TABLE kf_external_broker_additions ADD COLUMN resume_generation integer NOT NULL DEFAULT 0;
ALTER TABLE kf_external_broker_additions ADD COLUMN resume_inspection_id uuid;
ALTER TABLE kf_external_broker_additions ADD COLUMN verification text CHECK(octet_length(verification)<=131072);
ALTER TABLE kf_external_broker_additions ADD COLUMN verified_at timestamptz;
ALTER TABLE kf_external_broker_additions ADD COLUMN published_node_id uuid;
ALTER TABLE kf_external_broker_tasks ADD COLUMN resume_generation integer NOT NULL DEFAULT 0;
ALTER TABLE kf_external_broker_tasks ADD COLUMN retired_at timestamptz;
ALTER TABLE kf_external_broker_tasks ADD COLUMN claimed_at timestamptz;
UPDATE kf_external_broker_tasks SET claimed_at=created_at WHERE claim_token IS NOT NULL;
CREATE INDEX idx_external_broker_generation ON kf_external_broker_tasks(operation_id,resume_generation,status);

ALTER TABLE kf_external_broker_tasks ALTER COLUMN created_at SET DEFAULT clock_timestamp();
