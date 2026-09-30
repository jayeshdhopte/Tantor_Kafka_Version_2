ALTER TABLE jobs
    ADD COLUMN IF NOT EXISTS rollback_supported BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE job_steps (
    id UUID PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL REFERENCES jobs(id) ON DELETE CASCADE,
    step_order INT NOT NULL,
    name VARCHAR(255) NOT NULL,
    target_id VARCHAR(255),
    status VARCHAR(50) NOT NULL,
    payload JSONB,
    agent_task_id UUID REFERENCES tasks(id) ON DELETE SET NULL,
    logs TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_job_steps_order UNIQUE (job_id, step_order)
);

CREATE INDEX idx_job_steps_job ON job_steps(job_id, step_order);
CREATE INDEX idx_job_steps_status ON job_steps(status);
