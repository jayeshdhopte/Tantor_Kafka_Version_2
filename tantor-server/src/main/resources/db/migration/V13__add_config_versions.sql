CREATE TABLE config_versions (
    id UUID PRIMARY KEY,
    cluster_id UUID NOT NULL REFERENCES clusters(id) ON DELETE CASCADE,
    service_id UUID REFERENCES cluster_services(id) ON DELETE SET NULL,
    host_id VARCHAR(100) NOT NULL,
    component VARCHAR(100) NOT NULL,
    config_file_name VARCHAR(500) NOT NULL,
    config_version INT NOT NULL,
    old_config JSONB NOT NULL,
    new_config JSONB NOT NULL,
    status VARCHAR(50) NOT NULL,
    approval_required BOOLEAN NOT NULL DEFAULT FALSE,
    validation_result JSONB NOT NULL DEFAULT '{}',
    created_by VARCHAR(255) NOT NULL,
    approved_by VARCHAR(255),
    job_id VARCHAR(36) REFERENCES jobs(id) ON DELETE SET NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    approved_at TIMESTAMP WITH TIME ZONE,
    applied_at TIMESTAMP WITH TIME ZONE,
    rollback_version INT,
    CONSTRAINT uq_config_version UNIQUE (cluster_id, host_id, component, config_file_name, config_version)
);

CREATE INDEX idx_config_versions_target
    ON config_versions(cluster_id, host_id, component, config_file_name, config_version DESC);

CREATE INDEX idx_config_versions_status
    ON config_versions(status, created_at);
