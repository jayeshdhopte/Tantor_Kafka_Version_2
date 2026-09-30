CREATE TABLE host_parcels (
    id UUID PRIMARY KEY,
    host_id VARCHAR(100) NOT NULL REFERENCES hosts(id) ON DELETE CASCADE,
    artifact_id UUID NOT NULL,
    service_type VARCHAR(50) NOT NULL,
    version VARCHAR(80) NOT NULL,
    file_name VARCHAR(512),
    artifact_url TEXT,
    checksum VARCHAR(255),
    parcel_dir VARCHAR(1024),
    status VARCHAR(50) NOT NULL,
    active BOOLEAN NOT NULL DEFAULT FALSE,
    last_task_id UUID REFERENCES tasks(id) ON DELETE SET NULL,
    error_msg TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT uq_host_parcel_artifact UNIQUE (host_id, artifact_id)
);

CREATE INDEX idx_host_parcels_host ON host_parcels(host_id);
CREATE INDEX idx_host_parcels_artifact ON host_parcels(artifact_id);
CREATE INDEX idx_host_parcels_status ON host_parcels(status);
CREATE INDEX idx_host_parcels_active ON host_parcels(host_id, service_type, active);
