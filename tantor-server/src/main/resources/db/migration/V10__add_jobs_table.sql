CREATE TABLE jobs (
    id VARCHAR(36) PRIMARY KEY,
    type VARCHAR(50) NOT NULL,
    status VARCHAR(50) NOT NULL,
    requested_by VARCHAR(255),
    start_time TIMESTAMP,
    end_time TIMESTAMP,
    payload JSONB,
    progress JSONB,
    logs TEXT,
    retry_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
