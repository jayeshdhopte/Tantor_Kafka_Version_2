-- V1__init_schema.sql
-- Tantor Platform Database Schema (PostgreSQL 16)

-- 1. Users & RBAC
CREATE TABLE roles (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50) UNIQUE NOT NULL,
    description VARCHAR(255)
);

CREATE TABLE permissions (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    description VARCHAR(255)
);

CREATE TABLE role_permissions (
    role_id INT REFERENCES roles(id) ON DELETE CASCADE,
    permission_id INT REFERENCES permissions(id) ON DELETE CASCADE,
    PRIMARY KEY (role_id, permission_id)
);

CREATE TABLE users (
    id UUID PRIMARY KEY,
    username VARCHAR(100) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    email VARCHAR(255),
    role_id INT REFERENCES roles(id),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 2. Infrastructure Inventory
CREATE TABLE clusters (
    id UUID PRIMARY KEY,
    name VARCHAR(100) UNIQUE NOT NULL,
    cluster_type VARCHAR(50) NOT NULL, -- e.g., KAFKA, CONNECT, SCHEMA_REGISTRY
    status VARCHAR(50) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE hosts (
    id VARCHAR(100) PRIMARY KEY, -- Agent host_id
    hostname VARCHAR(255) NOT NULL,
    ip_addresses JSONB,
    os_details VARCHAR(255),
    agent_version VARCHAR(50),
    status VARCHAR(50) NOT NULL, -- ONLINE, OFFLINE, ERROR
    last_heartbeat TIMESTAMP WITH TIME ZONE,
    cpu_usage_pct DOUBLE PRECISION,
    mem_total_mb BIGINT,
    mem_used_mb BIGINT,
    disk_total_gb BIGINT,
    disk_used_gb BIGINT,
    java_version VARCHAR(100),
    cluster_id UUID REFERENCES clusters(id) ON DELETE SET NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 3. Services & Deployments
CREATE TABLE services (
    id UUID PRIMARY KEY,
    cluster_id UUID REFERENCES clusters(id) ON DELETE CASCADE,
    host_id VARCHAR(100) REFERENCES hosts(id) ON DELETE RESTRICT,
    service_type VARCHAR(50) NOT NULL, -- BROKER, CONTROLLER, WORKER
    node_id INT, -- e.g., Kafka broker.id
    status VARCHAR(50) NOT NULL,
    config_overrides JSONB,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- 4. Tasks & Audit
CREATE TABLE tasks (
    id UUID PRIMARY KEY,
    host_id VARCHAR(100) REFERENCES hosts(id) ON DELETE CASCADE,
    command VARCHAR(100) NOT NULL,
    parameters JSONB,
    artifact_url VARCHAR(255),
    checksum VARCHAR(255),
    status VARCHAR(50) NOT NULL, -- PENDING, IN_PROGRESS, SUCCESS, FAILED
    log_output TEXT,
    error_msg TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE audit_logs (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES users(id),
    action VARCHAR(100) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_id VARCHAR(100),
    details JSONB,
    ip_address VARCHAR(45),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_hosts_cluster ON hosts(cluster_id);
CREATE INDEX idx_services_cluster ON services(cluster_id);
CREATE INDEX idx_tasks_host ON tasks(host_id);
CREATE INDEX idx_tasks_status ON tasks(status);
CREATE INDEX idx_audit_created ON audit_logs(created_at);

-- Initial Data
INSERT INTO roles (name, description) VALUES ('ADMIN', 'Administrator with full access');
INSERT INTO roles (name, description) VALUES ('OPERATOR', 'Operator with deployment access');
INSERT INTO roles (name, description) VALUES ('VIEWER', 'Read-only access');

-- Note: Default admin user password should be populated via application logic or a secure seed
