DO $$ 
BEGIN
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'activity_logs') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_activity_logs') THEN ALTER TABLE activity_logs RENAME TO kf_activity_logs; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'alerts') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_alerts') THEN ALTER TABLE alerts RENAME TO kf_alerts; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'artifact') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_artifact') THEN ALTER TABLE artifact RENAME TO kf_artifact; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'artifact_audit_log') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_artifact_audit_log') THEN ALTER TABLE artifact_audit_log RENAME TO kf_artifact_audit_log; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'artifact_download_log') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_artifact_download_log') THEN ALTER TABLE artifact_download_log RENAME TO kf_artifact_download_log; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'audit_logs') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_audit_logs') THEN ALTER TABLE audit_logs RENAME TO kf_audit_logs; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'cluster_services') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_cluster_services') THEN ALTER TABLE cluster_services RENAME TO kf_cluster_services; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'clusters') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_clusters') THEN ALTER TABLE clusters RENAME TO kf_clusters; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'config_versions') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_config_versions') THEN ALTER TABLE config_versions RENAME TO kf_config_versions; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'host_parcels') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_host_parcels') THEN ALTER TABLE host_parcels RENAME TO kf_host_parcels; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'hosts') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_hosts') THEN ALTER TABLE hosts RENAME TO kf_hosts; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'job_steps') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_job_steps') THEN ALTER TABLE job_steps RENAME TO kf_job_steps; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'jobs') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_jobs') THEN ALTER TABLE jobs RENAME TO kf_jobs; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'ldap_configs') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_ldap_configs') THEN ALTER TABLE ldap_configs RENAME TO kf_ldap_configs; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'tasks') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_tasks') THEN ALTER TABLE tasks RENAME TO kf_tasks; END IF;
    IF EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'users') AND NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename = 'kf_users') THEN ALTER TABLE users RENAME TO kf_users; END IF;
END $$;

DO $$ 
BEGIN
    -- MAPPINGS FOR kf_audit_logs
    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_audit_logs' AND column_name='actor') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_audit_logs' AND column_name='actor_user') THEN
            ALTER TABLE kf_audit_logs RENAME COLUMN actor TO actor_user;
        ELSE
            UPDATE kf_audit_logs SET actor_user = actor WHERE actor_user IS NULL;
        END IF;
    ELSE
        ALTER TABLE kf_audit_logs ADD COLUMN IF NOT EXISTS actor_user VARCHAR(128);
    END IF;

    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_audit_logs' AND column_name='entity_type') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_audit_logs' AND column_name='resource_type') THEN
            ALTER TABLE kf_audit_logs RENAME COLUMN entity_type TO resource_type;
        ELSE
            UPDATE kf_audit_logs SET resource_type = entity_type WHERE resource_type IS NULL;
        END IF;
    ELSE
        ALTER TABLE kf_audit_logs ADD COLUMN IF NOT EXISTS resource_type VARCHAR(50);
    END IF;

    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_audit_logs' AND column_name='entity_id') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_audit_logs' AND column_name='resource_id') THEN
            ALTER TABLE kf_audit_logs RENAME COLUMN entity_id TO resource_id;
        ELSE
            UPDATE kf_audit_logs SET resource_id = entity_id WHERE resource_id IS NULL;
        END IF;
    ELSE
        ALTER TABLE kf_audit_logs ADD COLUMN IF NOT EXISTS resource_id VARCHAR(255);
    END IF;

    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_audit_logs' AND column_name='source') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_audit_logs' AND column_name='origin') THEN
            ALTER TABLE kf_audit_logs RENAME COLUMN source TO origin;
        ELSE
            UPDATE kf_audit_logs SET origin = source WHERE origin IS NULL;
        END IF;
    ELSE
        ALTER TABLE kf_audit_logs ADD COLUMN IF NOT EXISTS origin VARCHAR(100);
    END IF;

    ALTER TABLE kf_audit_logs 
      ADD COLUMN IF NOT EXISTS event VARCHAR(255),
      ADD COLUMN IF NOT EXISTS resource VARCHAR(100),
      ADD COLUMN IF NOT EXISTS host_id VARCHAR(255),
      ADD COLUMN IF NOT EXISTS host_ip VARCHAR(100),
      ADD COLUMN IF NOT EXISTS host_name VARCHAR(255),
      ADD COLUMN IF NOT EXISTS artifact_id UUID,
      ADD COLUMN IF NOT EXISTS alert_id UUID,
      ADD COLUMN IF NOT EXISTS user_id VARCHAR(255),
      ADD COLUMN IF NOT EXISTS created_by VARCHAR(128);

    -- MAPPINGS FOR kf_artifact_audit_log
    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact_audit_log' AND column_name='actor') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact_audit_log' AND column_name='actor_user') THEN
            ALTER TABLE kf_artifact_audit_log RENAME COLUMN actor TO actor_user;
        ELSE
            UPDATE kf_artifact_audit_log SET actor_user = actor WHERE actor_user IS NULL;
        END IF;
    ELSE
        ALTER TABLE kf_artifact_audit_log ADD COLUMN IF NOT EXISTS actor_user VARCHAR(128);
    END IF;

    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact_audit_log' AND column_name='resource_id') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact_audit_log' AND column_name='artifact_id') THEN
            ALTER TABLE kf_artifact_audit_log RENAME COLUMN resource_id TO artifact_id;
        ELSE
            UPDATE kf_artifact_audit_log SET artifact_id = CAST(resource_id AS UUID) WHERE artifact_id IS NULL;
        END IF;
    ELSE
        ALTER TABLE kf_artifact_audit_log ADD COLUMN IF NOT EXISTS artifact_id UUID;
    END IF;

    ALTER TABLE kf_artifact_audit_log
      ADD COLUMN IF NOT EXISTS host_ip VARCHAR(100),
      ADD COLUMN IF NOT EXISTS host_name VARCHAR(255),
      ADD COLUMN IF NOT EXISTS version VARCHAR(100),
      ADD COLUMN IF NOT EXISTS path VARCHAR(512),
      ADD COLUMN IF NOT EXISTS checksum VARCHAR(128),
      ADD COLUMN IF NOT EXISTS created_by VARCHAR(128);
END $$;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS kf_host_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    host_id VARCHAR(255) NOT NULL,
    cluster_id UUID,
    action VARCHAR(100) NOT NULL,
    event VARCHAR(255),
    status VARCHAR(50),
    origin VARCHAR(100),
    resource VARCHAR(100),
    resource_type VARCHAR(50) DEFAULT 'HOST',
    host_name VARCHAR(255),
    host_ip VARCHAR(100),
    agent_version VARCHAR(100),
    java_version VARCHAR(100),
    os_name VARCHAR(255),
    user_id VARCHAR(255),
    actor_user VARCHAR(128),
    created_by VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    details JSONB
);

CREATE TABLE IF NOT EXISTS kf_cluster_audit_log (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_id UUID NOT NULL,
    action VARCHAR(100) NOT NULL,
    event VARCHAR(255),
    status VARCHAR(50),
    origin VARCHAR(100),
    resource VARCHAR(100),
    resource_type VARCHAR(50) DEFAULT 'CLUSTER',
    cluster_name VARCHAR(255),
    bootstrap_ip VARCHAR(100),
    env VARCHAR(50),
    kafka_version VARCHAR(100),
    mode VARCHAR(100),
    user_id VARCHAR(255),
    actor_user VARCHAR(128),
    created_by VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    details JSONB
);

CREATE TABLE IF NOT EXISTS kf_host_alerts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    severity VARCHAR(50) NOT NULL,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    status VARCHAR(50) NOT NULL,
    host_id VARCHAR(255) NOT NULL,
    cluster_id UUID,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    resolved_at TIMESTAMPTZ,
    log_path VARCHAR(512),
    host_name VARCHAR(255),
    host_ip VARCHAR(100),
    agent_status VARCHAR(50),
    last_heartbeat TIMESTAMPTZ,
    env VARCHAR(50),
    alert_user VARCHAR(128),
    created_by VARCHAR(128),
    resolved_by VARCHAR(128),
    resource_type VARCHAR(50) DEFAULT 'HOST'
);
