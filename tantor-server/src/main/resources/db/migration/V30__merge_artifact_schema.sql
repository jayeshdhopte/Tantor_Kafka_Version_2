-- Merge Artifact Repository schema into Tantor Server schema management.
-- This script safely creates the final tables if they do not already exist,
-- preventing failures on existing environments where the tantor-artifact-repository
-- had already applied its own flyway migrations.

DO $$ 
BEGIN
    IF NOT EXISTS (SELECT FROM pg_tables WHERE schemaname = 'public' AND tablename  = 'artifact') THEN
        
        CREATE TABLE artifact (
            id                UUID         NOT NULL DEFAULT gen_random_uuid(),
            root_artifact_id  UUID         NOT NULL,
            action            VARCHAR(40)  NOT NULL DEFAULT 'UPLOAD',
            service_type      VARCHAR(40)  NOT NULL,
            version           VARCHAR(80)  NOT NULL,
            file_name         VARCHAR(512) NOT NULL,
            relative_path     VARCHAR(1024) NOT NULL,
            full_file_path    VARCHAR(2048),
            file_size_bytes   BIGINT       NOT NULL,
            content_type      VARCHAR(128) NOT NULL DEFAULT 'application/gzip',
            checksum_sha256   CHAR(64)     NOT NULL,
            checksum_md5      CHAR(32),
            status            VARCHAR(32)  NOT NULL DEFAULT 'UPLOADING',
            created_by        VARCHAR(128) NOT NULL DEFAULT 'system',
            created_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
            updated_by        VARCHAR(128) NOT NULL DEFAULT 'system',
            updated_at        TIMESTAMPTZ  NOT NULL DEFAULT now(),
            downloaded_by     VARCHAR(128),
            downloaded_at     TIMESTAMPTZ,
            verified_checksum BOOLEAN,
            CONSTRAINT pk_artifact PRIMARY KEY (id),
            CONSTRAINT ck_artifact_service_type CHECK (
                service_type IN ('KAFKA','KAFKA_CONNECT','SCHEMA_REGISTRY','KSQLDB',
                                 'CRUISE_CONTROL','PROMETHEUS','GRAFANA')
            ),
            CONSTRAINT ck_artifact_status CHECK (
                status IN ('UPLOADING','AVAILABLE','CORRUPTED','QUARANTINED','DELETED', 'LEGACY_DUPLICATE')
            )
        );
        
        CREATE INDEX ix_artifact_service_type ON artifact (service_type);
        CREATE INDEX ix_artifact_status       ON artifact (status);
        CREATE INDEX ix_artifact_sha256       ON artifact (checksum_sha256);
        CREATE INDEX ix_artifact_service_version_upload ON artifact(service_type, version) WHERE action = 'UPLOAD';
        CREATE INDEX ix_artifact_checksum_upload ON artifact(checksum_sha256) WHERE action = 'UPLOAD';
        CREATE INDEX ix_artifact_root_created ON artifact(root_artifact_id, created_at DESC);
        CREATE INDEX ix_artifact_action_created ON artifact(action, created_at DESC);
        
        CREATE TABLE artifact_audit_log (
            id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
            actor VARCHAR(255) NOT NULL,
            event_category VARCHAR(80) NOT NULL,
            action VARCHAR(100) NOT NULL,
            resource_type VARCHAR(80) NOT NULL,
            resource_id VARCHAR(255),
            status VARCHAR(40) NOT NULL,
            details JSONB,
            created_at TIMESTAMPTZ NOT NULL DEFAULT now()
        );
        
        CREATE INDEX idx_artifact_audit_created ON artifact_audit_log(created_at DESC);
        CREATE INDEX idx_artifact_audit_action ON artifact_audit_log(action, created_at DESC);
        CREATE INDEX idx_artifact_audit_status ON artifact_audit_log(status, created_at DESC);
        
    END IF;
END $$;

-- Ensure the trigger function exists and the trigger is applied in either case (new DB or existing DB).
CREATE OR REPLACE FUNCTION prevent_artifact_audit_mutation()
RETURNS trigger AS $$
BEGIN
    RAISE EXCEPTION 'artifact_audit_log is append-only; % is forbidden', TG_OP;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_artifact_audit_immutable ON artifact_audit_log;
CREATE TRIGGER trg_artifact_audit_immutable
BEFORE UPDATE OR DELETE ON artifact_audit_log
FOR EACH ROW EXECUTE PROCEDURE prevent_artifact_audit_mutation();
