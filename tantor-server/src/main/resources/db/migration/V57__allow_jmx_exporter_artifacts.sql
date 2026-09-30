-- Allow JMX exporter artifacts to be stored by the artifact repository.
-- The Java enum already supports JMX_EXPORTER, FAILED, and DEPRECATED; this
-- migration keeps older databases aligned with the current application model.

ALTER TABLE IF EXISTS kf_artifact
    DROP CONSTRAINT IF EXISTS ck_artifact_service_type;

ALTER TABLE IF EXISTS kf_artifact
    ADD CONSTRAINT ck_artifact_service_type CHECK (
        service_type IN (
            'KAFKA',
            'KAFKA_CONNECT',
            'SCHEMA_REGISTRY',
            'KSQLDB',
            'CRUISE_CONTROL',
            'PROMETHEUS',
            'GRAFANA',
            'JMX_EXPORTER'
        )
    );

ALTER TABLE IF EXISTS kf_artifact
    DROP CONSTRAINT IF EXISTS ck_artifact_status;

ALTER TABLE IF EXISTS kf_artifact
    ADD CONSTRAINT ck_artifact_status CHECK (
        status IN (
            'UPLOADING',
            'AVAILABLE',
            'CORRUPTED',
            'QUARANTINED',
            'DELETED',
            'FAILED',
            'DEPRECATED',
            'LEGACY_DUPLICATE'
        )
    );
