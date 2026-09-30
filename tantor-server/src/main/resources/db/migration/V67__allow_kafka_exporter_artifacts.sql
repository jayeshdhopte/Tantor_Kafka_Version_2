-- Allow Kafka exporter artifacts to be stored by the artifact repository.

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
            'JMX_EXPORTER',
            'KAFKA_EXPORTER'
        )
    );
