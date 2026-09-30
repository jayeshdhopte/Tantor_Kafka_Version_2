-- V45: Create kf_data_service_connections table
-- Stores connection config for Schema Registry and Kafka Connect per cluster.
-- Live data (subjects, connectors, etc.) is NOT stored here — still live-fetched from REST APIs.

CREATE TABLE IF NOT EXISTS kf_data_service_connections (
    id                          UUID            NOT NULL DEFAULT gen_random_uuid(),
    cluster_id                  UUID            NOT NULL,
    service_type                VARCHAR(32)     NOT NULL,
    connection_name             VARCHAR(255)    NOT NULL DEFAULT 'Default connection',
    protocol                    VARCHAR(8)      NOT NULL DEFAULT 'http',
    host                        VARCHAR(512)    NOT NULL,
    port                        INTEGER         NOT NULL,
    certificate_type            VARCHAR(16),
    certificate_data            TEXT,
    truststore_path             VARCHAR(512),
    truststore_password_encrypted VARCHAR(1024),
    security_protocol           VARCHAR(50),
    rest_endpoint               VARCHAR(1024),
    status                      VARCHAR(16)     NOT NULL DEFAULT 'UNKNOWN',
    last_error                  TEXT,
    last_checked_at             TIMESTAMPTZ,
    is_active                   BOOLEAN         NOT NULL DEFAULT TRUE,
    created_by                  VARCHAR(255)    NOT NULL DEFAULT 'system',
    updated_by                  VARCHAR(255)    NOT NULL DEFAULT 'system',
    created_at                  TIMESTAMPTZ     NOT NULL DEFAULT now(),
    updated_at                  TIMESTAMPTZ     NOT NULL DEFAULT now(),

    CONSTRAINT pk_data_service_connections PRIMARY KEY (id),

    CONSTRAINT chk_dsc_service_type
        CHECK (service_type IN ('SCHEMA_REGISTRY', 'KAFKA_CONNECT')),

    CONSTRAINT chk_dsc_protocol
        CHECK (protocol IN ('http', 'https')),

    CONSTRAINT chk_dsc_certificate_type
        CHECK (certificate_type IS NULL OR certificate_type IN ('PEM', 'PKCS12_JKS')),

    CONSTRAINT chk_dsc_status
        CHECK (status IN ('UNKNOWN', 'ONLINE', 'OFFLINE', 'ERROR'))
);

-- Partial unique index: only one active row per cluster+service+name
CREATE UNIQUE INDEX IF NOT EXISTS uidx_dsc_active
    ON kf_data_service_connections (cluster_id, service_type, connection_name)
    WHERE is_active = TRUE;

COMMENT ON TABLE kf_data_service_connections IS
    'Persists connection configuration (protocol, host, port, TLS) for Schema Registry and Kafka Connect per cluster. Live schema/connector data is not stored here.';
