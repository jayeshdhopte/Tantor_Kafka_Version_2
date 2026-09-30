-- Normalize Schema Registry and Kafka Connect certificate types.
-- JKS is no longer accepted; PKCS12 truststores use .p12 or .pfx files.

ALTER TABLE kf_data_service_connections
    DROP CONSTRAINT IF EXISTS chk_dsc_certificate_type;

UPDATE kf_data_service_connections
SET certificate_type = 'PKCS12'
WHERE certificate_type = 'PKCS12_JKS';

ALTER TABLE kf_data_service_connections
    ADD CONSTRAINT chk_dsc_certificate_type
        CHECK (certificate_type IS NULL OR certificate_type IN ('PEM', 'PKCS12'));
