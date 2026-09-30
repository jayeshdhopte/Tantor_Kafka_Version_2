ALTER TABLE kf_data_service_connections
DROP COLUMN IF EXISTS truststore_path,
DROP COLUMN IF EXISTS security_protocol;
