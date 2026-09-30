-- Add security protocol and SASL fields
ALTER TABLE kf_external_clusters ADD COLUMN IF NOT EXISTS security_protocol VARCHAR(50);
ALTER TABLE kf_external_clusters ADD COLUMN IF NOT EXISTS sasl_mechanism VARCHAR(50);
ALTER TABLE kf_external_clusters ADD COLUMN IF NOT EXISTS sasl_username VARCHAR(255);
ALTER TABLE kf_external_clusters ADD COLUMN IF NOT EXISTS sasl_password_encrypted VARCHAR(512);

-- Add truststore and keystore fields
ALTER TABLE kf_external_clusters ADD COLUMN IF NOT EXISTS truststore_path VARCHAR(512);
ALTER TABLE kf_external_clusters ADD COLUMN IF NOT EXISTS truststore_password_encrypted VARCHAR(512);
ALTER TABLE kf_external_clusters ADD COLUMN IF NOT EXISTS truststore_type VARCHAR(50);

ALTER TABLE kf_external_clusters ADD COLUMN IF NOT EXISTS keystore_path VARCHAR(512);
ALTER TABLE kf_external_clusters ADD COLUMN IF NOT EXISTS keystore_password_encrypted VARCHAR(512);
ALTER TABLE kf_external_clusters ADD COLUMN IF NOT EXISTS key_password_encrypted VARCHAR(512);
ALTER TABLE kf_external_clusters ADD COLUMN IF NOT EXISTS keystore_type VARCHAR(50);

-- Hostname verification
ALTER TABLE kf_external_clusters ADD COLUMN IF NOT EXISTS disable_hostname_verification BOOLEAN DEFAULT FALSE;
