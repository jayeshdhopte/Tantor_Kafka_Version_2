ALTER TABLE kf_external_clusters
    ADD COLUMN IF NOT EXISTS truststore_content_encrypted TEXT,
    ADD COLUMN IF NOT EXISTS keystore_content_encrypted TEXT;
