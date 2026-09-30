-- Add missing fields to artifact audit log
ALTER TABLE kf_artifact_audit_log ADD COLUMN IF NOT EXISTS version_no VARCHAR(80);
ALTER TABLE kf_artifact_audit_log ADD COLUMN IF NOT EXISTS path_of_tar VARCHAR(1024);
ALTER TABLE kf_artifact_audit_log ADD COLUMN IF NOT EXISTS checksum VARCHAR(64);
