ALTER TABLE public.kf_config_backup ADD COLUMN verification_method VARCHAR(40);
ALTER TABLE public.kf_config_backup ADD COLUMN integrity_status VARCHAR(24);
ALTER TABLE public.kf_config_backup ADD COLUMN last_checked_at TIMESTAMPTZ;
COMMENT ON COLUMN public.kf_config_backup.checksum_sha256 IS
    'SHA-256 of exact physical backup bytes, supplied by verified agent receipt or explicit legacy readback; NULL for logical checkpoints and unverified legacy files.';
COMMENT ON COLUMN public.kf_config_backup.verified_at IS
    'Actual UTC verification time; not job completion time. NULL when no physical-file verification evidence was received.';
COMMENT ON COLUMN public.kf_config_backup.verification_method IS
    'SOURCE_COPY_MATCH: agent compared saved bytes to captured source before mutation. LEGACY_READBACK_STABILITY: two matching current reads establish a baseline, not proof of original contents.';
COMMENT ON COLUMN public.kf_config_backup.integrity_status IS
    'VERIFIED when physical evidence exists, MISSING when an explicit host read finds no file; NULL for logical checkpoints or no recorded check.';
COMMENT ON COLUMN public.kf_config_backup.last_checked_at IS
    'Actual time of recorded physical-file check, including failed checks; distinct from successful verified_at.';
