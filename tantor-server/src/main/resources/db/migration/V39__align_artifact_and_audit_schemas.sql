-- Final Schema Alignment for Artifacts, Artifact Audit Log, and Global Audit Log

DO $$ 
BEGIN

    ---------------------------------------------------------------------------
    -- 1. kf_artifact
    ---------------------------------------------------------------------------
    -- Keep id as PK.
    -- Add artifact_id as a separate column.
    ALTER TABLE kf_artifact ADD COLUMN IF NOT EXISTS artifact_id UUID;
    
    -- Rename version to version_no
    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact' AND column_name='version') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact' AND column_name='version_no') THEN
            ALTER TABLE kf_artifact RENAME COLUMN version TO version_no;
        END IF;
    END IF;
    
    -- Rename file_name to binary_file_name
    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact' AND column_name='file_name') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact' AND column_name='binary_file_name') THEN
            ALTER TABLE kf_artifact RENAME COLUMN file_name TO binary_file_name;
        END IF;
    END IF;

    -- Rename relative_path to path_of_tar
    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact' AND column_name='relative_path') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact' AND column_name='path_of_tar') THEN
            ALTER TABLE kf_artifact RENAME COLUMN relative_path TO path_of_tar;
        END IF;
    END IF;
    
    -- Rename updated_at to update_time
    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact' AND column_name='updated_at') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact' AND column_name='update_time') THEN
            ALTER TABLE kf_artifact RENAME COLUMN updated_at TO update_time;
        END IF;
    END IF;

    -- Rename created_at to created_time
    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact' AND column_name='created_at') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact' AND column_name='created_time') THEN
            ALTER TABLE kf_artifact RENAME COLUMN created_at TO created_time;
        END IF;
    END IF;
    
    -- Rename checksum_sha256 to checksum
    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact' AND column_name='checksum_sha256') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact' AND column_name='checksum') THEN
            ALTER TABLE kf_artifact RENAME COLUMN checksum_sha256 TO checksum;
        END IF;
    END IF;
    
    -- Add new columns
    ALTER TABLE kf_artifact ADD COLUMN IF NOT EXISTS host_ip VARCHAR(100);
    ALTER TABLE kf_artifact ADD COLUMN IF NOT EXISTS hostname VARCHAR(255);
    ALTER TABLE kf_artifact ADD COLUMN IF NOT EXISTS user_name VARCHAR(255);
    ALTER TABLE kf_artifact ADD COLUMN IF NOT EXISTS resource_type VARCHAR(80);


    ---------------------------------------------------------------------------
    -- 2. kf_artifact_audit_log
    ---------------------------------------------------------------------------
    -- artifact_id was already renamed from resource_id in V31
    -- Rename actor_user to user_name
    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact_audit_log' AND column_name='actor_user') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_artifact_audit_log' AND column_name='user_name') THEN
            ALTER TABLE kf_artifact_audit_log RENAME COLUMN actor_user TO user_name;
        END IF;
    END IF;
    
    -- Add missing columns
    ALTER TABLE kf_artifact_audit_log ADD COLUMN IF NOT EXISTS host_ip VARCHAR(100);
    ALTER TABLE kf_artifact_audit_log ADD COLUMN IF NOT EXISTS created_by VARCHAR(128);
    ALTER TABLE kf_artifact_audit_log ADD COLUMN IF NOT EXISTS host_name VARCHAR(255);


    ---------------------------------------------------------------------------
    -- 3. kf_audit_logs
    ---------------------------------------------------------------------------
    -- Rename created_at to created_time
    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_audit_logs' AND column_name='created_at') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_audit_logs' AND column_name='created_time') THEN
            ALTER TABLE kf_audit_logs RENAME COLUMN created_at TO created_time;
        END IF;
    END IF;

    -- Rename actor_user to user_name
    IF EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_audit_logs' AND column_name='actor_user') THEN
        IF NOT EXISTS (SELECT FROM information_schema.columns WHERE table_name='kf_audit_logs' AND column_name='user_name') THEN
            ALTER TABLE kf_audit_logs RENAME COLUMN actor_user TO user_name;
        END IF;
    END IF;

END $$;
