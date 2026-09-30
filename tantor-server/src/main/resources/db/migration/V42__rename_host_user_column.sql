-- V42: Rename reserved keyword column "user" to "user_name" in kf_hosts.
-- Keep this idempotent because some environments may already have user_name.

DO $$
BEGIN
    IF EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_name = 'kf_hosts' AND column_name = 'user'
    ) AND NOT EXISTS (
        SELECT FROM information_schema.columns
        WHERE table_name = 'kf_hosts' AND column_name = 'user_name'
    ) THEN
        ALTER TABLE kf_hosts RENAME COLUMN "user" TO user_name;
    ELSE
        ALTER TABLE kf_hosts ADD COLUMN IF NOT EXISTS user_name VARCHAR(128);
    END IF;
END $$;
