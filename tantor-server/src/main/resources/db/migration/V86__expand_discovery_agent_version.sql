-- Feature-qualified external-agent versions exceed the original 50 characters.
ALTER TABLE kf_discovery_agents ALTER COLUMN version TYPE VARCHAR(128);
