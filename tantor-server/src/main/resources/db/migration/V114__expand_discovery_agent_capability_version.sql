-- Capability-bearing agent versions must be retained in full for feature checks.
ALTER TABLE kf_discovery_agents ALTER COLUMN version TYPE TEXT;
