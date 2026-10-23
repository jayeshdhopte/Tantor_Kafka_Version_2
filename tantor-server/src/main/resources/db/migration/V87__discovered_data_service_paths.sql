ALTER TABLE kf_discovery_agents ADD COLUMN data_service_paths_json TEXT;
ALTER TABLE kf_discovery_agents ADD COLUMN service_paths_reported_at TIMESTAMPTZ;
