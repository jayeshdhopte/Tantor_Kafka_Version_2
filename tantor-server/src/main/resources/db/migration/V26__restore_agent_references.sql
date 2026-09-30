-- V25 removed these columns while the corresponding entities and runtime
-- relationships still use them. Restore them without modifying an already
-- applied Flyway migration.
ALTER TABLE tasks
    ADD COLUMN IF NOT EXISTS agent_id UUID;

ALTER TABLE host_parcels
    ADD COLUMN IF NOT EXISTS agent_id UUID;

ALTER TABLE cluster_services
    ADD COLUMN IF NOT EXISTS agent_id UUID;

CREATE INDEX IF NOT EXISTS idx_tasks_agent_id ON tasks(agent_id);
CREATE INDEX IF NOT EXISTS idx_host_parcels_agent_id ON host_parcels(agent_id);
CREATE INDEX IF NOT EXISTS idx_cluster_services_agent_id ON cluster_services(agent_id);
