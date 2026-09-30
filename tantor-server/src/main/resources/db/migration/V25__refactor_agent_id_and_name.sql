-- Drop the auto-increment host_number sequence and column
ALTER TABLE hosts DROP COLUMN host_number;

-- Drop the UUID agent_id columns since we will now use 'id' for the UUID
ALTER TABLE hosts DROP COLUMN agent_id;
ALTER TABLE tasks DROP COLUMN agent_id;
ALTER TABLE host_parcels DROP COLUMN agent_id;
ALTER TABLE cluster_services DROP COLUMN agent_id;

-- Add agent_name back as a regular column
ALTER TABLE hosts ADD COLUMN agent_name VARCHAR(255);

-- Wipe the old data so the agent can re-register with its new UUID Primary Key
TRUNCATE TABLE hosts CASCADE;
