-- V38__add_discovery_agent_tasks_capability.sql
ALTER TABLE kf_discovery_agents ADD COLUMN IF NOT EXISTS can_execute_tasks BOOLEAN DEFAULT false;
