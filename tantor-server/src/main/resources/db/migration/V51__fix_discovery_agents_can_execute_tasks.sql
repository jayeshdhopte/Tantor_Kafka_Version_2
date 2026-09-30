ALTER TABLE public.kf_discovery_agents
ADD COLUMN IF NOT EXISTS can_execute_tasks boolean NOT NULL DEFAULT false;