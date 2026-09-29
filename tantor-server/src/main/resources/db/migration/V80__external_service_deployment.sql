ALTER TABLE kf_discovery_agents ADD COLUMN can_deploy_services boolean NOT NULL DEFAULT false;
CREATE TABLE kf_external_services (
 id uuid PRIMARY KEY, cluster_id uuid NOT NULL REFERENCES kf_external_clusters(id) ON DELETE CASCADE,
 agent_id varchar(255) NOT NULL, role varchar(40) NOT NULL, status varchar(40) NOT NULL,
 config_json text NOT NULL, last_error text, UNIQUE(cluster_id, role)
);
CREATE TABLE kf_external_service_tasks (
 id uuid PRIMARY KEY, service_id uuid NOT NULL REFERENCES kf_external_services(id) ON DELETE CASCADE,
 agent_id varchar(255) NOT NULL, command varchar(32) NOT NULL, status varchar(32) NOT NULL,
 message text, created_at timestamptz NOT NULL, deadline timestamptz NOT NULL
);
CREATE INDEX ix_external_service_tasks_agent ON kf_external_service_tasks(agent_id, status, created_at);
ALTER TABLE kf_job_steps ADD COLUMN external_service_task_id uuid REFERENCES kf_external_service_tasks(id) ON DELETE SET NULL;
