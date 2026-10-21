ALTER TABLE kf_external_cluster_nodes ADD COLUMN agent_id varchar(255), ADD COLUMN systemd_service varchar(255), ADD COLUMN can_manage_kafka boolean NOT NULL DEFAULT false, ADD COLUMN can_write_config boolean NOT NULL DEFAULT false, ADD COLUMN can_restart boolean NOT NULL DEFAULT false;
CREATE TABLE kf_external_kafka_tasks (
 id uuid PRIMARY KEY, cluster_id uuid NOT NULL REFERENCES kf_external_clusters(id) ON DELETE CASCADE,
 agent_id varchar(255) NOT NULL REFERENCES kf_discovery_agents(id), command varchar(48) NOT NULL,
 parameters text NOT NULL, status varchar(32) NOT NULL DEFAULT 'PENDING', message text,
 claim_token varchar(64), created_at timestamptz NOT NULL DEFAULT now(), deadline timestamptz NOT NULL
);
CREATE INDEX ix_external_kafka_tasks_claim ON kf_external_kafka_tasks(agent_id,status,created_at);
ALTER TABLE kf_job_steps ADD COLUMN external_kafka_task_id uuid REFERENCES kf_external_kafka_tasks(id) ON DELETE SET NULL;
ALTER TABLE kf_external_services DROP CONSTRAINT kf_external_services_cluster_id_role_key;
