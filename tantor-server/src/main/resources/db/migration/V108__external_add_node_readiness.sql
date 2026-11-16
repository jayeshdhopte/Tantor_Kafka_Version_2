-- Read-only preview evidence, independent of deployment jobs and reservations.
CREATE TABLE kf_external_add_node_previews (
 id uuid PRIMARY KEY,
 cluster_id uuid NOT NULL REFERENCES kf_external_clusters(id) ON DELETE CASCADE,
 agent_id varchar(255) NOT NULL REFERENCES kf_discovery_agents(id) ON DELETE CASCADE,
 context_hash varchar(64) NOT NULL,
 request text NOT NULL CHECK (octet_length(request)<=65536),
 checks text NOT NULL CHECK (octet_length(checks)<=131072),
 created_at timestamptz NOT NULL DEFAULT now(),
 expires_at timestamptz NOT NULL
);
CREATE TABLE kf_external_add_node_tasks (
 id uuid PRIMARY KEY,
 preview_id uuid NOT NULL REFERENCES kf_external_add_node_previews(id) ON DELETE CASCADE,
 agent_id varchar(255) NOT NULL REFERENCES kf_discovery_agents(id) ON DELETE CASCADE,
 command varchar(16) NOT NULL CHECK (command IN ('INSPECT','PROBE')),
 parameters text NOT NULL CHECK (octet_length(parameters)<=65536),
 status varchar(16) NOT NULL DEFAULT 'PENDING' CHECK (status IN ('PENDING','RUNNING','SUCCESS','FAILED')),
 claim_token uuid,
 result text CHECK (octet_length(result)<=131072),
 deadline timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_external_readiness_task_claim ON kf_external_add_node_tasks(agent_id,status,created_at);
CREATE INDEX idx_external_readiness_candidate ON kf_external_add_node_previews(agent_id,expires_at);
