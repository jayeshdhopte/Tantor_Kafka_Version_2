-- One summary per standalone restart. Existing execution storage is unchanged.
-- The diagram repeats updated_at; it is represented once.
CREATE TABLE kf_rolling_restart_table (
    unique_id UUID PRIMARY KEY,
    cluster_id UUID,
    cluster_name TEXT,
    status TEXT,
    type TEXT,
    mode TEXT,
    env TEXT,
    bootstrap_server_ip TEXT,
    "user" TEXT,
    updated_by TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    role_per_node JSONB,
    log_path TEXT,
    host_id JSONB,
    old_config_path TEXT,
    action TEXT,
    resource_type TEXT
);
COMMENT ON TABLE kf_rolling_restart_table IS 'Standalone three-dot-menu restart summaries; one row per existing job ID; no backfill';
COMMENT ON COLUMN kf_rolling_restart_table.unique_id IS 'ER field; source: kf_jobs.id; same identifier, no foreign key';
COMMENT ON COLUMN kf_rolling_restart_table.cluster_id IS 'ER field; source: kf_jobs.payload.clusterId';
COMMENT ON COLUMN kf_rolling_restart_table.cluster_name IS 'ER field; snapshot of kf_clusters.cluster_name or kf_external_clusters.cluster_name';
COMMENT ON COLUMN kf_rolling_restart_table.status IS 'ER field; source: kf_jobs.status';
COMMENT ON COLUMN kf_rolling_restart_table.type IS 'ER field; New for managed inventory, Existing for external inventory';
COMMENT ON COLUMN kf_rolling_restart_table.mode IS 'ER field; snapshot of kf_clusters.mode or kf_external_clusters.kafka_mode';
COMMENT ON COLUMN kf_rolling_restart_table.env IS 'ER field; snapshot of cluster inventory environment';
COMMENT ON COLUMN kf_rolling_restart_table.bootstrap_server_ip IS 'ER field; snapshot of inventory bootstrap_servers; selected targets are in host_id and role_per_node';
COMMENT ON COLUMN kf_rolling_restart_table."user" IS 'ER field; source: kf_jobs.requested_by; requesting application user, not OS account';
COMMENT ON COLUMN kf_rolling_restart_table.updated_by IS 'ER field; original requester, replaced by authenticated retry requester; background transitions preserve that identity';
COMMENT ON COLUMN kf_rolling_restart_table.created_at IS 'ER field; source: kf_jobs.created_at; preserved on retry';
COMMENT ON COLUMN kf_rolling_restart_table.updated_at IS 'ER field; initial kf_jobs.updated_at, then actual time of summary status transition';
COMMENT ON COLUMN kf_rolling_restart_table.role_per_node IS 'ER field; JSON array from selected kf_job_steps payload hostId, role, nodeId, serviceName, hostname and ipAddress';
COMMENT ON COLUMN kf_rolling_restart_table.log_path IS 'ER field; EMPTY; restart logs remain in existing job, step and task text columns';
COMMENT ON COLUMN kf_rolling_restart_table.host_id IS 'ER field; JSON array of distinct selected kf_job_steps.target_id values';
COMMENT ON COLUMN kf_rolling_restart_table.old_config_path IS 'ER field; EMPTY; standalone restart does not modify or back up configuration';
COMMENT ON COLUMN kf_rolling_restart_table.action IS 'ER field; source: kf_jobs.type = ROLLING_RESTART';
COMMENT ON COLUMN kf_rolling_restart_table.resource_type IS 'ER field; CLUSTER, derived from kf_jobs.resource_key cluster prefix';
