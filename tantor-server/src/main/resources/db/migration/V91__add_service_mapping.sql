-- Additive service mapping. Existing deployment/job/connection storage is unchanged.
-- The diagram's "host_id/fqdn" label is represented by host_id (stable inventory ID).
CREATE TABLE kf_add_service (
    add_service_id UUID PRIMARY KEY,
    cluster_id UUID,
    created_time TIMESTAMPTZ,
    status TEXT,
    host_id TEXT,
    broker_ip TEXT,
    "user" TEXT,
    created_by TEXT,
    updated_by TEXT,
    secured BOOLEAN,
    security_protocol TEXT,
    security_mechanism TEXT,
    trust_store_location TEXT,
    user_security TEXT,
    password_security TEXT,
    process_role TEXT
);

COMMENT ON TABLE kf_add_service IS 'Add-service mapping for new Schema Registry and Kafka Connect deployments; existing service ID reused; no historical backfill';
COMMENT ON COLUMN kf_add_service.add_service_id IS 'ER field; source: kf_cluster_services.id or kf_external_services.id';
COMMENT ON COLUMN kf_add_service.cluster_id IS 'ER field; source: kf_cluster_services.cluster_id or kf_external_services.cluster_id; plain ID, no foreign key';
COMMENT ON COLUMN kf_add_service.created_time IS 'ER field; source: application clock when the new service assignment is recorded; preserved on retry';
COMMENT ON COLUMN kf_add_service.status IS 'ER field; source: kf_cluster_services.status or kf_external_services.status';
COMMENT ON COLUMN kf_add_service.host_id IS 'ER field; source: kf_cluster_services.host_id or kf_external_services.agent_id; approved stable host/agent ID mapping for host_id/fqdn';
COMMENT ON COLUMN kf_add_service.broker_ip IS 'ER field; source: service config_json.bootstrap_servers; approved full bootstrap address list mapping';
COMMENT ON COLUMN kf_add_service."user" IS 'ER field; source: service config_json.service_user; OS service account';
COMMENT ON COLUMN kf_add_service.created_by IS 'ER field; source: authenticated Add Service requester';
COMMENT ON COLUMN kf_add_service.updated_by IS 'ER field; source: authenticated requester or kf_jobs.requested_by for status transitions';
COMMENT ON COLUMN kf_add_service.secured IS 'ER field; EMPTY, to be clarified; current service payloads do not record an explicit security setting';
COMMENT ON COLUMN kf_add_service.security_protocol IS 'ER field; EMPTY, to be clarified; current service payloads do not record an explicit security setting';
COMMENT ON COLUMN kf_add_service.security_mechanism IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN kf_add_service.trust_store_location IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN kf_add_service.user_security IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN kf_add_service.password_security IS 'ER field; EMPTY, to be clarified; no plaintext credentials copied';
COMMENT ON COLUMN kf_add_service.process_role IS 'ER field; source: kf_cluster_services.role or kf_external_services.role';
