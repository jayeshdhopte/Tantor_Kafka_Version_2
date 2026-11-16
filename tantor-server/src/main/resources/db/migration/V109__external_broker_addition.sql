CREATE TABLE kf_external_broker_additions (
 id uuid PRIMARY KEY,
 cluster_id uuid NOT NULL REFERENCES kf_external_clusters(id) ON DELETE RESTRICT,
 agent_id varchar(255) NOT NULL REFERENCES kf_discovery_agents(id) ON DELETE RESTRICT,
 job_id varchar(36) NOT NULL UNIQUE REFERENCES kf_jobs(id) ON DELETE RESTRICT,
 request_key varchar(128) NOT NULL,
 request_digest varchar(64) NOT NULL,
 preview_id uuid NOT NULL,
 node_id integer NOT NULL CHECK(node_id>0),
 state varchar(40) NOT NULL DEFAULT 'ACCEPTED' CHECK(state IN ('ACCEPTED','VALIDATING','PREPARING','PREPARED','FORMATTING','FORMATTED','STARTING','STARTED_PENDING_VERIFICATION','RECOVERY_REQUIRED','SUCCEEDED','CANCELLED_SAFE')),
 plan text NOT NULL CHECK(octet_length(plan)<=524288),
 final_preview_id uuid,
 last_error text,
 last_evidence text,
 created_at timestamptz NOT NULL DEFAULT now(),
 updated_at timestamptz NOT NULL DEFAULT now(),
 UNIQUE(cluster_id,request_key)
);
CREATE UNIQUE INDEX uq_external_broker_reserved_agent ON kf_external_broker_additions(agent_id) WHERE state NOT IN ('SUCCEEDED','CANCELLED_SAFE');
CREATE UNIQUE INDEX uq_external_broker_reserved_cluster ON kf_external_broker_additions(cluster_id) WHERE state NOT IN ('SUCCEEDED','CANCELLED_SAFE');
CREATE UNIQUE INDEX uq_external_broker_reserved_node ON kf_external_broker_additions(cluster_id,node_id) WHERE state <> 'CANCELLED_SAFE';
CREATE TABLE kf_external_broker_tasks (
 id uuid PRIMARY KEY,
 operation_id uuid NOT NULL REFERENCES kf_external_broker_additions(id) ON DELETE RESTRICT,
 step_id uuid NOT NULL REFERENCES kf_job_steps(id) ON DELETE RESTRICT,
 command varchar(40) NOT NULL CHECK(command IN ('VALIDATE_ADD_BROKER_PLAN','PREPARE_ADD_BROKER','FORMAT_ADD_BROKER','START_ADD_BROKER','INSPECT_ADD_BROKER')),
 parameters text NOT NULL CHECK(octet_length(parameters)<=524288),
 status varchar(16) NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING','RUNNING','SUCCESS','FAILED')),
 claim_token uuid,
 result text CHECK(octet_length(result)<=131072),
 deadline timestamptz NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX idx_external_broker_task_claim ON kf_external_broker_tasks(status,created_at);
CREATE FUNCTION guard_external_broker_discovery() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reserved uuid;
BEGIN
 IF TG_TABLE_NAME='kf_discovery_agents' THEN
  SELECT id INTO reserved FROM kf_external_broker_additions WHERE agent_id=NEW.id AND state NOT IN ('SUCCEEDED','CANCELLED_SAFE');
 ELSE
  SELECT id INTO reserved FROM kf_external_broker_additions WHERE (cluster_id=NEW.cluster_id OR agent_id=NEW.agent_id) AND state NOT IN ('SUCCEEDED','CANCELLED_SAFE') LIMIT 1;
 END IF;
 IF reserved IS NOT NULL AND current_setting('tantor.external_broker_operation',true) IS DISTINCT FROM reserved::text THEN
  RAISE EXCEPTION 'External Add Node reservation % requires verified publication',reserved;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER external_broker_agent_ownership BEFORE UPDATE OF cluster_id ON kf_discovery_agents
 FOR EACH ROW WHEN (NEW.cluster_id IS DISTINCT FROM OLD.cluster_id) EXECUTE FUNCTION guard_external_broker_discovery();
CREATE TRIGGER external_broker_node_insert BEFORE INSERT ON kf_external_cluster_nodes FOR EACH ROW EXECUTE FUNCTION guard_external_broker_discovery();
CREATE TRIGGER external_broker_node_identity BEFORE UPDATE OF cluster_id,agent_id,node_id,host,config_file,systemd_service,is_broker,is_controller,jmx_exporter_port ON kf_external_cluster_nodes
 FOR EACH ROW WHEN ((NEW.cluster_id,NEW.agent_id,NEW.node_id,NEW.host,NEW.config_file,NEW.systemd_service,NEW.is_broker,NEW.is_controller,NEW.jmx_exporter_port)
 IS DISTINCT FROM (OLD.cluster_id,OLD.agent_id,OLD.node_id,OLD.host,OLD.config_file,OLD.systemd_service,OLD.is_broker,OLD.is_controller,OLD.jmx_exporter_port)) EXECUTE FUNCTION guard_external_broker_discovery();
CREATE FUNCTION guard_external_broker_node_delete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reserved uuid;
BEGIN
 SELECT id INTO reserved FROM kf_external_broker_additions WHERE (cluster_id=OLD.cluster_id OR agent_id=OLD.agent_id) AND state NOT IN ('SUCCEEDED','CANCELLED_SAFE') LIMIT 1;
 IF reserved IS NOT NULL AND current_setting('tantor.external_broker_operation',true) IS DISTINCT FROM reserved::text THEN RAISE EXCEPTION 'External Add Node reservation % protects participant inventory',reserved; END IF;
 RETURN OLD;
END $$;
CREATE TRIGGER external_broker_node_delete BEFORE DELETE ON kf_external_cluster_nodes FOR EACH ROW EXECUTE FUNCTION guard_external_broker_node_delete();
CREATE FUNCTION guard_external_broker_jobs() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reserved uuid;
BEGIN
 IF NEW.status IN ('PENDING','IN_PROGRESS','ROLLBACK_PENDING','ROLLING_BACK') THEN
  SELECT id INTO reserved FROM kf_external_broker_additions WHERE state NOT IN ('SUCCEEDED','CANCELLED_SAFE')
   AND NEW.resource_key IN ('cluster:'||cluster_id::text,'external-services:'||cluster_id::text) AND job_id<>NEW.id LIMIT 1;
  IF reserved IS NOT NULL THEN RAISE EXCEPTION 'External Add Node reservation % blocks another operation',reserved; END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER external_broker_job_ownership BEFORE INSERT OR UPDATE OF status,resource_key ON kf_jobs FOR EACH ROW EXECUTE FUNCTION guard_external_broker_jobs();

-- Serialize other external task dispatchers on the same candidate identity.
-- Normal read-only inspections and completed callbacks remain compatible.
CREATE FUNCTION guard_external_broker_other_tasks() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reserved uuid;
BEGIN
 IF NEW.status NOT IN ('PENDING','RUNNING','IN_PROGRESS') THEN RETURN NEW; END IF;
 IF TG_TABLE_NAME='kf_external_kafka_tasks' AND NEW.command IN ('READ_KAFKA_PROPERTY','READ_KAFKA_PROPERTIES','VERIFY_KAFKA_SERVICE') THEN RETURN NEW; END IF;
 IF TG_TABLE_NAME='kf_external_service_tasks' AND NEW.command='PRECHECK' THEN RETURN NEW; END IF;
 PERFORM id FROM kf_discovery_agents WHERE id=NEW.agent_id FOR UPDATE;
 SELECT id INTO reserved FROM kf_external_broker_additions WHERE agent_id=NEW.agent_id AND state NOT IN ('SUCCEEDED','CANCELLED_SAFE') LIMIT 1;
 IF reserved IS NOT NULL THEN RAISE EXCEPTION 'External Add Node reservation % protects the candidate from another task',reserved; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER external_broker_service_task BEFORE INSERT OR UPDATE OF status ON kf_external_service_tasks FOR EACH ROW EXECUTE FUNCTION guard_external_broker_other_tasks();
CREATE TRIGGER external_broker_legacy_kafka_task BEFORE INSERT OR UPDATE OF status ON kf_external_kafka_tasks FOR EACH ROW EXECUTE FUNCTION guard_external_broker_other_tasks();
