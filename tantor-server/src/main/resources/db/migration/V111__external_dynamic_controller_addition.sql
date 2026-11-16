-- External dynamic controller work has a separate queue and typed frozen plan.
CREATE TABLE kf_external_controller_previews(
 id uuid PRIMARY KEY,cluster_id uuid NOT NULL REFERENCES kf_external_clusters(id),agent_id varchar(255) NOT NULL REFERENCES kf_discovery_agents(id),
 request text NOT NULL,context_hash varchar(64) NOT NULL,binding_hash varchar(64) NOT NULL,common_request text NOT NULL,
 plan text,plan_digest varchar(64),created_at timestamptz NOT NULL DEFAULT clock_timestamp(),expires_at timestamptz NOT NULL
);
CREATE TABLE kf_external_controller_operations(
 id uuid PRIMARY KEY REFERENCES kf_external_controller_previews(id),cluster_id uuid NOT NULL REFERENCES kf_external_clusters(id),agent_id varchar(255) NOT NULL REFERENCES kf_discovery_agents(id),
 node_id integer NOT NULL CHECK(node_id>0),job_id varchar(36) NOT NULL UNIQUE REFERENCES kf_jobs(id),request_key varchar(128) NOT NULL,request_digest varchar(64) NOT NULL,
 state varchar(40) NOT NULL DEFAULT 'ACCEPTED' CHECK(state IN ('ACCEPTED','RECONCILING','VALIDATING','PREPARING','PREPARED','FORMATTING','FORMATTED','STARTING','OBSERVER_STARTED','CATCHING_UP','ADMITTING','VOTER_VERIFIED','VERIFYING','PUBLISHING','RECOVERY_REQUIRED','SUCCEEDED','CANCELLED_SAFE')),
 resume_generation integer NOT NULL DEFAULT 0,last_error text,last_evidence text,verification text,published_node_id uuid,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(),updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),UNIQUE(cluster_id,request_key)
);
CREATE UNIQUE INDEX uq_external_controller_cluster ON kf_external_controller_operations(cluster_id) WHERE state NOT IN ('SUCCEEDED','CANCELLED_SAFE');
CREATE UNIQUE INDEX uq_external_controller_agent ON kf_external_controller_operations(agent_id) WHERE state NOT IN ('SUCCEEDED','CANCELLED_SAFE');
CREATE TABLE kf_external_controller_tasks(
 id uuid PRIMARY KEY,preview_id uuid NOT NULL REFERENCES kf_external_controller_previews(id),operation_id uuid REFERENCES kf_external_controller_operations(id),
 step_id uuid REFERENCES kf_job_steps(id),agent_id varchar(255) NOT NULL REFERENCES kf_discovery_agents(id),
 command varchar(16) NOT NULL CHECK(command IN ('RUNTIME','VALIDATE','INSPECT','PREPARE','FORMAT','START','ADMIT','RECONCILE_FORMAT')),parameters text NOT NULL CHECK(octet_length(parameters)<=2097152),
 status varchar(16) NOT NULL DEFAULT 'PENDING' CHECK(status IN ('PENDING','RUNNING','SUCCESS','FAILED')),
 claim_token uuid,result text CHECK(octet_length(result)<=2097152),deadline timestamptz NOT NULL,claimed_at timestamptz,retired_at timestamptz,
 resume_generation integer NOT NULL DEFAULT 0,created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE INDEX ix_external_controller_claim ON kf_external_controller_tasks(agent_id,status,created_at);
CREATE VIEW kf_external_add_node_owners AS
 SELECT id,cluster_id,agent_id,node_id,job_id,state FROM kf_external_broker_additions
 UNION ALL SELECT id,cluster_id,agent_id,node_id,job_id,state FROM kf_external_controller_operations;
-- Shared arbitration also protects retained IDs after verified publication.
CREATE TABLE kf_external_add_node_resources(resource_key text PRIMARY KEY,operation_id uuid NOT NULL,permanent boolean NOT NULL);
CREATE FUNCTION sync_external_add_node_resources() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE key text;keep boolean;
BEGIN
 IF TG_OP='UPDATE' AND (NEW.id<>OLD.id OR NEW.cluster_id<>OLD.cluster_id OR NEW.agent_id<>OLD.agent_id OR NEW.node_id<>OLD.node_id OR NEW.job_id<>OLD.job_id OR NEW.request_key<>OLD.request_key OR NEW.request_digest<>OLD.request_digest) THEN RAISE EXCEPTION 'Frozen external Add Node ownership cannot change'; END IF;
 IF TG_OP='UPDATE' AND OLD.state IN ('SUCCEEDED','CANCELLED_SAFE') AND NEW.state<>OLD.state THEN RAISE EXCEPTION 'Terminal external Add Node operation cannot reopen'; END IF;
 IF NEW.state='CANCELLED_SAFE' THEN
   IF TG_TABLE_NAME='kf_external_controller_operations' AND EXISTS(SELECT 1 FROM kf_external_controller_tasks WHERE operation_id=NEW.id AND command IN ('PREPARE','FORMAT','START','ADMIT','RECONCILE_FORMAT')) THEN RAISE EXCEPTION 'Controller mutation was dispatched; retain ownership';END IF;
   DELETE FROM kf_external_add_node_resources WHERE operation_id=NEW.id;RETURN NEW;
 END IF;
 FOREACH key IN ARRAY ARRAY['node:'||NEW.cluster_id||':'||NEW.node_id,'cluster:'||NEW.cluster_id,'agent:'||NEW.agent_id] LOOP
  keep=key LIKE 'node:%';
  IF NEW.state='SUCCEEDED' AND NOT keep THEN CONTINUE;END IF;
  INSERT INTO kf_external_add_node_resources AS r VALUES(key,NEW.id,keep) ON CONFLICT(resource_key) DO UPDATE SET permanent=EXCLUDED.permanent WHERE r.operation_id=EXCLUDED.operation_id;
  IF NOT FOUND THEN RAISE EXCEPTION 'External Add Node resource retained: %',key;END IF;
 END LOOP;
 IF NEW.state='SUCCEEDED' THEN DELETE FROM kf_external_add_node_resources WHERE operation_id=NEW.id AND NOT permanent;END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER external_broker_shared_resources AFTER INSERT OR UPDATE ON kf_external_broker_additions FOR EACH ROW EXECUTE FUNCTION sync_external_add_node_resources();
CREATE TRIGGER external_controller_shared_resources AFTER INSERT OR UPDATE ON kf_external_controller_operations FOR EACH ROW EXECUTE FUNCTION sync_external_add_node_resources();
UPDATE kf_external_broker_additions SET updated_at=updated_at WHERE state<>'CANCELLED_SAFE';

CREATE OR REPLACE FUNCTION guard_external_broker_discovery() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reserved uuid;
BEGIN
 IF TG_TABLE_NAME='kf_discovery_agents' THEN
  SELECT id INTO reserved FROM kf_external_add_node_owners WHERE agent_id=NEW.id AND state NOT IN ('SUCCEEDED','CANCELLED_SAFE');
 ELSE
  SELECT id INTO reserved FROM kf_external_add_node_owners WHERE (cluster_id=NEW.cluster_id OR agent_id=NEW.agent_id) AND state NOT IN ('SUCCEEDED','CANCELLED_SAFE') LIMIT 1;
 END IF;
 IF reserved IS NOT NULL AND current_setting('tantor.external_broker_operation',true) IS DISTINCT FROM reserved::text THEN
  RAISE EXCEPTION 'External Add Node reservation % requires verified publication',reserved;
 END IF;
 RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION guard_external_broker_node_delete() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reserved uuid;
BEGIN
 SELECT id INTO reserved FROM kf_external_add_node_owners WHERE (cluster_id=OLD.cluster_id OR agent_id=OLD.agent_id) AND state NOT IN ('SUCCEEDED','CANCELLED_SAFE') LIMIT 1;
 IF reserved IS NOT NULL AND current_setting('tantor.external_broker_operation',true) IS DISTINCT FROM reserved::text THEN RAISE EXCEPTION 'External Add Node reservation % protects participant inventory',reserved; END IF;
 RETURN OLD;
END $$;

CREATE OR REPLACE FUNCTION guard_external_broker_jobs() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reserved uuid;
BEGIN
 IF NEW.status IN ('PENDING','IN_PROGRESS','ROLLBACK_PENDING','ROLLING_BACK') THEN
  SELECT id INTO reserved FROM kf_external_add_node_owners WHERE state NOT IN ('SUCCEEDED','CANCELLED_SAFE')
   AND NEW.resource_key IN ('cluster:'||cluster_id::text,'external-services:'||cluster_id::text) AND job_id<>NEW.id LIMIT 1;
  IF reserved IS NOT NULL THEN RAISE EXCEPTION 'External Add Node reservation % blocks another operation',reserved; END IF;
 END IF;
 RETURN NEW;
END $$;

CREATE OR REPLACE FUNCTION guard_external_broker_other_tasks() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reserved uuid;
BEGIN
 IF NEW.status NOT IN ('PENDING','RUNNING','IN_PROGRESS') THEN RETURN NEW; END IF;
 IF TG_TABLE_NAME='kf_external_kafka_tasks' AND NEW.command IN ('READ_KAFKA_PROPERTY','READ_KAFKA_PROPERTIES','VERIFY_KAFKA_SERVICE') THEN RETURN NEW; END IF;
 IF TG_TABLE_NAME='kf_external_service_tasks' AND NEW.command='PRECHECK' THEN RETURN NEW; END IF;
 PERFORM id FROM kf_discovery_agents WHERE id=NEW.agent_id FOR UPDATE;
 SELECT id INTO reserved FROM kf_external_add_node_owners WHERE (agent_id=NEW.agent_id OR cluster_id IN (SELECT cluster_id FROM kf_external_cluster_nodes WHERE agent_id=NEW.agent_id)) AND state NOT IN ('SUCCEEDED','CANCELLED_SAFE') LIMIT 1;
 IF reserved IS NOT NULL THEN RAISE EXCEPTION 'External Add Node reservation % protects the candidate from another task',reserved; END IF;
 RETURN NEW;
END $$;

CREATE FUNCTION freeze_external_controller_preview() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF (NEW.id,NEW.cluster_id,NEW.agent_id,NEW.request,NEW.context_hash,NEW.binding_hash,NEW.common_request,NEW.expires_at) IS DISTINCT FROM (OLD.id,OLD.cluster_id,OLD.agent_id,OLD.request,OLD.context_hash,OLD.binding_hash,OLD.common_request,OLD.expires_at) OR OLD.plan IS NOT NULL AND (NEW.plan,NEW.plan_digest) IS DISTINCT FROM (OLD.plan,OLD.plan_digest) THEN RAISE EXCEPTION 'Frozen controller review cannot change'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER external_controller_preview_frozen BEFORE UPDATE ON kf_external_controller_previews FOR EACH ROW EXECUTE FUNCTION freeze_external_controller_preview();
