-- Additive broker-only ownership; inventory is published only after verification.
CREATE TABLE kf_broker_addition_operation (
    id uuid PRIMARY KEY,
    cluster_id uuid NOT NULL REFERENCES kf_clusters(id) ON DELETE RESTRICT,
    host_id varchar(255) NOT NULL,
    job_id varchar(36) NOT NULL UNIQUE REFERENCES kf_jobs(id) ON DELETE RESTRICT,
    request_key varchar(128) NOT NULL,
    request_digest varchar(64) NOT NULL,
    node_id integer NOT NULL CHECK(node_id>0),
    state varchar(32) NOT NULL CHECK(state IN ('ACCEPTED','VALIDATING','PREPARING','PREPARED','FORMATTING','FORMATTED','STARTING','VERIFYING','RECOVERY_REQUIRED','SUCCEEDED','CANCELLED_SAFE')),
    plan text NOT NULL,
    last_evidence text,
    last_error text,
    version bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(cluster_id,request_key)
);
CREATE UNIQUE INDEX uq_broker_addition_reserved_host ON kf_broker_addition_operation(host_id)
    WHERE state NOT IN ('SUCCEEDED','CANCELLED_SAFE');
CREATE UNIQUE INDEX uq_broker_addition_reserved_node ON kf_broker_addition_operation(cluster_id,node_id)
    WHERE state NOT IN ('CANCELLED_SAFE');
CREATE UNIQUE INDEX uq_broker_addition_reserved_cluster ON kf_broker_addition_operation(cluster_id)
    WHERE state NOT IN ('SUCCEEDED','CANCELLED_SAFE');

CREATE TABLE kf_broker_addition_action (
    task_id uuid PRIMARY KEY REFERENCES kf_tasks(id) ON DELETE RESTRICT,
    operation_id uuid NOT NULL REFERENCES kf_broker_addition_operation(id) ON DELETE RESTRICT,
    step_id uuid NOT NULL REFERENCES kf_job_steps(id) ON DELETE RESTRICT,
    command varchar(64) NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE FUNCTION guard_broker_addition_host() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reserved uuid;
BEGIN
    SELECT id INTO reserved FROM kf_broker_addition_operation
      WHERE host_id=CASE WHEN TG_TABLE_NAME='kf_hosts' THEN (to_jsonb(NEW)->>'id') ELSE (to_jsonb(NEW)->>'host_id') END
        AND state NOT IN ('SUCCEEDED','CANCELLED_SAFE');
    IF reserved IS NOT NULL AND current_setting('tantor.broker_addition_id',true) IS DISTINCT FROM reserved::text THEN
        RAISE EXCEPTION 'Host is reserved by broker addition %',reserved;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_broker_addition_host_assignment BEFORE UPDATE OF cluster_id ON kf_hosts
    FOR EACH ROW WHEN (NEW.cluster_id IS DISTINCT FROM OLD.cluster_id) EXECUTE FUNCTION guard_broker_addition_host();
CREATE TRIGGER guard_broker_addition_service_assignment BEFORE INSERT OR UPDATE OF host_id ON kf_cluster_services
    FOR EACH ROW EXECUTE FUNCTION guard_broker_addition_host();

CREATE FUNCTION guard_broker_addition_task() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reserved uuid;
BEGIN
    SELECT id INTO reserved FROM kf_broker_addition_operation WHERE host_id=NEW.host_id
      AND state NOT IN ('SUCCEEDED','CANCELLED_SAFE');
    IF reserved IS NOT NULL
       AND NEW.status IN ('PENDING','IN_PROGRESS')
       AND NEW.command NOT IN ('INSPECT_ADD_NODE','PROBE_ADD_NODE','VALIDATE_ADD_BROKER_PLAN','INSPECT_ADD_BROKER','CHECK_PORTS','CHECK_PREREQUISITES','READ_KAFKA_PROPERTIES','READ_KAFKA_PROPERTY')
       AND current_setting('tantor.broker_addition_id',true) IS DISTINCT FROM reserved::text
       AND NOT EXISTS (SELECT 1 FROM kf_broker_addition_action a WHERE a.task_id=NEW.id AND a.operation_id=reserved AND a.command=NEW.command) THEN
        RAISE EXCEPTION 'Host mutation blocked by broker addition %',reserved;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_broker_addition_candidate_task BEFORE INSERT OR UPDATE OF status ON kf_tasks
    FOR EACH ROW EXECUTE FUNCTION guard_broker_addition_task();
