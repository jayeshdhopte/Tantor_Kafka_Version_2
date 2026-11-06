-- One parent owns controller-only or two-process expansion, including recovery.
CREATE TABLE kf_quorum_expansion_operation (
    id uuid PRIMARY KEY,
    cluster_id uuid NOT NULL REFERENCES kf_clusters(id) ON DELETE RESTRICT,
    host_id varchar(255) NOT NULL,
    participant_hosts text[] NOT NULL CHECK(cardinality(participant_hosts) BETWEEN 1 AND 128),
    controller_id integer NOT NULL CHECK(controller_id>0),
    broker_id integer CHECK(broker_id>0 AND broker_id<>controller_id),
    job_id varchar(36) NOT NULL UNIQUE REFERENCES kf_jobs(id) ON DELETE RESTRICT,
    request_key varchar(128) NOT NULL,
    request_digest varchar(64) NOT NULL,
    plan text NOT NULL,
    plan_digest varchar(64) NOT NULL,
    checkpoints text NOT NULL DEFAULT '{}',
    state varchar(32) NOT NULL DEFAULT 'ACCEPTED' CHECK(state IN ('ACCEPTED','EXECUTING','RECOVERY_REQUIRED','SUCCEEDED','CANCELLED_SAFE')),
    last_evidence text,
    last_error text,
    version bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(cluster_id,request_key)
);
CREATE TABLE kf_quorum_expansion_action (
    task_id uuid PRIMARY KEY REFERENCES kf_tasks(id) ON DELETE RESTRICT,
    operation_id uuid NOT NULL REFERENCES kf_quorum_expansion_operation(id) ON DELETE RESTRICT,
    step_id uuid NOT NULL REFERENCES kf_job_steps(id) ON DELETE RESTRICT,
    workflow_step varchar(512) NOT NULL,
    command varchar(64) NOT NULL,
    action varchar(32) NOT NULL,
    mutation boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ix_quorum_expansion_action_operation ON kf_quorum_expansion_action(operation_id,workflow_step,created_at);

-- Shared keys arbitrate with existing broker additions under concurrent accepts.
-- Cluster/host leases end only on success or safe cancellation. Published node
-- identities remain owned after success, as in the existing broker ledger.
CREATE TABLE kf_add_node_resource_reservation (
    resource_key text PRIMARY KEY,
    operation_id uuid NOT NULL,
    family varchar(16) NOT NULL CHECK(family IN ('BROKER','EXPANSION')),
    permanent boolean NOT NULL DEFAULT false
);
CREATE INDEX ix_add_node_resource_owner ON kf_add_node_resource_reservation(operation_id,family);
CREATE FUNCTION claim_add_node_resource(key text,owner_id uuid,owner_family text,keep boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO kf_add_node_resource_reservation AS r(resource_key,operation_id,family,permanent)
      VALUES(key,owner_id,owner_family,keep)
      ON CONFLICT(resource_key) DO UPDATE SET permanent=EXCLUDED.permanent
        WHERE r.operation_id=EXCLUDED.operation_id AND r.family=EXCLUDED.family;
    IF NOT FOUND THEN RAISE EXCEPTION 'Add Node resource is retained by another operation: %',key; END IF;
END $$;
CREATE FUNCTION sync_add_node_resources() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE family_name text; participant text;
BEGIN
    family_name=CASE WHEN TG_TABLE_NAME='kf_broker_addition_operation' THEN 'BROKER' ELSE 'EXPANSION' END;
    IF TG_OP='UPDATE' AND (NEW.id<>OLD.id OR NEW.cluster_id<>OLD.cluster_id OR NEW.host_id<>OLD.host_id OR NEW.job_id<>OLD.job_id OR NEW.request_key<>OLD.request_key OR NEW.request_digest<>OLD.request_digest) THEN
        RAISE EXCEPTION 'Frozen Add Node ownership cannot change';
    END IF;
    IF family_name='BROKER' THEN
        IF TG_OP='UPDATE' AND NEW.node_id<>OLD.node_id THEN RAISE EXCEPTION 'Frozen broker identity cannot change'; END IF;
    ELSE
        IF TG_OP='UPDATE' AND (NEW.controller_id<>OLD.controller_id OR NEW.broker_id IS DISTINCT FROM OLD.broker_id OR NEW.participant_hosts<>OLD.participant_hosts OR NEW.plan<>OLD.plan OR NEW.plan_digest<>OLD.plan_digest) THEN
            RAISE EXCEPTION 'Frozen expansion plan cannot change';
        END IF;
        IF NEW.state='CANCELLED_SAFE' AND EXISTS(SELECT 1 FROM kf_quorum_expansion_action WHERE operation_id=NEW.id AND mutation) THEN
            RAISE EXCEPTION 'Expansion mutation was dispatched; retain ownership and reconcile';
        END IF;
    END IF;
    IF TG_OP='UPDATE' AND OLD.state IN ('SUCCEEDED','CANCELLED_SAFE') AND NEW.state<>OLD.state THEN RAISE EXCEPTION 'Terminal Add Node ownership cannot reopen'; END IF;
    IF NEW.state='CANCELLED_SAFE' THEN
        DELETE FROM kf_add_node_resource_reservation WHERE operation_id=NEW.id AND family=family_name;
        RETURN NEW;
    END IF;
    IF family_name='BROKER' THEN
        PERFORM claim_add_node_resource('node:'||NEW.cluster_id||':'||NEW.node_id,NEW.id,family_name,true);
    ELSE
        PERFORM claim_add_node_resource('node:'||NEW.cluster_id||':'||NEW.controller_id,NEW.id,family_name,true);
        IF NEW.broker_id IS NOT NULL THEN PERFORM claim_add_node_resource('node:'||NEW.cluster_id||':'||NEW.broker_id,NEW.id,family_name,true); END IF;
    END IF;
    IF NEW.state='SUCCEEDED' THEN
        DELETE FROM kf_add_node_resource_reservation WHERE operation_id=NEW.id AND family=family_name AND NOT permanent;
        RETURN NEW;
    END IF;
    PERFORM claim_add_node_resource('cluster:'||NEW.cluster_id,NEW.id,family_name,false);
    PERFORM claim_add_node_resource('host:'||NEW.host_id,NEW.id,family_name,false);
    IF family_name='EXPANSION' THEN
        FOREACH participant IN ARRAY NEW.participant_hosts LOOP
            IF participant IS NULL OR btrim(participant)='' THEN RAISE EXCEPTION 'Empty participant host reservation'; END IF;
            PERFORM claim_add_node_resource('host:'||participant,NEW.id,family_name,false);
        END LOOP;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER sync_broker_addition_shared_resources AFTER INSERT OR UPDATE ON kf_broker_addition_operation
    FOR EACH ROW EXECUTE FUNCTION sync_add_node_resources();
CREATE TRIGGER sync_quorum_expansion_resources AFTER INSERT OR UPDATE ON kf_quorum_expansion_operation
    FOR EACH ROW EXECUTE FUNCTION sync_add_node_resources();

-- Backfill through the same atomic ownership path. Existing unique indexes in
-- V105 already rule out competing broker cluster/host/node reservations.
UPDATE kf_broker_addition_operation SET updated_at=updated_at WHERE state<>'CANCELLED_SAFE';

CREATE FUNCTION guard_quorum_expansion_host() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reserved uuid; target text;
BEGIN
    target=CASE WHEN TG_TABLE_NAME='kf_hosts' THEN to_jsonb(NEW)->>'id' ELSE to_jsonb(NEW)->>'host_id' END;
    SELECT operation_id INTO reserved FROM kf_add_node_resource_reservation WHERE resource_key='host:'||target AND family='EXPANSION';
    IF reserved IS NOT NULL AND current_setting('tantor.quorum_expansion_id',true) IS DISTINCT FROM reserved::text THEN
        RAISE EXCEPTION 'Host is retained by quorum expansion %',reserved;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_quorum_expansion_host_assignment BEFORE UPDATE OF cluster_id ON kf_hosts
    FOR EACH ROW WHEN (NEW.cluster_id IS DISTINCT FROM OLD.cluster_id) EXECUTE FUNCTION guard_quorum_expansion_host();
CREATE TRIGGER guard_quorum_expansion_service_assignment BEFORE INSERT OR UPDATE OF host_id ON kf_cluster_services
    FOR EACH ROW EXECUTE FUNCTION guard_quorum_expansion_host();

CREATE FUNCTION guard_quorum_expansion_task() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE reserved uuid;
BEGIN
    SELECT operation_id INTO reserved FROM kf_add_node_resource_reservation WHERE resource_key='host:'||NEW.host_id AND family='EXPANSION';
    IF reserved IS NOT NULL AND NEW.status IN ('PENDING','IN_PROGRESS')
      AND NEW.command NOT IN ('INSPECT_QUORUM_RUNTIME','INSPECT_ADD_NODE_PROCESS','INSPECT_ADD_NODE','PROBE_ADD_NODE','CHECK_PORTS','CHECK_PREREQUISITES','READ_KAFKA_PROPERTIES','READ_KAFKA_PROPERTY')
      AND current_setting('tantor.quorum_expansion_id',true) IS DISTINCT FROM reserved::text
      AND NOT EXISTS(SELECT 1 FROM kf_quorum_expansion_action a WHERE a.task_id=NEW.id AND a.operation_id=reserved AND a.command=NEW.command) THEN
        RAISE EXCEPTION 'Host mutation blocked by quorum expansion %',reserved;
    END IF;
    RETURN NEW;
END $$;
CREATE TRIGGER guard_quorum_expansion_reserved_task BEFORE INSERT OR UPDATE OF status ON kf_tasks
    FOR EACH ROW EXECUTE FUNCTION guard_quorum_expansion_task();
