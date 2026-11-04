-- Registration ownership is disposable; execution evidence is not.
ALTER TABLE kf_external_kafka_tasks
    ADD COLUMN source_cluster_id uuid,
    ADD COLUMN source_agent_id varchar(255),
    ADD COLUMN archived_at timestamptz;
UPDATE kf_external_kafka_tasks SET source_cluster_id=cluster_id, source_agent_id=agent_id;
ALTER TABLE kf_external_kafka_tasks
    ALTER COLUMN source_cluster_id SET NOT NULL,
    ALTER COLUMN source_agent_id SET NOT NULL,
    ALTER COLUMN cluster_id DROP NOT NULL,
    ALTER COLUMN agent_id DROP NOT NULL;

ALTER TABLE kf_external_kafka_tasks DROP CONSTRAINT kf_external_kafka_tasks_cluster_id_fkey;
ALTER TABLE kf_external_kafka_tasks ADD CONSTRAINT kf_external_kafka_tasks_cluster_id_fkey
    FOREIGN KEY (cluster_id) REFERENCES kf_external_clusters(id) ON DELETE SET NULL;
ALTER TABLE kf_external_kafka_tasks DROP CONSTRAINT kf_external_kafka_tasks_agent_id_fkey;
ALTER TABLE kf_external_kafka_tasks ADD CONSTRAINT kf_external_kafka_tasks_agent_id_fkey
    FOREIGN KEY (agent_id) REFERENCES kf_discovery_agents(id) ON DELETE SET NULL;
ALTER TABLE kf_job_steps DROP CONSTRAINT kf_job_steps_external_kafka_task_id_fkey;
ALTER TABLE kf_job_steps ADD CONSTRAINT kf_job_steps_external_kafka_task_id_fkey
    FOREIGN KEY (external_kafka_task_id) REFERENCES kf_external_kafka_tasks(id) ON DELETE RESTRICT;

CREATE FUNCTION public.retain_external_kafka_task_history() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        IF NEW.cluster_id IS NULL OR NEW.agent_id IS NULL THEN
            RAISE EXCEPTION 'New Kafka tasks require a live cluster and agent';
        END IF;
        NEW.source_cluster_id := NEW.cluster_id;
        NEW.source_agent_id := NEW.agent_id;
        NEW.archived_at := NULL;
        RETURN NEW;
    END IF;
    IF TG_OP='DELETE' THEN
        IF OLD.archived_at IS NOT NULL THEN
            RAISE EXCEPTION 'Archived external Kafka execution evidence is retained with job history';
        END IF;
        RETURN OLD;
    END IF;
    IF OLD.archived_at IS NOT NULL THEN
        -- A later agent/cluster deletion may detach the remaining live FK.
        -- No execution fields or original identity may change.
        IF (to_jsonb(NEW)-'cluster_id'-'agent_id') IS DISTINCT FROM
           (to_jsonb(OLD)-'cluster_id'-'agent_id')
           OR (NEW.cluster_id IS DISTINCT FROM OLD.cluster_id AND NEW.cluster_id IS NOT NULL)
           OR (NEW.agent_id IS DISTINCT FROM OLD.agent_id AND NEW.agent_id IS NOT NULL) THEN
            RAISE EXCEPTION 'Archived external Kafka execution evidence is immutable';
        END IF;
        RETURN NEW;
    END IF;
    IF NEW.source_cluster_id IS DISTINCT FROM OLD.source_cluster_id
       OR NEW.source_agent_id IS DISTINCT FROM OLD.source_agent_id
       OR NEW.archived_at IS DISTINCT FROM OLD.archived_at
       OR (NEW.cluster_id IS DISTINCT FROM OLD.cluster_id AND NEW.cluster_id IS NOT NULL)
       OR (NEW.agent_id IS DISTINCT FROM OLD.agent_id AND NEW.agent_id IS NOT NULL) THEN
        RAISE EXCEPTION 'External Kafka task ownership and provenance cannot be rewritten';
    END IF;
    IF NEW.cluster_id IS NULL OR NEW.agent_id IS NULL THEN
        NEW.archived_at := clock_timestamp();
        NEW.claim_token := NULL;
        IF OLD.status IN ('PENDING','IN_PROGRESS') THEN
            NEW.status := 'FAILED';
            NEW.message := concat_ws(E'\n',OLD.message,
                'Registration removed; task archived. Inspect the host before any new operation.');
        END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER retain_external_kafka_task_history
    BEFORE INSERT OR UPDATE OR DELETE ON kf_external_kafka_tasks
    FOR EACH ROW EXECUTE FUNCTION public.retain_external_kafka_task_history();

COMMENT ON COLUMN kf_external_kafka_tasks.source_cluster_id IS
    'Immutable original registration UUID, deliberately independent of disposable registration rows.';
COMMENT ON COLUMN kf_external_kafka_tasks.source_agent_id IS
    'Immutable original agent identity; retained after agent registration deletion.';
COMMENT ON COLUMN kf_external_kafka_tasks.archived_at IS
    'Registration detached. Execution evidence is retained indefinitely with job history; never claim or replay archived tasks.';
