-- The job resource index is the execution gate, not a distributed lease/fence.
-- Keep one reporting interval per forward/rollback attempt, including restarts.
ALTER TABLE public.kf_cluster_operation_lock
    ADD COLUMN job_id VARCHAR(36),
    ADD COLUMN attempt_number INTEGER,
    ADD COLUMN execution_phase VARCHAR(16),
    ADD COLUMN protection_mode VARCHAR(40),
    ADD COLUMN history_source VARCHAR(40),
    ADD COLUMN observed_at TIMESTAMPTZ,
    ADD COLUMN legacy_snapshot JSONB,
    ADD CONSTRAINT fk_operation_lock_job FOREIGN KEY (job_id) REFERENCES public.kf_jobs(id)
        ON DELETE NO ACTION DEFERRABLE INITIALLY DEFERRED;

CREATE UNIQUE INDEX uq_operation_lock_attempt ON public.kf_cluster_operation_lock(job_id,attempt_number,execution_phase)
    WHERE job_id IS NOT NULL AND attempt_number IS NOT NULL;

CREATE FUNCTION public.cluster_operation_has_unfinished_tasks(owner_id TEXT)
RETURNS BOOLEAN LANGUAGE sql STABLE SET search_path=public,pg_temp AS $$
    SELECT EXISTS (
        SELECT 1 FROM kf_job_steps s WHERE s.job_id=owner_id AND (
            EXISTS (SELECT 1 FROM kf_tasks t
                WHERE (t.id=s.agent_task_id OR t.id::text IN (s.payload->>'configMutationTaskId',s.payload->>'recoveryTaskId'))
                  AND (t.status IS NULL OR t.status NOT IN ('SUCCESS','FAILED','CANCELLED')))
            OR EXISTS (SELECT 1 FROM kf_external_kafka_tasks t
                WHERE (t.id=s.external_kafka_task_id OR t.id::text IN (s.payload->>'configMutationTaskId',s.payload->>'recoveryTaskId'))
                  AND (t.status IS NULL OR t.status NOT IN ('SUCCESS','FAILED','CANCELLED')))
        )
    );
$$;

CREATE FUNCTION public.sync_cluster_operation_lock(owner_id TEXT, snapshot_status TEXT DEFAULT NULL,
    snapshot_retry INTEGER DEFAULT NULL, event_time TIMESTAMPTZ DEFAULT NULL,
    reconstructed BOOLEAN DEFAULT FALSE, phase_override TEXT DEFAULT NULL)
RETURNS UUID LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
DECLARE
    owner kf_jobs%ROWTYPE;
    cluster_uuid UUID;
    config_uuid UUID;
    state TEXT;
    phase TEXT;
    attempt INTEGER;
    event_at TIMESTAMPTZ:=COALESCE(event_time,clock_timestamp());
    acquired TIMESTAMPTZ;
    finished BOOLEAN;
    waiting BOOLEAN;
    identity UUID;
BEGIN
    SELECT * INTO owner FROM kf_jobs WHERE id=owner_id;
    IF NOT FOUND OR owner.type NOT IN ('ROLLING_RESTART','TARGETED_CONFIG_CHANGE','CONFIG_CHANGE','ROLLING_CONFIG_UPDATE') THEN RETURN NULL; END IF;
    BEGIN
        cluster_uuid:=CASE WHEN owner.resource_key LIKE 'cluster:%' THEN substring(owner.resource_key FROM 9)::uuid
            ELSE (owner.payload->>'clusterId')::uuid END;
    EXCEPTION WHEN invalid_text_representation THEN RETURN NULL;
    END;
    IF cluster_uuid IS NULL THEN RETURN NULL; END IF;
    IF owner.type<>'ROLLING_RESTART' THEN
        SELECT change_id INTO config_uuid FROM kf_config_change WHERE change_id::text=owner_id;
    END IF;
    state:=COALESCE(snapshot_status,owner.status);
    attempt:=COALESCE(snapshot_retry,owner.retry_count,0)+1;
    phase:=COALESCE(phase_override,CASE WHEN state IN ('ROLLBACK_PENDING','ROLLING_BACK','ROLLED_BACK','ROLLBACK_FAILED') THEN 'ROLLBACK' ELSE 'APPLY' END);
    identity:=CASE WHEN attempt=1 AND phase='APPLY' THEN owner_id::uuid
        ELSE md5('cluster-operation:'||owner_id||':'||phase||':'||attempt)::uuid END;
    acquired:=CASE WHEN reconstructed AND attempt=1 AND phase='APPLY'
        THEN COALESCE(owner.created_at AT TIME ZONE 'UTC',event_at) ELSE event_at END;
    finished:=state IN ('SUCCESS','FAILED','PARTIAL_SUCCESS','ROLLED_BACK','ROLLBACK_FAILED');
    waiting:=finished AND NOT reconstructed AND cluster_operation_has_unfinished_tasks(owner_id);

    -- A resume transfers the same job's ownership; the old interval is not stretched.
    UPDATE kf_cluster_operation_lock SET lock_status='RELEASED',released_at=event_at,release_reason='ATTEMPT_HANDOFF'
    WHERE job_id=owner_id AND lock_id<>identity AND released_at IS NULL
      AND lock_status IN ('ACTIVE','WAITING_AGENT');

    -- Reconcile completed owners before admitting another live ledger interval.
    IF NOT reconstructed THEN
        UPDATE kf_cluster_operation_lock l SET lock_status='RELEASED',released_at=event_at,release_reason=j.status
        FROM kf_jobs j WHERE j.id=l.job_id AND l.cluster_id=cluster_uuid AND l.job_id<>owner_id
          AND l.history_source='LIVE_JOB_STATE' AND l.released_at IS NULL
          AND j.status IN ('SUCCESS','FAILED','PARTIAL_SUCCESS','ROLLED_BACK','ROLLBACK_FAILED')
          AND NOT cluster_operation_has_unfinished_tasks(j.id);
    END IF;
    INSERT INTO kf_cluster_operation_lock
        (lock_id,cluster_id,change_id,job_id,attempt_number,execution_phase,lock_type,lock_status,
         acquired_by,acquired_at,released_at,release_reason,protection_mode,history_source,observed_at)
    VALUES (identity,cluster_uuid,config_uuid,owner_id,attempt,phase,
        CASE WHEN owner.type='ROLLING_RESTART' THEN 'ROLLING_RESTART' ELSE 'CONFIG_CHANGE' END,
        CASE WHEN waiting THEN 'WAITING_AGENT' WHEN finished THEN 'RELEASED' ELSE 'ACTIVE' END,
        owner.requested_by,acquired,CASE WHEN finished AND NOT waiting THEN event_at END,
        CASE WHEN finished AND NOT waiting THEN state END,'JOB_RESOURCE_SERIALIZATION',
        CASE WHEN reconstructed THEN 'RECONSTRUCTED_JOB_LOG' ELSE 'LIVE_JOB_STATE' END,clock_timestamp())
    ON CONFLICT(lock_id) DO UPDATE SET
        cluster_id=EXCLUDED.cluster_id,change_id=EXCLUDED.change_id,job_id=EXCLUDED.job_id,
        attempt_number=EXCLUDED.attempt_number,execution_phase=EXCLUDED.execution_phase,
        lock_type=EXCLUDED.lock_type,lock_status=EXCLUDED.lock_status,
        acquired_by=COALESCE(kf_cluster_operation_lock.acquired_by,EXCLUDED.acquired_by),
        acquired_at=COALESCE(kf_cluster_operation_lock.acquired_at,EXCLUDED.acquired_at),
        released_at=EXCLUDED.released_at,release_reason=EXCLUDED.release_reason,
        protection_mode=EXCLUDED.protection_mode,history_source=EXCLUDED.history_source,observed_at=EXCLUDED.observed_at
    WHERE kf_cluster_operation_lock.released_at IS NULL;
    RETURN identity;
END;
$$;

-- Preserve original aggregate fields, including the misleading retry-wide intervals.
UPDATE kf_cluster_operation_lock l SET legacy_snapshot=to_jsonb(l),
    job_id=CASE WHEN EXISTS(SELECT 1 FROM kf_jobs j WHERE j.id=l.change_id::text) THEN l.change_id::text END,
    lock_status='HISTORY_UNKNOWN',released_at=NULL,release_reason=NULL,
    protection_mode='JOB_RESOURCE_SERIALIZATION',history_source='LEGACY_JOB_AGGREGATE';

DO $$
DECLARE
    owner RECORD;
    line RECORD;
    fields TEXT[];
    stamp TIMESTAMPTZ;
    message TEXT;
    retry INTEGER;
    phase TEXT;
    identity UUID;
    terminal_seen BOOLEAN;
BEGIN
    FOR owner IN SELECT * FROM kf_jobs WHERE type IN ('ROLLING_RESTART','TARGETED_CONFIG_CHANGE','CONFIG_CHANGE','ROLLING_CONFIG_UPDATE') ORDER BY created_at,id LOOP
        retry:=0;phase:='APPLY';terminal_seen:=FALSE;
        identity:=sync_cluster_operation_lock(owner.id,'PENDING',0,owner.created_at AT TIME ZONE 'UTC',TRUE,phase);
        FOR line IN SELECT value,ordinality FROM unnest(string_to_array(COALESCE(owner.logs,''),E'\n')) WITH ORDINALITY AS lines(value,ordinality) ORDER BY ordinality LOOP
            fields:=regexp_match(line.value,'^\[([^]]+)\] (.*)$');
            IF fields IS NULL THEN CONTINUE; END IF;
            BEGIN stamp:=fields[1]::timestamptz; EXCEPTION WHEN invalid_datetime_format THEN CONTINUE; END;
            message:=fields[2];
            IF message LIKE 'Retry requested;%' OR message='Rollback retry requested.' THEN
                retry:=retry+1;phase:=CASE WHEN message='Rollback retry requested.' THEN 'ROLLBACK' ELSE 'APPLY' END;
                PERFORM sync_cluster_operation_lock(owner.id,CASE WHEN phase='ROLLBACK' THEN 'ROLLBACK_PENDING' ELSE 'PENDING' END,retry,stamp,TRUE,phase);
                terminal_seen:=FALSE;
            ELSIF message LIKE 'Rollback requested by %' THEN
                phase:='ROLLBACK';terminal_seen:=FALSE;
                PERFORM sync_cluster_operation_lock(owner.id,'ROLLBACK_PENDING',retry,stamp,TRUE,phase);
            ELSIF message='Job execution completed successfully.' OR message LIKE 'Job execution failed:%'
                OR message='Rollback completed successfully.' OR message LIKE 'Rollback failed:%' THEN
                PERFORM sync_cluster_operation_lock(owner.id,CASE
                    WHEN message='Job execution completed successfully.' THEN 'SUCCESS'
                    WHEN message='Rollback completed successfully.' THEN 'ROLLED_BACK'
                    WHEN message LIKE 'Rollback failed:%' THEN 'ROLLBACK_FAILED'
                    WHEN retry=COALESCE(owner.retry_count,0) AND owner.status='PARTIAL_SUCCESS' THEN 'PARTIAL_SUCCESS'
                    ELSE 'FAILED' END,retry,stamp,TRUE,phase);
                terminal_seen:=TRUE;
            END IF;
        END LOOP;
        IF NOT terminal_seen AND retry=COALESCE(owner.retry_count,0) AND owner.end_time IS NOT NULL THEN
            PERFORM sync_cluster_operation_lock(owner.id,owner.status,retry,owner.end_time AT TIME ZONE 'UTC',TRUE,phase);
        END IF;
    END LOOP;
END;
$$;

SET CONSTRAINTS ALL IMMEDIATE;
CREATE UNIQUE INDEX uq_operation_lock_live_cluster ON public.kf_cluster_operation_lock(cluster_id)
    WHERE history_source='LIVE_JOB_STATE' AND lock_status IN ('ACTIVE','WAITING_AGENT');
SET CONSTRAINTS ALL DEFERRED;

CREATE FUNCTION public.track_cluster_operation_job() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
BEGIN
    IF TG_OP='INSERT' THEN
        PERFORM sync_cluster_operation_lock(NEW.id,NEW.status,NEW.retry_count,clock_timestamp());
    ELSIF NEW.status IS DISTINCT FROM OLD.status OR NEW.retry_count IS DISTINCT FROM OLD.retry_count THEN
        PERFORM sync_cluster_operation_lock(NEW.id,NEW.status,NEW.retry_count,clock_timestamp());
    END IF;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER trg_cluster_operation_job AFTER INSERT OR UPDATE ON public.kf_jobs
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.track_cluster_operation_job();

CREATE FUNCTION public.track_cluster_operation_task() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
DECLARE owner RECORD;
BEGIN
    IF TG_OP='UPDATE' AND NEW.status IS NOT DISTINCT FROM OLD.status THEN RETURN NEW; END IF;
    FOR owner IN SELECT DISTINCT s.job_id FROM kf_job_steps s
        WHERE NEW.id IN (s.agent_task_id,s.external_kafka_task_id)
           OR NEW.id::text IN (s.payload->>'configMutationTaskId',s.payload->>'recoveryTaskId') LOOP
        PERFORM sync_cluster_operation_lock(owner.job_id);
    END LOOP;
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER trg_cluster_operation_internal_task AFTER INSERT OR UPDATE ON public.kf_tasks
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.track_cluster_operation_task();
CREATE CONSTRAINT TRIGGER trg_cluster_operation_external_task AFTER INSERT OR UPDATE ON public.kf_external_kafka_tasks
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.track_cluster_operation_task();

CREATE FUNCTION public.track_cluster_operation_step() RETURNS TRIGGER LANGUAGE plpgsql SET search_path=public,pg_temp AS $$
BEGIN
    PERFORM sync_cluster_operation_lock(NEW.job_id);
    RETURN NEW;
END;
$$;
CREATE CONSTRAINT TRIGGER trg_cluster_operation_step AFTER INSERT OR UPDATE ON public.kf_job_steps
    DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION public.track_cluster_operation_step();

COMMENT ON COLUMN public.kf_cluster_operation_lock.change_id IS 'Config Change FK only; NULL for standalone restart. job_id identifies both operation families.';
COMMENT ON COLUMN public.kf_cluster_operation_lock.fencing_token IS 'NOT ENABLED: no distributed monotonic-token fencing contract. Protection is explicitly recorded in protection_mode; do not treat attempt_number as a fencing token.';
COMMENT ON COLUMN public.kf_cluster_operation_lock.lease_expires_at IS 'No cluster-operation lease expiration in JOB_RESOURCE_SERIALIZATION mode; stalled work is not released by a TTL.';
COMMENT ON COLUMN public.kf_cluster_operation_lock.legacy_snapshot IS 'Unmodified pre-V101 aggregate lock record preserved before reconstructing per-attempt history.';
COMMENT ON TABLE public.kf_cluster_operation_lock IS 'Shared per-attempt Config Change and Rolling Restart reporting. kf_jobs active-resource uniqueness plus task-drain checks gate execution; LIVE_JOB_STATE active intervals are additionally unique per cluster. Fencing tokens are not implemented.';
