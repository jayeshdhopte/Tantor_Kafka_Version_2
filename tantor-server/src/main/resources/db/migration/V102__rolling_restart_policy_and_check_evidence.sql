-- Standalone restart evidence; existing Config Change health rows remain separate.
CREATE TABLE public.kf_rolling_restart_check_definition (
    definition_version TEXT NOT NULL,
    check_key TEXT NOT NULL,
    description TEXT NOT NULL,
    rule TEXT NOT NULL,
    PRIMARY KEY(definition_version,check_key)
);
CREATE TABLE public.kf_rolling_restart_policy (
    policy_id UUID PRIMARY KEY,
    job_id VARCHAR(36) NOT NULL REFERENCES public.kf_jobs(id) ON DELETE NO ACTION,
    attempt_number INTEGER NOT NULL CHECK(attempt_number>0),
    policy_version TEXT NOT NULL,
    definition_version TEXT NOT NULL,
    settings JSONB NOT NULL,
    captured_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    UNIQUE(job_id,attempt_number),
    UNIQUE(policy_id,definition_version)
);
CREATE TABLE public.kf_rolling_restart_check_result (
    result_id UUID PRIMARY KEY,
    policy_id UUID NOT NULL,
    definition_version TEXT NOT NULL,
    check_key TEXT NOT NULL,
    step_id UUID REFERENCES public.kf_job_steps(id) ON DELETE NO ACTION,
    phase TEXT NOT NULL,
    sample_id UUID NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('PASS','FAIL','ERROR','SKIPPED','UNKNOWN')),
    observed_at TIMESTAMPTZ NOT NULL,
    metrics JSONB NOT NULL,
    message TEXT,
    FOREIGN KEY(policy_id,definition_version) REFERENCES public.kf_rolling_restart_policy(policy_id,definition_version) ON DELETE NO ACTION,
    FOREIGN KEY(definition_version,check_key) REFERENCES public.kf_rolling_restart_check_definition(definition_version,check_key) ON DELETE NO ACTION,
    UNIQUE(policy_id,sample_id,check_key)
);
CREATE INDEX ix_restart_check_result_policy_time ON public.kf_rolling_restart_check_result(policy_id,observed_at);
CREATE INDEX ix_restart_check_result_step ON public.kf_rolling_restart_check_result(step_id);
INSERT INTO public.kf_rolling_restart_check_definition VALUES
    ('restart-checks-v1','topology','Restart topology and agent capability','Targets must match current topology and support restart; >=2 brokers unless confirmed single-host; >=3 controller members unless override.'),
    ('restart-checks-v1','brokerReachability','Broker reachability','visibleBrokers >= expectedBrokerCount'),
    ('restart-checks-v1','controller','Active Kafka controller','controllerId is present and nonnegative'),
    ('restart-checks-v1','offlinePartitions','Partitions have leaders','offlinePartitions = 0'),
    ('restart-checks-v1','underReplicatedPartitions','Replica synchronization','underReplicatedPartitions = 0'),
    ('restart-checks-v1','minimumIsr','Minimum in-sync replicas','minIsrViolations = 0 using effective topic min.insync.replicas'),
    ('restart-checks-v1','consumerLag','Total committed consumer lag','consumerLag <= maxConsumerLag'),
    ('restart-checks-v1','kafkaInspection','Kafka health observation completed','Metadata, topics, effective minimum ISR and consumer lag inspections all complete'),
    ('restart-checks-v1','hostReachability','Agent heartbeat','All selected agents are online/occupied with heartbeat within heartbeatTimeoutSeconds'),
    ('restart-checks-v1','disk','Host disk headroom','Available disk-used metrics must be below maxDiskUsedPercent; absent metrics are UNKNOWN and preserve existing nonblocking behavior'),
    ('restart-checks-v1','quorum','Metadata quorum catch-up','Leader is a voter; >=3 voters unless single override; voters at high watermark and follower timestamps within quorumFreshnessMillis of latest voter fetch'),
    ('restart-checks-v1','brokerCanStop','Broker stop safety','Removing broker leaves at least max(1,min.insync.replicas) ISR for each assigned partition; skipped on confirmed single-host override'),
    ('restart-checks-v1','serviceActive','Kafka system service active','Agent VERIFY_KAFKA_SERVICE reports state=active'),
    ('restart-checks-v1','stableSamples','Consecutive stable samples','At least stableSamples consecutive aggregate healthy observations within healthTimeoutSeconds');

CREATE FUNCTION public.guard_restart_evidence() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'Rolling restart evidence is immutable; append a new observation or policy version';
END;
$$;
CREATE TRIGGER trg_restart_policy_immutable BEFORE UPDATE OR DELETE ON public.kf_rolling_restart_policy
    FOR EACH ROW EXECUTE FUNCTION public.guard_restart_evidence();
CREATE TRIGGER trg_restart_definition_immutable BEFORE UPDATE OR DELETE ON public.kf_rolling_restart_check_definition
    FOR EACH ROW EXECUTE FUNCTION public.guard_restart_evidence();
CREATE TRIGGER trg_restart_result_immutable BEFORE UPDATE OR DELETE ON public.kf_rolling_restart_check_result
    FOR EACH ROW EXECUTE FUNCTION public.guard_restart_evidence();
CREATE FUNCTION public.guard_restart_evidence_owner() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_TABLE_NAME='kf_rolling_restart_policy' THEN
        IF NOT EXISTS(SELECT 1 FROM public.kf_jobs WHERE id=NEW.job_id AND type='ROLLING_RESTART') THEN
            RAISE EXCEPTION 'Policy owner must be a standalone rolling restart job';
        END IF;
    ELSIF NEW.step_id IS NOT NULL AND NOT EXISTS(
        SELECT 1 FROM public.kf_job_steps s JOIN public.kf_rolling_restart_policy p ON p.job_id=s.job_id
        WHERE s.id=NEW.step_id AND p.policy_id=NEW.policy_id) THEN
        RAISE EXCEPTION 'Restart check step belongs to a different job';
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER trg_restart_policy_owner BEFORE INSERT ON public.kf_rolling_restart_policy
    FOR EACH ROW EXECUTE FUNCTION public.guard_restart_evidence_owner();
CREATE TRIGGER trg_restart_result_owner BEFORE INSERT ON public.kf_rolling_restart_check_result
    FOR EACH ROW EXECUTE FUNCTION public.guard_restart_evidence_owner();
COMMENT ON TABLE public.kf_rolling_restart_policy IS 'Immutable effective runtime policy per standalone restart job attempt; captured when execution starts, not inferred for old jobs.';
COMMENT ON TABLE public.kf_rolling_restart_check_result IS 'Actual individual observations, including failed polling samples. UNKNOWN is unmeasured, SKIPPED is not applicable; neither implies PASS. Historical job success is not backfilled as check evidence.';
