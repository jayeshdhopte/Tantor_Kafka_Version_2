-- Phase 2: isolated, empty ER schema. Application mappings stay unchanged.
-- V89 is immutable. Only its explicitly identified prototype objects are replaced.
-- No existing table rows are changed. No FK or CHECK is added by this migration.
-- BIGSERIAL is retained only for the ER catalog primary key; no non-key defaults.

DO $$
DECLARE
    table_name TEXT;
    has_rows BOOLEAN;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'kf_config_catalog', 'kf_config_change', 'kf_config_change_detail',
        'kf_config_change_target_node', 'kf_config_change_node_value', 'kf_cluster_operation_lock',
        'kf_health_check', 'kf_config_execution_attempt', 'kf_config_backup',
        'kf_rolling_restart_operation', 'kf_rolling_restart_node_step'
    ] LOOP
        EXECUTE format('LOCK TABLE public.%I IN ACCESS EXCLUSIVE MODE', table_name);
        EXECUTE format('SELECT EXISTS (SELECT 1 FROM public.%I)', table_name) INTO has_rows;
        IF has_rows THEN
            RAISE EXCEPTION 'V90 refused: prototype table % is not empty; no objects were replaced', table_name;
        END IF;
    END LOOP;
END;
$$;

-- The only changes to an existing application table are these authorized removals.
-- Both existing nullable audit columns and their indexes remain untouched.
ALTER TABLE public.kf_audit_logs
    DROP CONSTRAINT fk_audit_config_change,
    DROP CONSTRAINT ck_audit_config_cluster;

DROP VIEW public.kf_config_change_audit;
DROP TABLE public.kf_rolling_restart_node_step;
DROP TABLE public.kf_rolling_restart_operation;
DROP TABLE public.kf_config_backup;
DROP TABLE public.kf_config_execution_attempt;
DROP TABLE public.kf_health_check;
DROP TABLE public.kf_cluster_operation_lock;
DROP TABLE public.kf_config_change_node_value;
DROP TABLE public.kf_config_change_target_node;
DROP TABLE public.kf_config_change_detail;
DROP TABLE public.kf_config_change;
DROP TABLE public.kf_config_catalog;

-- V89-only helpers have no remaining triggers after the prototype tables drop.
DROP FUNCTION public.kf_config_validate_target_cluster();
DROP FUNCTION public.kf_config_validate_change_cluster();
DROP FUNCTION public.kf_config_touch_updated_at();

CREATE TABLE public.kf_config_catalog (
    config_catalog_id BIGSERIAL PRIMARY KEY,
    config_key VARCHAR(255),
    applicable_role VARCHAR(16),
    kafka_update_mode VARCHAR(24),
    execution_policy VARCHAR(32),
    scope_policy VARCHAR(40),
    value_type VARCHAR(24),
    kafka_version_from VARCHAR(32),
    kafka_version_to VARCHAR(32),
    catalog_version BIGINT,
    static_source_key VARCHAR(255),
    validation_rules JSONB,
    safety_rules JSONB,
    is_sensitive BOOLEAN,
    is_active BOOLEAN,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE public.kf_config_change (
    change_id UUID PRIMARY KEY,
    cluster_id UUID,
    title VARCHAR(255),
    change_reason TEXT,
    target_role VARCHAR(16),
    target_scope VARCHAR(16),
    execution_type VARCHAR(40),
    status VARCHAR(32),
    failure_policy VARCHAR(24),
    topology_version BIGINT,
    catalog_snapshot_version BIGINT,
    requested_by VARCHAR(160),
    requested_at TIMESTAMPTZ,
    approved_at TIMESTAMPTZ,
    execution_started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    version_no BIGINT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE public.kf_config_change_detail (
    detail_id UUID PRIMARY KEY,
    change_id UUID,
    config_catalog_id BIGINT,
    config_key VARCHAR(255),
    operation VARCHAR(12),
    desired_value TEXT,
    desired_secret_ref TEXT,
    desired_value_hash CHAR(64),
    is_sensitive BOOLEAN,
    derived_execution_type VARCHAR(40),
    sequence_no SMALLINT,
    catalog_snapshot JSONB,
    created_at TIMESTAMPTZ
);

CREATE TABLE public.kf_config_change_target_node (
    target_id UUID PRIMARY KEY,
    change_id UUID,
    node_id UUID,
    frozen_broker_role BOOLEAN,
    frozen_controller_role BOOLEAN,
    was_active_controller BOOLEAN,
    planned_order INTEGER,
    target_status VARCHAR(24),
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE public.kf_config_change_node_value (
    node_value_id UUID PRIMARY KEY,
    change_id UUID,
    detail_id UUID,
    target_id UUID,
    is_sensitive BOOLEAN,
    before_present BOOLEAN,
    source_before VARCHAR(32),
    value_before TEXT,
    secret_ref_before TEXT,
    desired_value TEXT,
    desired_secret_ref TEXT,
    value_after TEXT,
    secret_ref_after TEXT,
    source_after VARCHAR(32),
    rollback_operation VARCHAR(12),
    verification_status VARCHAR(16),
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE public.kf_cluster_operation_lock (
    lock_id UUID PRIMARY KEY,
    cluster_id UUID,
    change_id UUID,
    lock_type VARCHAR(32),
    lock_status VARCHAR(16),
    fencing_token BIGINT,
    acquired_by VARCHAR(160),
    acquired_at TIMESTAMPTZ,
    heartbeat_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ,
    released_at TIMESTAMPTZ,
    release_reason TEXT
);

CREATE TABLE public.kf_health_check (
    health_check_id UUID PRIMARY KEY,
    change_id UUID,
    target_id UUID,
    phase VARCHAR(24),
    check_type VARCHAR(64),
    result VARCHAR(12),
    observed_at TIMESTAMPTZ,
    metrics JSONB,
    message TEXT,
    evidence_uri TEXT
);

CREATE TABLE public.kf_config_execution_attempt (
    attempt_id UUID PRIMARY KEY,
    change_id UUID,
    detail_id UUID,
    target_id UUID,
    phase VARCHAR(24),
    attempt_number INTEGER,
    idempotency_key UUID,
    attempt_status VARCHAR(16),
    requested_payload JSONB,
    response_payload JSONB,
    error_code VARCHAR(80),
    error_message TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE public.kf_config_backup (
    backup_id UUID PRIMARY KEY,
    change_id UUID,
    detail_id UUID,
    target_id UUID,
    backup_type VARCHAR(32),
    config_key VARCHAR(255),
    source_before VARCHAR(32),
    before_present BOOLEAN,
    value_before TEXT,
    secret_ref_before TEXT,
    value_hash CHAR(64),
    is_sensitive BOOLEAN,
    source_path TEXT,
    backup_uri TEXT,
    checksum_sha256 CHAR(64),
    owner_name VARCHAR(128),
    group_name VARCHAR(128),
    file_mode VARCHAR(4),
    captured_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ
);

CREATE TABLE public.kf_rolling_restart_operation (
    restart_operation_id UUID PRIMARY KEY,
    change_id UUID,
    order_strategy VARCHAR(40),
    restart_status VARCHAR(24),
    target_count INTEGER,
    completed_count INTEGER,
    current_target_id UUID,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

CREATE TABLE public.kf_rolling_restart_node_step (
    restart_step_id UUID PRIMARY KEY,
    restart_operation_id UUID,
    change_id UUID,
    target_id UUID,
    planned_order INTEGER,
    actual_order INTEGER,
    step_status VARCHAR(24),
    was_active_controller BOOLEAN,
    controller_epoch BIGINT,
    attempt_count INTEGER,
    candidate_activated_at TIMESTAMPTZ,
    restart_started_at TIMESTAMPTZ,
    process_started_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ
);

COMMENT ON COLUMN public.kf_config_catalog.config_catalog_id IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.config_key IS 'ER field; source: kf_job_steps.payload.changes object keys [L]; single-property kf_job_steps.payload.key [C].';
COMMENT ON COLUMN public.kf_config_catalog.applicable_role IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.kafka_update_mode IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.execution_policy IS 'ER field; source: kf_jobs.payload.classification [L]; current decision only, not a versioned master policy.';
COMMENT ON COLUMN public.kf_config_catalog.scope_policy IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.value_type IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.kafka_version_from IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.kafka_version_to IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.catalog_version IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.static_source_key IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.validation_rules IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.safety_rules IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.is_sensitive IS 'ER field; source: kf_job_steps.payload.recoveryCheckpoint.dynamic[<key>].isSensitive [L dynamic only]; filters prohibit sensitive edits.';
COMMENT ON COLUMN public.kf_config_catalog.is_active IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.created_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_catalog.updated_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change.change_id IS 'ER field; source: kf_jobs.id [L]; reuse job identity; VARCHAR UUID in DB, not a new UUID FK.';
COMMENT ON COLUMN public.kf_config_change.cluster_id IS 'ER field; source: kf_jobs.payload.clusterId [L]; kf_config_versions.cluster_id is also an existing column [C].';
COMMENT ON COLUMN public.kf_config_change.title IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change.change_reason IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change.target_role IS 'ER field; source: kf_jobs.payload.role [L]; kf_job_steps.payload.role [L].';
COMMENT ON COLUMN public.kf_config_change.target_scope IS 'ER field; source: kf_jobs.payload.scope [L: ALL/SPECIFIC].';
COMMENT ON COLUMN public.kf_config_change.execution_type IS 'ER field; source: kf_jobs.payload.classification [L]; kf_job_steps.payload.operation [L: DYNAMIC/STATIC].';
COMMENT ON COLUMN public.kf_config_change.status IS 'ER field; source: kf_jobs.status [L]; existing state vocabulary retained.';
COMMENT ON COLUMN public.kf_config_change.failure_policy IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change.topology_version IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change.catalog_snapshot_version IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change.requested_by IS 'ER field; source: kf_jobs.requested_by [L].';
COMMENT ON COLUMN public.kf_config_change.requested_at IS 'ER field; source: kf_jobs.created_at [L]; job creation is request submission.';
COMMENT ON COLUMN public.kf_config_change.approved_at IS 'ER field; source: kf_config_versions.approved_at [C: no version rows; no targeted approval field].';
COMMENT ON COLUMN public.kf_config_change.execution_started_at IS 'ER field; source: kf_jobs.start_time [L].';
COMMENT ON COLUMN public.kf_config_change.completed_at IS 'ER field; source: kf_jobs.end_time [L]; terminal job completion.';
COMMENT ON COLUMN public.kf_config_change.version_no IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change.created_at IS 'ER field; source: kf_jobs.created_at [L].';
COMMENT ON COLUMN public.kf_config_change.updated_at IS 'ER field; source: kf_jobs.updated_at [L].';
COMMENT ON COLUMN public.kf_config_change_detail.detail_id IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_detail.change_id IS 'ER field; source: kf_jobs.id or kf_job_steps.job_id [L].';
COMMENT ON COLUMN public.kf_config_change_detail.config_catalog_id IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_detail.config_key IS 'ER field; source: kf_job_steps.payload.changes keys [L]; single kf_job_steps.payload.key [C]; kf_config_versions.validation_result.diff[*].key [C].';
COMMENT ON COLUMN public.kf_config_change_detail.operation IS 'ER field; source: kf_job_steps.payload.changes[<key>] null => REMOVE, non-null => SET [L values; C removal]; kf_job_steps.payload.removal [C].';
COMMENT ON COLUMN public.kf_config_change_detail.desired_value IS 'ER field; source: kf_job_steps.payload.changes[<key>] [L]; kf_job_steps.payload.value [C]; kf_config_versions.new_config[<key>] [C].';
COMMENT ON COLUMN public.kf_config_change_detail.desired_secret_ref IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_detail.desired_value_hash IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_detail.is_sensitive IS 'ER field; source: kf_job_steps.payload.recoveryCheckpoint.dynamic[<key>].isSensitive [L dynamic only].';
COMMENT ON COLUMN public.kf_config_change_detail.derived_execution_type IS 'ER field; source: kf_jobs.payload.classification and kf_job_steps.payload.operation [L]; batches enforce one classification.';
COMMENT ON COLUMN public.kf_config_change_detail.sequence_no IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_detail.catalog_snapshot IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_detail.created_at IS 'ER field; source: kf_jobs.created_at [L] as submission time of its property batch; no independent detail row.';
COMMENT ON COLUMN public.kf_config_change_target_node.target_id IS 'ER field; source: kf_job_steps.id [L targeted: one node batch per step; not every legacy path has this cardinality].';
COMMENT ON COLUMN public.kf_config_change_target_node.change_id IS 'ER field; source: kf_job_steps.job_id [L].';
COMMENT ON COLUMN public.kf_config_change_target_node.node_id IS 'ER field; source: kf_nodes.id [L inventory UUID]. Exact join on kf_jobs.payload.clusterId + kf_job_steps.payload.nodeId/brokerId + hostId + role matched 6/9 historical targets; 3/9 have no current match. Preserve NULL for unresolved UUIDs; do not fabricate.';
COMMENT ON COLUMN public.kf_config_change_target_node.frozen_broker_role IS 'ER field; source: kf_job_steps.payload.role [L]; derive broker membership, including broker_controller. Saved role snapshot, not live kf_nodes.role.';
COMMENT ON COLUMN public.kf_config_change_target_node.frozen_controller_role IS 'ER field; source: kf_job_steps.payload.role [L]; derive controller membership, including broker_controller.';
COMMENT ON COLUMN public.kf_config_change_target_node.was_active_controller IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_target_node.planned_order IS 'ER field; source: kf_job_steps.step_order [L]; per-target order in targeted flow.';
COMMENT ON COLUMN public.kf_config_change_target_node.target_status IS 'ER field; source: kf_job_steps.status [L]; batch/node outcome.';
COMMENT ON COLUMN public.kf_config_change_target_node.started_at IS 'ER field; source: kf_job_steps.start_time [L]; latest node-step start.';
COMMENT ON COLUMN public.kf_config_change_target_node.completed_at IS 'ER field; source: kf_job_steps.end_time [L]; latest node-step completion.';
COMMENT ON COLUMN public.kf_config_change_target_node.created_at IS 'ER field; source: kf_job_steps.created_at [L].';
COMMENT ON COLUMN public.kf_config_change_target_node.updated_at IS 'ER field; source: kf_job_steps.updated_at [L].';
COMMENT ON COLUMN public.kf_config_change_node_value.node_value_id IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_node_value.change_id IS 'ER field; source: kf_job_steps.job_id [L].';
COMMENT ON COLUMN public.kf_config_change_node_value.detail_id IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_node_value.target_id IS 'ER field; source: kf_job_steps.id [L targeted].';
COMMENT ON COLUMN public.kf_config_change_node_value.is_sensitive IS 'ER field; source: kf_job_steps.payload.recoveryCheckpoint.dynamic[<key>].isSensitive [L dynamic only].';
COMMENT ON COLUMN public.kf_config_change_node_value.before_present IS 'ER field; source: kf_job_steps.payload.recoveryCheckpoint.file[<key>] non-null [L static]; dynamic checkpoint source indicates explicit broker override [L]. Source-layer presence, not merely existence of an effective Kafka default.';
COMMENT ON COLUMN public.kf_config_change_node_value.source_before IS 'ER field; source: kf_job_steps.payload.recoveryCheckpoint.dynamic[<key>].source [L]; static file path kf_job_steps.payload.configPath exists, but Kafka source enum is not saved for static updates.';
COMMENT ON COLUMN public.kf_config_change_node_value.value_before IS 'ER field; source: kf_job_steps.payload.recoveryCheckpoint.dynamic[<key>].value / recoveryCheckpoint.file[<key>] [L]; expectedValues[<key>] is preview value [L].';
COMMENT ON COLUMN public.kf_config_change_node_value.secret_ref_before IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_node_value.desired_value IS 'ER field; source: kf_job_steps.payload.changes[<key>] [L]; single kf_job_steps.payload.value [C].';
COMMENT ON COLUMN public.kf_config_change_node_value.desired_secret_ref IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_node_value.value_after IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_node_value.secret_ref_after IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_node_value.source_after IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_node_value.rollback_operation IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_node_value.verification_status IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_node_value.verified_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_change_node_value.created_at IS 'ER field; source: kf_job_steps.created_at [L] as property-batch creation; no independent value-row lifecycle.';
COMMENT ON COLUMN public.kf_config_change_node_value.updated_at IS 'ER field; source: kf_job_steps.updated_at [L] as batch update; no independent property-row update.';
COMMENT ON COLUMN public.kf_cluster_operation_lock.lock_id IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_cluster_operation_lock.cluster_id IS 'ER field; source: kf_jobs.payload.clusterId [L]; kf_jobs.resource_key carries cluster:<UUID> [L].';
COMMENT ON COLUMN public.kf_cluster_operation_lock.change_id IS 'ER field; source: kf_jobs.id [L]; owning configuration job.';
COMMENT ON COLUMN public.kf_cluster_operation_lock.lock_type IS 'ER field; source: kf_jobs.type [L] describes operation owner; not an independent lease type.';
COMMENT ON COLUMN public.kf_cluster_operation_lock.lock_status IS 'ER field; source: kf_jobs.status [L], existing active-status lock predicate; not a separate lease state.';
COMMENT ON COLUMN public.kf_cluster_operation_lock.fencing_token IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_cluster_operation_lock.acquired_by IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_cluster_operation_lock.acquired_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_cluster_operation_lock.heartbeat_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_cluster_operation_lock.lease_expires_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_cluster_operation_lock.released_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_cluster_operation_lock.release_reason IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_health_check.health_check_id IS 'ER field; source: kf_job_steps.id [C legacy ROLLING_CONFIG_UPDATE dedicated PREFLIGHT/HEALTH_CHECK/FINAL_VERIFY steps only]. Targeted flow has no separate health observation ID.';
COMMENT ON COLUMN public.kf_health_check.change_id IS 'ER field; source: kf_jobs.id / kf_job_steps.job_id [L].';
COMMENT ON COLUMN public.kf_health_check.target_id IS 'ER field; source: kf_job_steps.id [L] identifies enclosing node; no individual observation relationship.';
COMMENT ON COLUMN public.kf_health_check.phase IS 'ER field; source: kf_job_steps.name prefixes PREFLIGHT, HEALTH_CHECK:, FINAL_VERIFY [C legacy file flow]. Targeted flow''s inline check phases are not individually saved.';
COMMENT ON COLUMN public.kf_health_check.check_type IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_health_check.result IS 'ER field; source: kf_job_steps.status for dedicated legacy health gate steps [C]. Targeted kf_job_steps.status covers an entire node batch and is not an individual check outcome.';
COMMENT ON COLUMN public.kf_health_check.observed_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_health_check.metrics IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_health_check.message IS 'ER field; source: kf_job_steps.logs on legacy dedicated health steps [C: ''Preflight Validation Passed'', ''Node healthy'', failure messages]. Targeted checks have only combined node logs.';
COMMENT ON COLUMN public.kf_health_check.evidence_uri IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_execution_attempt.attempt_id IS 'ER field; source: kf_tasks.id / kf_external_kafka_tasks.id [L agent commands only]; no matching per-call ID for direct dynamic Admin API mutations.';
COMMENT ON COLUMN public.kf_config_execution_attempt.change_id IS 'ER field; source: kf_job_steps.job_id [L].';
COMMENT ON COLUMN public.kf_config_execution_attempt.detail_id IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_execution_attempt.target_id IS 'ER field; source: kf_job_steps.id [L] via agent_task_id/external_kafka_task_id or payload.configMutationTaskId.';
COMMENT ON COLUMN public.kf_config_execution_attempt.phase IS 'ER field; source: kf_tasks.command / kf_external_kafka_tasks.command [L: BACKUP_KAFKA_CONFIG, PATCH_KAFKA_CONFIG] identify agent action phase; kf_job_steps.payload.operation [L] identifies direct DYNAMIC batch. No separate durable per-call phase history.';
COMMENT ON COLUMN public.kf_config_execution_attempt.attempt_number IS 'ER field; source: kf_tasks.attempt_count [L agent claim attempts]; kf_job_steps.retry_count [L logical retries]. Neither is complete immutable per-property attempt history.';
COMMENT ON COLUMN public.kf_config_execution_attempt.idempotency_key IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_execution_attempt.attempt_status IS 'ER field; source: kf_tasks.status / kf_external_kafka_tasks.status [L command outcome]; kf_job_steps.status [L enclosing batch]. Dynamic Admin API has no separate attempt status row.';
COMMENT ON COLUMN public.kf_config_execution_attempt.requested_payload IS 'ER field; source: kf_tasks.parameters / kf_external_kafka_tasks.parameters::jsonb [L agent requests]; kf_job_steps.payload.changes and expectedValues [L direct dynamic request].';
COMMENT ON COLUMN public.kf_config_execution_attempt.response_payload IS 'ER field; source: kf_tasks.log_output / kf_external_kafka_tasks.message [L raw response columns; not guaranteed JSON, preserve text]. No persisted dynamic API response object.';
COMMENT ON COLUMN public.kf_config_execution_attempt.error_code IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_execution_attempt.error_message IS 'ER field; source: kf_tasks.error_msg / kf_external_kafka_tasks.message / kf_job_steps.logs [columns verified; no failed config samples]. Logs can contain mixed output, not a clean standalone error field.';
COMMENT ON COLUMN public.kf_config_execution_attempt.started_at IS 'ER field; source: kf_job_steps.start_time [L latest enclosing node attempt only]; no precise start per phase/command. kf_tasks.claimed_at is explicitly excluded as a start substitute.';
COMMENT ON COLUMN public.kf_config_execution_attempt.completed_at IS 'ER field; source: kf_job_steps.end_time [L latest enclosing node attempt only]; kf_tasks.updated_at is not treated as command completion.';
COMMENT ON COLUMN public.kf_config_execution_attempt.created_at IS 'ER field; source: kf_tasks.created_at / kf_external_kafka_tasks.created_at [L agent command]; kf_job_steps.created_at identifies batch creation, not each retry.';
COMMENT ON COLUMN public.kf_config_execution_attempt.updated_at IS 'ER field; source: kf_tasks.updated_at [L agent task]; kf_job_steps.updated_at [L latest batch state]; X has no updated_at column.';
COMMENT ON COLUMN public.kf_config_backup.backup_id IS 'ER field; source: kf_tasks.id / kf_external_kafka_tasks.id [L backup-producing command grouping]; one command can produce file + sidecar, so no separate artifact ID.';
COMMENT ON COLUMN public.kf_config_backup.change_id IS 'ER field; source: kf_job_steps.job_id [L] via bound backup or mutation command.';
COMMENT ON COLUMN public.kf_config_backup.detail_id IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_backup.target_id IS 'ER field; source: kf_job_steps.id [L targeted], via agent task bindings.';
COMMENT ON COLUMN public.kf_config_backup.backup_type IS 'ER field; source: kf_tasks.parameters.dynamic_snapshot_json parsed $.mode [L DYNAMIC]; kf_tasks.command / kf_external_kafka_tasks.command distinguish backup/patch [L].';
COMMENT ON COLUMN public.kf_config_backup.config_key IS 'ER field; source: kf_tasks.parameters.config_key [L batch sentinel]; actual keys in parsed dynamic_snapshot_json.changes and kf_job_steps.payload.changes [L].';
COMMENT ON COLUMN public.kf_config_backup.source_before IS 'ER field; source: Parsed kf_tasks.parameters.dynamic_snapshot_json.previous[<key>].source [L]; kf_job_steps.payload.recoveryCheckpoint.dynamic[<key>].source [L].';
COMMENT ON COLUMN public.kf_config_backup.before_present IS 'ER field; source: kf_job_steps.payload.recoveryCheckpoint.file[<key>] non-null [L]; dynamic source indicates explicit broker override [L].';
COMMENT ON COLUMN public.kf_config_backup.value_before IS 'ER field; source: Parsed kf_tasks.parameters.dynamic_snapshot_json.previous[<key>].value / kf_job_steps.payload.recoveryCheckpoint.file[<key>] [L].';
COMMENT ON COLUMN public.kf_config_backup.secret_ref_before IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_backup.value_hash IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_backup.is_sensitive IS 'ER field; source: Parsed kf_tasks.parameters.dynamic_snapshot_json.previous[<key>].isSensitive [L dynamic only].';
COMMENT ON COLUMN public.kf_config_backup.source_path IS 'ER field; source: kf_job_steps.payload.configPath / kf_tasks.parameters.config_path / kf_external_kafka_tasks.parameters::jsonb.config_path [L].';
COMMENT ON COLUMN public.kf_config_backup.backup_uri IS 'ER field; source: TEXT extraction: kf_tasks.log_output or kf_external_kafka_tasks.message (also copied to kf_job_steps.logs); regex (?m)^(?:Configuration backup|Rollback checkpoint) \\(7-day retention\\): ([^\\r\\n]+). Internal BACKUP: first prefix 3/3; internal PATCH: second prefix 3/3. External managed code uses first prefix; external path text needs the same prefix check, not the internal PATCH prefix.';
COMMENT ON COLUMN public.kf_config_backup.checksum_sha256 IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_backup.owner_name IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_backup.group_name IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_backup.file_mode IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_config_backup.captured_at IS 'ER field; source: Parsed kf_tasks.parameters.dynamic_snapshot_json.capturedAt [L]; timestamp is dynamic-snapshot construction before file copy, not file-copy completion.';
COMMENT ON COLUMN public.kf_config_backup.verified_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_rolling_restart_operation.restart_operation_id IS 'ER field; source: kf_jobs.id [L static config job]; reuse enclosing operation, not a separately created child restart.';
COMMENT ON COLUMN public.kf_rolling_restart_operation.change_id IS 'ER field; source: kf_jobs.id [L static config job].';
COMMENT ON COLUMN public.kf_rolling_restart_operation.order_strategy IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_rolling_restart_operation.restart_status IS 'ER field; source: kf_jobs.status [L whole static-change operation; no independently tracked restart sub-operation].';
COMMENT ON COLUMN public.kf_rolling_restart_operation.target_count IS 'ER field; source: kf_jobs.progress.totalSteps [L targeted: one step per target; not valid for legacy multi-stage or standalone restart without filtering].';
COMMENT ON COLUMN public.kf_rolling_restart_operation.completed_count IS 'ER field; source: kf_jobs.progress.currentStep [L counts SUCCESS/ROLLED_BACK/SKIPPED, not a strict successful-restart count; filter kf_job_steps.status for that distinction].';
COMMENT ON COLUMN public.kf_rolling_restart_operation.current_target_id IS 'ER field; source: kf_job_steps.id selected by kf_job_steps.status=''IN_PROGRESS'' [existing columns; none active in current rows].';
COMMENT ON COLUMN public.kf_rolling_restart_operation.started_at IS 'ER field; source: kf_jobs.start_time [L whole operation]; no separate child restart-start timestamp.';
COMMENT ON COLUMN public.kf_rolling_restart_operation.completed_at IS 'ER field; source: kf_jobs.end_time [L whole operation].';
COMMENT ON COLUMN public.kf_rolling_restart_operation.created_at IS 'ER field; source: kf_jobs.created_at [L].';
COMMENT ON COLUMN public.kf_rolling_restart_operation.updated_at IS 'ER field; source: kf_jobs.updated_at [L].';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.restart_step_id IS 'ER field; source: kf_job_steps.id [L static targeted node batch]; no separate restart-only child step.';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.restart_operation_id IS 'ER field; source: kf_job_steps.job_id [L].';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.change_id IS 'ER field; source: kf_job_steps.job_id [L].';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.target_id IS 'ER field; source: kf_job_steps.id [L same node batch identity].';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.planned_order IS 'ER field; source: kf_job_steps.step_order [L].';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.actual_order IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.step_status IS 'ER field; source: kf_job_steps.status [L whole static node batch].';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.was_active_controller IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.controller_epoch IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.attempt_count IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.candidate_activated_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.restart_started_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.process_started_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.verified_at IS 'ER field; EMPTY, to be clarified';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.completed_at IS 'ER field; source: kf_job_steps.end_time [L node batch completion].';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.last_error IS 'ER field; source: kf_job_steps.logs / bound kf_tasks.error_msg / kf_external_kafka_tasks.message [existing columns; no failed config row samples].';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.created_at IS 'ER field; source: kf_job_steps.created_at [L].';
COMMENT ON COLUMN public.kf_rolling_restart_node_step.updated_at IS 'ER field; source: kf_job_steps.updated_at [L].';
