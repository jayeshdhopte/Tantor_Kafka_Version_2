-- Configuration-change ER, Config_Change page. Inventory and global audit are reused.
-- Composite keys below replace the diagram's individual UNIQUE annotations.
CREATE TABLE kf_config_catalog (
    config_catalog_id BIGSERIAL PRIMARY KEY,
    config_key VARCHAR(255) NOT NULL,
    applicable_role VARCHAR(16) NOT NULL,
    kafka_update_mode VARCHAR(24) NOT NULL,
    execution_policy VARCHAR(32) NOT NULL,
    scope_policy VARCHAR(40) NOT NULL,
    value_type VARCHAR(24) NOT NULL,
    kafka_version_from VARCHAR(32) NOT NULL,
    kafka_version_to VARCHAR(32),
    catalog_version BIGINT NOT NULL CHECK (catalog_version > 0),
    static_source_key VARCHAR(255),
    validation_rules JSONB NOT NULL DEFAULT '{}',
    safety_rules JSONB NOT NULL DEFAULT '{}',
    is_sensitive BOOLEAN NOT NULL DEFAULT FALSE,
    is_active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_config_catalog_version UNIQUE
        (config_key, applicable_role, kafka_version_from, catalog_version),
    CONSTRAINT uq_config_catalog_id_key UNIQUE (config_catalog_id, config_key)
);

CREATE TABLE kf_config_change (
    change_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_id UUID NOT NULL REFERENCES kf_clusters(id),
    title VARCHAR(255) NOT NULL,
    change_reason TEXT,
    target_role VARCHAR(16) NOT NULL,
    target_scope VARCHAR(16) NOT NULL,
    execution_type VARCHAR(40),
    status VARCHAR(32) NOT NULL DEFAULT 'DRAFT',
    failure_policy VARCHAR(24),
    topology_version BIGINT,
    catalog_snapshot_version BIGINT,
    requested_by VARCHAR(160) NOT NULL,
    requested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    approved_at TIMESTAMPTZ,
    execution_started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    version_no BIGINT NOT NULL DEFAULT 0 CHECK (version_no >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_config_change_cluster UNIQUE (change_id, cluster_id)
);

CREATE TABLE kf_config_change_detail (
    detail_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    change_id UUID NOT NULL REFERENCES kf_config_change(change_id),
    config_catalog_id BIGINT NOT NULL,
    config_key VARCHAR(255) NOT NULL,
    operation VARCHAR(12) NOT NULL,
    desired_value TEXT,
    desired_secret_ref TEXT,
    desired_value_hash CHAR(64),
    is_sensitive BOOLEAN NOT NULL DEFAULT FALSE,
    derived_execution_type VARCHAR(40),
    sequence_no SMALLINT NOT NULL CHECK (sequence_no > 0),
    catalog_snapshot JSONB NOT NULL DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_config_detail_catalog FOREIGN KEY (config_catalog_id, config_key)
        REFERENCES kf_config_catalog(config_catalog_id, config_key),
    CONSTRAINT uq_config_detail_key UNIQUE (change_id, config_key),
    CONSTRAINT uq_config_detail_sequence UNIQUE (change_id, sequence_no),
    CONSTRAINT uq_config_detail_change UNIQUE (change_id, detail_id),
    CONSTRAINT ck_config_detail_value CHECK
        (desired_value IS NULL OR desired_secret_ref IS NULL),
    CONSTRAINT ck_config_detail_sensitive CHECK (NOT is_sensitive OR desired_value IS NULL)
);

CREATE TABLE kf_config_change_target_node (
    target_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    change_id UUID NOT NULL REFERENCES kf_config_change(change_id),
    node_id UUID NOT NULL REFERENCES kf_nodes(id),
    frozen_broker_role BOOLEAN NOT NULL DEFAULT FALSE,
    frozen_controller_role BOOLEAN NOT NULL DEFAULT FALSE,
    was_active_controller BOOLEAN NOT NULL DEFAULT FALSE,
    planned_order INTEGER CHECK (planned_order > 0),
    target_status VARCHAR(24) NOT NULL DEFAULT 'PENDING',
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_config_target_node UNIQUE (change_id, node_id),
    CONSTRAINT uq_config_target_order UNIQUE (change_id, planned_order),
    CONSTRAINT uq_config_target_change UNIQUE (change_id, target_id),
    CONSTRAINT ck_config_target_role CHECK (frozen_broker_role OR frozen_controller_role),
    CONSTRAINT ck_config_target_controller CHECK
        (NOT was_active_controller OR frozen_controller_role)
);

CREATE TABLE kf_config_change_node_value (
    node_value_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    change_id UUID NOT NULL REFERENCES kf_config_change(change_id),
    detail_id UUID NOT NULL,
    target_id UUID NOT NULL,
    is_sensitive BOOLEAN NOT NULL DEFAULT FALSE,
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
    verification_status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    verified_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_config_value_detail FOREIGN KEY (change_id, detail_id)
        REFERENCES kf_config_change_detail(change_id, detail_id),
    CONSTRAINT fk_config_value_target FOREIGN KEY (change_id, target_id)
        REFERENCES kf_config_change_target_node(change_id, target_id),
    CONSTRAINT uq_config_value_pair UNIQUE (detail_id, target_id),
    CONSTRAINT ck_config_value_storage CHECK (
        (value_before IS NULL OR secret_ref_before IS NULL) AND
        (desired_value IS NULL OR desired_secret_ref IS NULL) AND
        (value_after IS NULL OR secret_ref_after IS NULL)),
    CONSTRAINT ck_config_value_sensitive CHECK (NOT is_sensitive OR
        (value_before IS NULL AND desired_value IS NULL AND value_after IS NULL))
);

CREATE TABLE kf_cluster_operation_lock (
    lock_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    cluster_id UUID NOT NULL,
    change_id UUID NOT NULL,
    lock_type VARCHAR(32) NOT NULL,
    lock_status VARCHAR(16) NOT NULL DEFAULT 'ACTIVE',
    fencing_token BIGINT GENERATED ALWAYS AS IDENTITY UNIQUE,
    acquired_by VARCHAR(160) NOT NULL,
    acquired_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    heartbeat_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_expires_at TIMESTAMPTZ NOT NULL,
    released_at TIMESTAMPTZ,
    release_reason TEXT,
    CONSTRAINT fk_config_lock_change FOREIGN KEY (change_id, cluster_id)
        REFERENCES kf_config_change(change_id, cluster_id),
    CONSTRAINT ck_config_lock_lease CHECK (lease_expires_at > acquired_at),
    CONSTRAINT ck_config_lock_release CHECK (released_at IS NULL OR released_at >= acquired_at)
);
-- Preserve released lease history; an expired lease must be explicitly recovered/released.
CREATE UNIQUE INDEX uq_config_lock_open_cluster
    ON kf_cluster_operation_lock(cluster_id) WHERE released_at IS NULL;
CREATE UNIQUE INDEX uq_config_lock_open_change
    ON kf_cluster_operation_lock(change_id) WHERE released_at IS NULL;

CREATE TABLE kf_health_check (
    health_check_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    change_id UUID NOT NULL REFERENCES kf_config_change(change_id),
    target_id UUID,
    phase VARCHAR(24) NOT NULL,
    check_type VARCHAR(64) NOT NULL,
    result VARCHAR(12) NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    metrics JSONB NOT NULL DEFAULT '{}',
    message TEXT,
    evidence_uri TEXT,
    CONSTRAINT fk_config_health_target FOREIGN KEY (change_id, target_id)
        REFERENCES kf_config_change_target_node(change_id, target_id)
);

CREATE TABLE kf_config_execution_attempt (
    attempt_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    change_id UUID NOT NULL REFERENCES kf_config_change(change_id),
    detail_id UUID,
    target_id UUID,
    phase VARCHAR(24) NOT NULL,
    attempt_number INTEGER NOT NULL DEFAULT 1 CHECK (attempt_number > 0),
    idempotency_key UUID NOT NULL DEFAULT gen_random_uuid() UNIQUE,
    attempt_status VARCHAR(16) NOT NULL DEFAULT 'PENDING',
    requested_payload JSONB NOT NULL DEFAULT '{}',
    response_payload JSONB,
    error_code VARCHAR(80),
    error_message TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_config_attempt_detail FOREIGN KEY (change_id, detail_id)
        REFERENCES kf_config_change_detail(change_id, detail_id),
    CONSTRAINT fk_config_attempt_target FOREIGN KEY (change_id, target_id)
        REFERENCES kf_config_change_target_node(change_id, target_id),
    CONSTRAINT uq_config_attempt_number UNIQUE NULLS NOT DISTINCT
        (change_id, detail_id, target_id, phase, attempt_number)
);

CREATE TABLE kf_config_backup (
    backup_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    change_id UUID NOT NULL REFERENCES kf_config_change(change_id),
    detail_id UUID,
    target_id UUID,
    backup_type VARCHAR(32) NOT NULL,
    config_key VARCHAR(255),
    source_before VARCHAR(32),
    before_present BOOLEAN,
    value_before TEXT,
    secret_ref_before TEXT,
    value_hash CHAR(64),
    is_sensitive BOOLEAN NOT NULL DEFAULT FALSE,
    source_path TEXT,
    backup_uri TEXT,
    checksum_sha256 CHAR(64),
    owner_name VARCHAR(128),
    group_name VARCHAR(128),
    file_mode VARCHAR(4),
    captured_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    verified_at TIMESTAMPTZ,
    CONSTRAINT fk_config_backup_detail FOREIGN KEY (change_id, detail_id)
        REFERENCES kf_config_change_detail(change_id, detail_id),
    CONSTRAINT fk_config_backup_target FOREIGN KEY (change_id, target_id)
        REFERENCES kf_config_change_target_node(change_id, target_id),
    CONSTRAINT ck_config_backup_value CHECK (value_before IS NULL OR secret_ref_before IS NULL),
    CONSTRAINT ck_config_backup_sensitive CHECK (NOT is_sensitive OR value_before IS NULL),
    CONSTRAINT ck_config_backup_mode CHECK (file_mode IS NULL OR file_mode ~ '^[0-7]{3,4}$')
);

CREATE TABLE kf_rolling_restart_operation (
    restart_operation_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    change_id UUID NOT NULL UNIQUE REFERENCES kf_config_change(change_id),
    order_strategy VARCHAR(40) NOT NULL,
    restart_status VARCHAR(24) NOT NULL DEFAULT 'PENDING',
    target_count INTEGER NOT NULL DEFAULT 0 CHECK (target_count >= 0),
    completed_count INTEGER NOT NULL DEFAULT 0 CHECK (completed_count >= 0),
    current_target_id UUID,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT uq_config_restart_change UNIQUE (change_id, restart_operation_id),
    CONSTRAINT fk_config_restart_current FOREIGN KEY (change_id, current_target_id)
        REFERENCES kf_config_change_target_node(change_id, target_id),
    CONSTRAINT ck_config_restart_counts CHECK (completed_count <= target_count)
);

CREATE TABLE kf_rolling_restart_node_step (
    restart_step_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    restart_operation_id UUID NOT NULL,
    change_id UUID NOT NULL,
    target_id UUID NOT NULL,
    planned_order INTEGER NOT NULL CHECK (planned_order > 0),
    actual_order INTEGER CHECK (actual_order > 0),
    step_status VARCHAR(24) NOT NULL DEFAULT 'PENDING',
    was_active_controller BOOLEAN NOT NULL DEFAULT FALSE,
    controller_epoch BIGINT CHECK (controller_epoch >= 0),
    attempt_count INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    candidate_activated_at TIMESTAMPTZ,
    restart_started_at TIMESTAMPTZ,
    process_started_at TIMESTAMPTZ,
    verified_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT fk_config_restart_step_operation FOREIGN KEY (change_id, restart_operation_id)
        REFERENCES kf_rolling_restart_operation(change_id, restart_operation_id),
    CONSTRAINT fk_config_restart_step_target FOREIGN KEY (change_id, target_id)
        REFERENCES kf_config_change_target_node(change_id, target_id),
    CONSTRAINT uq_config_restart_step_target UNIQUE (restart_operation_id, target_id),
    CONSTRAINT uq_config_restart_step_plan UNIQUE (restart_operation_id, planned_order),
    CONSTRAINT uq_config_restart_step_actual UNIQUE (restart_operation_id, actual_order)
);

CREATE INDEX idx_config_change_cluster_time ON kf_config_change(cluster_id, requested_at DESC);
CREATE INDEX idx_config_change_status ON kf_config_change(status, requested_at);
CREATE INDEX idx_config_detail_catalog ON kf_config_change_detail(config_catalog_id);
CREATE INDEX idx_config_target_node ON kf_config_change_target_node(node_id);
CREATE INDEX idx_config_value_target ON kf_config_change_node_value(target_id);
CREATE INDEX idx_config_health_change_time ON kf_health_check(change_id, observed_at DESC);
CREATE INDEX idx_config_health_target ON kf_health_check(target_id);
CREATE INDEX idx_config_attempt_change_time ON kf_config_execution_attempt(change_id, created_at);
CREATE INDEX idx_config_attempt_detail ON kf_config_execution_attempt(detail_id);
CREATE INDEX idx_config_attempt_target ON kf_config_execution_attempt(target_id);
CREATE INDEX idx_config_backup_change ON kf_config_backup(change_id, captured_at);
CREATE INDEX idx_config_backup_detail ON kf_config_backup(detail_id);
CREATE INDEX idx_config_backup_target ON kf_config_backup(target_id);
CREATE INDEX idx_config_lock_change ON kf_cluster_operation_lock(change_id);
CREATE INDEX idx_config_restart_step_target ON kf_rolling_restart_node_step(target_id);

-- Validate node membership on selection; UUID node_id here means kf_nodes.id,
-- not the integer Kafka broker/node number in kf_nodes.node_id.
CREATE FUNCTION kf_config_validate_target_cluster() RETURNS trigger AS $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM kf_nodes n JOIN kf_config_change c ON c.change_id = NEW.change_id
        WHERE n.id = NEW.node_id AND n.cluster_id IS DISTINCT FROM c.cluster_id
    ) THEN
        RAISE EXCEPTION 'Configuration target node must belong to the selected cluster'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_config_target_cluster
    BEFORE INSERT OR UPDATE OF change_id, node_id ON kf_config_change_target_node
    FOR EACH ROW EXECUTE FUNCTION kf_config_validate_target_cluster();

CREATE FUNCTION kf_config_validate_change_cluster() RETURNS trigger AS $$
BEGIN
    IF NEW.cluster_id IS DISTINCT FROM OLD.cluster_id AND EXISTS (
        SELECT 1 FROM kf_config_change_target_node t JOIN kf_nodes n ON n.id = t.node_id
        WHERE t.change_id = OLD.change_id AND n.cluster_id IS DISTINCT FROM NEW.cluster_id
    ) THEN
        RAISE EXCEPTION 'Cannot move configuration change to another cluster with existing targets'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
CREATE TRIGGER trg_config_change_cluster
    BEFORE UPDATE OF cluster_id ON kf_config_change
    FOR EACH ROW EXECUTE FUNCTION kf_config_validate_change_cluster();

CREATE FUNCTION kf_config_touch_updated_at() RETURNS trigger AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DO $$
DECLARE table_name TEXT;
BEGIN
    FOREACH table_name IN ARRAY ARRAY[
        'kf_config_catalog', 'kf_config_change', 'kf_config_change_detail',
        'kf_config_change_target_node', 'kf_config_change_node_value',
        'kf_config_execution_attempt', 'kf_rolling_restart_operation',
        'kf_rolling_restart_node_step'
    ] LOOP
        -- Detail has created_at only in the diagram; all other listed tables have updated_at.
        IF table_name <> 'kf_config_change_detail' THEN
            EXECUTE format('CREATE TRIGGER trg_config_updated_at BEFORE UPDATE ON %I '
                'FOR EACH ROW EXECUTE FUNCTION kf_config_touch_updated_at()', table_name);
        END IF;
    END LOOP;
END;
$$;

-- Keep one global audit dataset. The ER's KA_AUDIT label is exposed as a
-- configuration-focused view; existing audit IDs remain UUID, not BIGSERIAL.
ALTER TABLE kf_audit_logs
    ADD COLUMN change_id UUID,
    ADD COLUMN correlation_id UUID,
    ADD CONSTRAINT fk_audit_config_change FOREIGN KEY (change_id, cluster_id)
        REFERENCES kf_config_change(change_id, cluster_id),
    ADD CONSTRAINT ck_audit_config_cluster CHECK (change_id IS NULL OR cluster_id IS NOT NULL);
CREATE INDEX idx_audit_config_change ON kf_audit_logs(change_id, created_time DESC)
    WHERE change_id IS NOT NULL;
CREATE INDEX idx_audit_config_correlation ON kf_audit_logs(correlation_id)
    WHERE correlation_id IS NOT NULL;
CREATE VIEW kf_config_change_audit AS
    SELECT id AS audit_id, cluster_id, change_id, correlation_id,
        created_by AS actor_id, action AS event_type,
        resource_type AS entity_type, resource_id AS entity_id,
        details AS event_payload,
        details -> 'before_state' AS before_state,
        details -> 'after_state' AS after_state,
        details ->> 'previous_hash' AS previous_hash,
        details ->> 'event_hash' AS event_hash,
        created_time AS occurred_at
    FROM kf_audit_logs WHERE change_id IS NOT NULL;

COMMENT ON TABLE kf_config_catalog IS 'Versioned property metadata and rules; no Kafka properties are seeded by this schema migration.';
COMMENT ON COLUMN kf_config_catalog.kafka_update_mode IS 'Kafka update capability for this property/version/role; execution policy is resolved by future application code.';
COMMENT ON TABLE kf_config_change IS 'One configuration change request; execution_type may be unresolved while DRAFT.';
COMMENT ON COLUMN kf_config_change.version_no IS 'Optimistic concurrency counter; the application must compare and increment it.';
COMMENT ON TABLE kf_config_change_detail IS 'One property SET/remove operation within a change.';
COMMENT ON TABLE kf_config_change_target_node IS 'One selected Kafka node within a change; frozen roles preserve planned topology.';
COMMENT ON COLUMN kf_config_change_target_node.node_id IS 'References kf_nodes.id UUID; kf_nodes.node_id is the separate integer Kafka node number.';
COMMENT ON TABLE kf_config_change_node_value IS 'One property x target pair: captured effective value, desired value, verified value and sources.';
COMMENT ON TABLE kf_cluster_operation_lock IS 'Durable configuration-change leases. Only one unreleased lease per cluster. No worker acquires or enforces these leases yet.';
COMMENT ON COLUMN kf_cluster_operation_lock.fencing_token IS 'Monotonically allocated token; command dispatch/agents must enforce it when execution is implemented.';
COMMENT ON TABLE kf_health_check IS 'One health observation; NULL target_id denotes a cluster-level check.';
COMMENT ON TABLE kf_config_execution_attempt IS 'One execution/retry attempt; NULL target_id supports cluster-wide operations. Payload writers must redact secrets.';
COMMENT ON TABLE kf_config_backup IS 'Captured property or file backup metadata; NULL detail_id supports whole-file backups. Sensitive values use secret references.';
COMMENT ON TABLE kf_rolling_restart_operation IS 'One rolling restart linked to a configuration change requiring restart.';
COMMENT ON TABLE kf_rolling_restart_node_step IS 'One target in a rolling restart; plan/actual ordering is unique within that operation.';
COMMENT ON VIEW kf_config_change_audit IS 'Diagram audit aliases over kf_audit_logs. Before/after states and hashes are exposed only if the event writer supplies redacted metadata in details. No hash chain is generated or validated by this view.';
