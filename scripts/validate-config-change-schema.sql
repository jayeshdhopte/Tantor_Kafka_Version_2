-- Run against the isolated local database. All fixture rows are rolled back.
-- psql -X -v ON_ERROR_STOP=1 -f scripts/validate-config-change-schema.sql
BEGIN;
DO $$
DECLARE
    cluster_a UUID := gen_random_uuid();
    cluster_b UUID := gen_random_uuid();
    change_a UUID := gen_random_uuid();
    change_b UUID := gen_random_uuid();
    nodes UUID[] := ARRAY[gen_random_uuid(), gen_random_uuid(), gen_random_uuid()];
    targets UUID[] := ARRAY[gen_random_uuid(), gen_random_uuid(), gen_random_uuid()];
    other_target UUID := gen_random_uuid();
    other_node UUID := gen_random_uuid();
    details UUID[] := ARRAY[gen_random_uuid(), gen_random_uuid()];
    other_detail UUID := gen_random_uuid();
    catalog_ids BIGINT[] := ARRAY[NULL::BIGINT, NULL::BIGINT];
    new_catalog_id BIGINT;
    operation_a UUID := gen_random_uuid();
    operation_b UUID := gen_random_uuid();
    audit_event UUID := gen_random_uuid();
    lease_a UUID;
    token_a BIGINT;
    token_b BIGINT;
    host_key TEXT;
    suffix TEXT := replace(gen_random_uuid()::text, '-', '');
    config_keys TEXT[];
    i INTEGER;
    j INTEGER;
BEGIN
    config_keys := ARRAY['fixture.dynamic.' || suffix, 'fixture.static.' || suffix];
    INSERT INTO kf_clusters(id, cluster_name, kafka_version, mode, status)
    VALUES (cluster_a, 'schema-fixture-a-' || suffix, 'fixture', 'kraft', 'RUNNING'),
           (cluster_b, 'schema-fixture-b-' || suffix, 'fixture', 'kraft', 'RUNNING');
    FOR i IN 1..3 LOOP
        host_key := 'schema-fixture-' || nodes[i]::text;
        INSERT INTO kf_hosts(id, hostname, status, cluster_id)
        VALUES (host_key, host_key, 'ONLINE', cluster_a);
        INSERT INTO kf_nodes(id, cluster_id, node_id, host_id, role)
        VALUES (nodes[i], cluster_a, i, host_key, 'BROKER');
    END LOOP;
    INSERT INTO kf_nodes(id, cluster_id, node_id, host_id, role)
    VALUES (other_node, cluster_b, 1, 'schema-fixture-other-' || suffix, 'BROKER');

    INSERT INTO kf_config_change(change_id, cluster_id, title, target_role, target_scope,
                                 execution_type, requested_by)
    VALUES (change_a, cluster_a, 'Schema fixture A', 'BROKER', 'ALL_NODES', 'MIXED', 'schema-check'),
           (change_b, cluster_a, 'Schema fixture B', 'BROKER', 'SELECTED_NODES', 'DYNAMIC', 'schema-check');
    FOR i IN 1..2 LOOP
        INSERT INTO kf_config_catalog(config_key, applicable_role, kafka_update_mode,
            execution_policy, scope_policy, value_type, kafka_version_from, catalog_version,
            updated_at)
        VALUES (config_keys[i], 'BROKER', CASE WHEN i = 1 THEN 'DYNAMIC' ELSE 'STATIC' END,
            'FIXTURE', 'FIXTURE', 'STRING', 'fixture', 1, '2000-01-01')
        RETURNING config_catalog_id INTO new_catalog_id;
        catalog_ids[i] := new_catalog_id;
        INSERT INTO kf_config_change_detail(detail_id, change_id, config_catalog_id,
            config_key, operation, desired_value, sequence_no, derived_execution_type)
        VALUES (details[i], change_a, catalog_ids[i], config_keys[i], 'SET', '2', i,
            CASE WHEN i = 1 THEN 'DYNAMIC' ELSE 'STATIC' END);
    END LOOP;
    FOR i IN 1..3 LOOP
        INSERT INTO kf_config_change_target_node(target_id, change_id, node_id,
            frozen_broker_role, planned_order)
        VALUES (targets[i], change_a, nodes[i], TRUE, i);
        FOR j IN 1..2 LOOP
            INSERT INTO kf_config_change_node_value(change_id, detail_id, target_id,
                before_present, source_before, value_before, desired_value)
            VALUES (change_a, details[j], targets[i], TRUE, 'FIXTURE', '1', '2');
        END LOOP;
    END LOOP;
    IF (SELECT count(*) FROM kf_config_change_node_value WHERE change_id = change_a) <> 6 THEN
        RAISE EXCEPTION 'Expected six property x node value rows';
    END IF;
    -- A later request can use the same property and node.
    INSERT INTO kf_config_change_target_node(target_id, change_id, node_id, frozen_broker_role)
    VALUES (other_target, change_b, nodes[1], TRUE);
    INSERT INTO kf_config_change_detail(detail_id, change_id, config_catalog_id,
        config_key, operation, desired_value, sequence_no)
    VALUES (other_detail, change_b, catalog_ids[1], config_keys[1], 'SET', '3', 1);

    BEGIN
        INSERT INTO kf_config_change_target_node(change_id, node_id, frozen_broker_role)
        VALUES (change_a, nodes[1], TRUE);
        RAISE EXCEPTION 'Duplicate target was accepted';
    EXCEPTION WHEN unique_violation THEN NULL; END;
    BEGIN
        INSERT INTO kf_config_change_target_node(change_id, node_id, frozen_broker_role)
        VALUES (change_a, other_node, TRUE);
        RAISE EXCEPTION 'Node from another cluster was accepted';
    EXCEPTION WHEN check_violation THEN NULL; END;
    BEGIN
        UPDATE kf_config_change SET cluster_id = cluster_b WHERE change_id = change_a;
        RAISE EXCEPTION 'Request moved to incompatible cluster';
    EXCEPTION WHEN check_violation THEN NULL; END;
    BEGIN
        INSERT INTO kf_config_change_node_value(change_id, detail_id, target_id)
        VALUES (change_a, details[1], other_target);
        RAISE EXCEPTION 'Cross-request target was accepted';
    EXCEPTION WHEN foreign_key_violation THEN NULL; END;
    BEGIN
        INSERT INTO kf_config_change_node_value(change_id, detail_id, target_id)
        VALUES (change_a, other_detail, targets[1]);
        RAISE EXCEPTION 'Cross-request detail was accepted';
    EXCEPTION WHEN foreign_key_violation THEN NULL; END;
    BEGIN
        INSERT INTO kf_config_change_node_value(change_id, detail_id, target_id)
        VALUES (change_a, details[1], targets[1]);
        RAISE EXCEPTION 'Duplicate property x node pair was accepted';
    EXCEPTION WHEN unique_violation THEN NULL; END;
    BEGIN
        UPDATE kf_config_change_node_value SET is_sensitive = TRUE
        WHERE detail_id = details[1] AND target_id = targets[1];
        RAISE EXCEPTION 'Sensitive row with plaintext values was accepted';
    EXCEPTION WHEN check_violation THEN NULL; END;

    INSERT INTO kf_health_check(change_id, phase, check_type, result)
    VALUES (change_a, 'PRECHECK', 'FIXTURE_CLUSTER', 'PASS');
    INSERT INTO kf_health_check(change_id, target_id, phase, check_type, result)
    VALUES (change_a, targets[1], 'PRECHECK', 'FIXTURE_NODE', 'PASS');
    INSERT INTO kf_config_execution_attempt(change_id, detail_id, phase, attempt_number)
    VALUES (change_a, details[1], 'APPLY', 1), (change_a, details[1], 'APPLY', 2);
    BEGIN
        INSERT INTO kf_config_execution_attempt(change_id, detail_id, phase, attempt_number)
        VALUES (change_a, details[1], 'APPLY', 1);
        RAISE EXCEPTION 'Duplicate cluster-wide attempt number was accepted';
    EXCEPTION WHEN unique_violation THEN NULL; END;
    INSERT INTO kf_config_backup(change_id, target_id, backup_type, source_path, backup_uri)
    VALUES (change_a, targets[1], 'FILE', '/fixture/server.properties', 'fixture://backup');
    INSERT INTO kf_config_backup(change_id, detail_id, backup_type, config_key,
                                 before_present, value_before)
    VALUES (change_a, details[1], 'PROPERTY', config_keys[1], TRUE, '1');

    INSERT INTO kf_rolling_restart_operation(restart_operation_id, change_id,
                                             order_strategy, target_count)
    VALUES (operation_a, change_a, 'FIXTURE', 3), (operation_b, change_b, 'FIXTURE', 1);
    FOR i IN 1..3 LOOP
        INSERT INTO kf_rolling_restart_node_step(restart_operation_id, change_id,
                                                 target_id, planned_order)
        VALUES (operation_a, change_a, targets[i], i);
    END LOOP;
    INSERT INTO kf_rolling_restart_node_step(restart_operation_id, change_id, target_id, planned_order)
    VALUES (operation_b, change_b, other_target, 1);
    BEGIN
        INSERT INTO kf_rolling_restart_node_step(restart_operation_id, change_id, target_id, planned_order)
        VALUES (operation_b, change_a, targets[1], 2);
        RAISE EXCEPTION 'Restart operation from another change was accepted';
    EXCEPTION WHEN foreign_key_violation THEN NULL; END;

    INSERT INTO kf_cluster_operation_lock(cluster_id, change_id, lock_type,
                                          acquired_by, lease_expires_at)
    VALUES (cluster_a, change_a, 'CONFIG_CHANGE', 'schema-check', now() + interval '5 minutes')
    RETURNING lock_id, fencing_token INTO lease_a, token_a;
    BEGIN
        INSERT INTO kf_cluster_operation_lock(cluster_id, change_id, lock_type,
                                              acquired_by, lease_expires_at)
        VALUES (cluster_a, change_b, 'CONFIG_CHANGE', 'schema-check', now() + interval '5 minutes');
        RAISE EXCEPTION 'Two unreleased cluster leases were accepted';
    EXCEPTION WHEN unique_violation THEN NULL; END;
    UPDATE kf_cluster_operation_lock SET released_at = now(), lock_status = 'RELEASED'
    WHERE lock_id = lease_a;
    INSERT INTO kf_cluster_operation_lock(cluster_id, change_id, lock_type,
                                          acquired_by, lease_expires_at)
    VALUES (cluster_a, change_b, 'CONFIG_CHANGE', 'schema-check', now() + interval '5 minutes')
    RETURNING fencing_token INTO token_b;
    IF token_b <= token_a THEN RAISE EXCEPTION 'Fencing token did not increase'; END IF;
    IF (SELECT count(*) FROM kf_cluster_operation_lock WHERE cluster_id = cluster_a) <> 2 THEN
        RAISE EXCEPTION 'Released lease history was lost';
    END IF;

    UPDATE kf_config_catalog SET is_active = FALSE WHERE config_catalog_id = catalog_ids[1];
    IF (SELECT updated_at FROM kf_config_catalog WHERE config_catalog_id = catalog_ids[1]) <= '2000-01-01' THEN
        RAISE EXCEPTION 'updated_at trigger did not update timestamp';
    END IF;
    INSERT INTO kf_audit_logs(id, action, resource_type, resource_id, cluster_id,
                              change_id, correlation_id, created_by, details)
    VALUES (audit_event, 'SCHEMA_FIXTURE', 'CONFIG_CHANGE', change_a::text, cluster_a,
        change_a, gen_random_uuid(), 'schema-check', '{"before_state":{"value":"1"}}');
    IF NOT EXISTS (SELECT 1 FROM kf_config_change_audit WHERE audit_id = audit_event
                   AND before_state = '{"value":"1"}'::jsonb) THEN
        RAISE EXCEPTION 'Audit view mapping failed';
    END IF;
    RAISE NOTICE 'PASS: 2 properties x 3 nodes; request isolation; scoped uniqueness; sensitive-value check; cluster/node health; retries; backups; restart steps; lock history/fencing; timestamps; audit mapping. Fixtures roll back.';
END;
$$;
ROLLBACK;
