-- Keep the UI and configuration audit history in the same append-only ledger.
-- Deferred triggers capture the complete ER projection at transaction completion.
CREATE FUNCTION public.record_config_change_audit(
    change_uuid UUID, previous_status TEXT, next_status TEXT, event_uuid UUID,
    occurred_at TIMESTAMPTZ, reconstructed BOOLEAN DEFAULT FALSE)
RETURNS UUID
LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
DECLARE
    change_row public.kf_config_change%ROWTYPE;
    nodes JSONB;
    before_snapshot JSONB;
    after_snapshot JSONB;
    event_action TEXT;
BEGIN
    SELECT * INTO change_row FROM public.kf_config_change WHERE change_id = change_uuid;
    IF NOT FOUND THEN RETURN NULL; END IF;

    SELECT COALESCE(jsonb_agg(node_info ORDER BY planned_order, target_id), '[]'::jsonb)
    INTO nodes
    FROM (
        SELECT t.planned_order, t.target_id,
            jsonb_build_object('targetId', t.target_id, 'inventoryNodeId', t.node_id,
                'nodeId', COALESCE(e.requested_payload->>'nodeId', n.node_id::text),
                'hostId', COALESCE(e.requested_payload->>'hostId', n.host_id),
                'hostname', COALESCE(s.payload->>'hostname', h.hostname),
                'hostIp', COALESCE(s.payload->>'ipAddress', h.host_ip),
                'status', t.target_status,
                'properties', COALESCE(p.properties, '[]'::jsonb)) AS node_info
        FROM public.kf_config_change_target_node t
        LEFT JOIN public.kf_nodes n ON n.id = t.node_id
        LEFT JOIN LATERAL (
            SELECT a.requested_payload FROM public.kf_config_execution_attempt a
            WHERE a.change_id = change_uuid AND a.target_id = t.target_id
            ORDER BY a.updated_at DESC NULLS LAST, a.attempt_id LIMIT 1
        ) e ON TRUE
        LEFT JOIN public.kf_job_steps s ON s.id::text = e.requested_payload->>'stepId'
        LEFT JOIN public.kf_hosts h ON h.id = COALESCE(e.requested_payload->>'hostId', n.host_id)
        LEFT JOIN LATERAL (
            SELECT jsonb_agg(jsonb_build_object('key', d.config_key, 'operation', d.operation,
                'sensitive', sensitive_value,
                'beforeValue', CASE WHEN sensitive_value THEN NULL ELSE v.value_before END,
                'beforeSource', v.source_before, 'beforePresent', v.before_present,
                'requestedValue', CASE WHEN sensitive_value THEN NULL ELSE v.desired_value END,
                'observedValue', CASE WHEN sensitive_value THEN NULL ELSE v.value_after END,
                'observedSource', v.source_after, 'verificationStatus', v.verification_status,
                'verifiedAt', v.verified_at) ORDER BY d.sequence_no, d.config_key) AS properties
            FROM public.kf_config_change_node_value v
            JOIN public.kf_config_change_detail d ON d.detail_id = v.detail_id AND d.change_id = change_uuid
            CROSS JOIN LATERAL (SELECT COALESCE(v.is_sensitive, FALSE) OR COALESCE(d.is_sensitive, FALSE)
                OR lower(COALESCE(d.config_key, '')) ~ '(password|secret|token|credential|authorization|privatekey|jaas|keystore|truststore)'
                AS sensitive_value) redaction
            WHERE v.change_id = change_uuid AND v.target_id = t.target_id
        ) p ON TRUE
        WHERE t.change_id = change_uuid
    ) target_snapshots;

    SELECT COALESCE(jsonb_agg(jsonb_build_object('targetId', node->'targetId',
        'nodeId', node->'nodeId', 'hostId', node->'hostId', 'properties',
        (SELECT COALESCE(jsonb_agg(jsonb_build_object('key', property->'key',
            'value', property->'beforeValue', 'source', property->'beforeSource',
            'present', property->'beforePresent', 'sensitive', property->'sensitive')), '[]'::jsonb)
         FROM jsonb_array_elements(node->'properties') property))), '[]'::jsonb)
    INTO before_snapshot FROM jsonb_array_elements(nodes) node;

    SELECT COALESCE(jsonb_agg(jsonb_build_object('targetId', node->'targetId',
        'nodeId', node->'nodeId', 'hostId', node->'hostId', 'status', node->'status', 'properties',
        (SELECT COALESCE(jsonb_agg(jsonb_build_object('key', property->'key',
            'value', property->'observedValue', 'source', property->'observedSource',
            'verificationStatus', property->'verificationStatus', 'sensitive', property->'sensitive')), '[]'::jsonb)
         FROM jsonb_array_elements(node->'properties') property))), '[]'::jsonb)
    INTO after_snapshot FROM jsonb_array_elements(nodes) node;

    event_action := CASE next_status
        WHEN 'PENDING' THEN 'CONFIG_CHANGE_REQUESTED'
        WHEN 'IN_PROGRESS' THEN 'CONFIG_CHANGE_STARTED'
        WHEN 'SUCCESS' THEN 'CONFIG_CHANGE_COMPLETED'
        WHEN 'FAILED' THEN 'CONFIG_CHANGE_FAILED'
        WHEN 'PARTIAL_SUCCESS' THEN 'CONFIG_CHANGE_PARTIALLY_COMPLETED'
        WHEN 'ROLLBACK_PENDING' THEN 'CONFIG_CHANGE_ROLLBACK_REQUESTED'
        WHEN 'ROLLING_BACK' THEN 'CONFIG_CHANGE_ROLLBACK_STARTED'
        WHEN 'ROLLED_BACK' THEN 'CONFIG_CHANGE_ROLLED_BACK'
        WHEN 'ROLLBACK_FAILED' THEN 'CONFIG_CHANGE_ROLLBACK_FAILED'
        ELSE 'CONFIG_CHANGE_STATUS_CHANGED' END;

    INSERT INTO public.kf_audit_logs
        (id, action, event_category, resource_type, resource_id, resource, cluster_id,
         change_id, correlation_id, status, origin, created_time, created_by,
         host_id, host_name, host_ip, details)
    VALUES (event_uuid, event_action, 'CONFIGURATION', 'CONFIG_CHANGE', change_uuid::text,
        COALESCE(change_row.title, 'Configuration change'), change_row.cluster_id,
        change_uuid, change_uuid, COALESCE(next_status, 'UNKNOWN'), 'MANAGEMENT_SERVER',
        COALESCE(occurred_at, now()), left(COALESCE(change_row.requested_by, 'system'), 128),
        CASE WHEN jsonb_array_length(nodes) = 1 THEN nodes->0->>'hostId' END,
        CASE WHEN jsonb_array_length(nodes) = 1 THEN nodes->0->>'hostname' END,
        CASE WHEN jsonb_array_length(nodes) = 1 THEN nodes->0->>'hostIp' END,
        jsonb_build_object('message', replace(initcap(replace(event_action, '_', ' ')), 'Config Change', 'Configuration change'),
            'changeId', change_uuid, 'jobId', change_uuid, 'clusterId', change_row.cluster_id,
            'kafkaClusterId', change_row.kafka_cluster_id,
            'actor_id', change_row.requested_by, 'executor', 'MANAGEMENT_SERVER',
            'event_type', event_action, 'correlation_id', change_uuid,
            'previousStatus', previous_status, 'status', next_status,
            'role', change_row.target_role, 'scope', change_row.target_scope,
            'executionType', change_row.execution_type, 'version', change_row.version_no,
            'requestedAt', change_row.requested_at, 'completedAt', change_row.completed_at,
            'affectedNodes', nodes, 'before_state', before_snapshot, 'after_state', after_snapshot,
            'reconstructed', reconstructed,
            'evidenceSource', CASE WHEN reconstructed THEN 'STORED_CONFIG_CHANGE_HISTORY' ELSE 'CONFIG_CHANGE_TRANSACTION' END,
            'recordedAt', clock_timestamp(), 'integrity', 'NOT_ENABLED'))
    ON CONFLICT (id) DO NOTHING;
    RETURN event_uuid;
END;
$$;

CREATE FUNCTION public.audit_config_change_transition()
RETURNS TRIGGER LANGUAGE plpgsql SET search_path = public, pg_temp AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        PERFORM public.record_config_change_audit(NEW.change_id, NULL, NEW.status,
            gen_random_uuid(), COALESCE(NEW.requested_at, NEW.created_at, now()), FALSE);
    ELSIF NEW.status IS DISTINCT FROM OLD.status THEN
        PERFORM public.record_config_change_audit(NEW.change_id, OLD.status, NEW.status,
            gen_random_uuid(), COALESCE(NEW.updated_at, NEW.completed_at, now()), FALSE);
    END IF;
    RETURN NEW;
END;
$$;

CREATE CONSTRAINT TRIGGER trg_config_change_audit
AFTER INSERT OR UPDATE ON public.kf_config_change
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION public.audit_config_change_transition();

-- Recover one current-state snapshot per existing change, not invented transitions.
SELECT public.record_config_change_audit(c.change_id, NULL, c.status,
    md5('config-audit-history:' || c.change_id::text)::uuid,
    COALESCE(c.completed_at, c.updated_at, c.requested_at, c.created_at), TRUE)
FROM public.kf_config_change c
WHERE NOT EXISTS (SELECT 1 FROM public.kf_audit_logs a WHERE a.change_id = c.change_id
    AND a.resource_type = 'CONFIG_CHANGE' AND a.action LIKE 'CONFIG_CHANGE_%');

CREATE VIEW public.kf_config_change_audit AS
SELECT id AS audit_id, cluster_id, change_id, correlation_id,
    created_by AS actor_id, action AS event_type, status,
    resource_type AS entity_type, resource_id AS entity_id,
    details->'before_state' AS before_state, details->'after_state' AS after_state,
    details AS event_payload, created_time AS occurred_at
FROM public.kf_audit_logs WHERE change_id IS NOT NULL AND resource_type = 'CONFIG_CHANGE';

COMMENT ON VIEW public.kf_config_change_audit IS
    'Configuration audit aliases over the global UI ledger kf_audit_logs; KA_AUDIT design mapping. Hash-chain integrity is NOT_ENABLED; previous_hash/event_hash are not implemented.';
