-- Add the native Kafka identifier alongside the existing Tantor UUID.
-- Preserve existing records, constraints, immutable audit history and writers.
CREATE FUNCTION public.resolve_native_kafka_cluster_id(tantor_cluster_id UUID)
RETURNS TEXT
LANGUAGE sql VOLATILE
SET search_path = public, pg_temp
AS $$
    SELECT CASE
        WHEN EXISTS (SELECT 1 FROM public.kf_external_clusters WHERE id = tantor_cluster_id)
        THEN (SELECT NULLIF(btrim(kafka_cluster_id), '') FROM public.kf_external_clusters WHERE id = tantor_cluster_id)
        ELSE (SELECT NULLIF(btrim(kafka_cluster_id), '') FROM public.kf_clusters WHERE id = tantor_cluster_id)
    END;
$$;

CREATE FUNCTION public.fill_native_kafka_cluster_id()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
BEGIN
    -- Always resolve from inventory: do not trust a caller-supplied native ID
    -- that could belong to another cluster. Missing observations stay NULL.
    NEW.kafka_cluster_id := public.resolve_native_kafka_cluster_id(NEW.cluster_id);
    RETURN NEW;
END;
$$;

DO $$
DECLARE
    target_table TEXT;
BEGIN
    FOREACH target_table IN ARRAY ARRAY[
        'kf_activity_logs', 'kf_add_service', 'kf_alerts', 'kf_audit_logs',
        'kf_cluster_audit_log', 'kf_cluster_operation_lock', 'kf_cluster_services',
        'kf_config_change', 'kf_config_versions', 'kf_data_service_connections',
        'kf_discovery_agents', 'kf_external_cluster_nodes', 'kf_external_kafka_tasks',
        'kf_external_services', 'kf_host_alerts', 'kf_hosts', 'kf_nodes',
        'kf_rolling_restart_table', 'kf_tasks'
    ] LOOP
        EXECUTE format('ALTER TABLE public.%I ADD COLUMN kafka_cluster_id TEXT', target_table);
        EXECUTE format('COMMENT ON COLUMN public.%I.kafka_cluster_id IS %L', target_table,
            'Native Kafka cluster ID observed in kf_external_clusters.kafka_cluster_id or kf_clusters.kafka_cluster_id; cluster_id remains the Tantor UUID. Snapshot on insert or cluster-ID update; NULL when unknown; no historical backfill.');
        EXECUTE format(
            'CREATE TRIGGER trg_fill_native_kafka_cluster_id BEFORE INSERT OR UPDATE OF cluster_id, kafka_cluster_id ON public.%I FOR EACH ROW EXECUTE FUNCTION public.fill_native_kafka_cluster_id()',
            target_table);
    END LOOP;
END;
$$;

-- No UPDATE/backfill: historical audit entries are append-only and existing
-- inventory UPDATE triggers can have business side effects. Existing rows get
-- no invented historical snapshot; subsequent cluster-association writes resolve
-- the then-known ID through the new trigger. No existing function is replaced.
