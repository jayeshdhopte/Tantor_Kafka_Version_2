ALTER TABLE public.kf_add_service ADD COLUMN cluster_origin TEXT;

COMMENT ON COLUMN public.kf_add_service.cluster_origin IS
    'INTERNAL or EXTERNAL, resolved from kf_external_clusters membership or kf_clusters.origin_type using cluster_id; NULL when inventory cannot classify the cluster.';

CREATE FUNCTION public.resolve_add_service_cluster_origin(tantor_cluster_id UUID)
RETURNS TEXT
LANGUAGE sql VOLATILE
SET search_path = public, pg_temp
AS $$
    SELECT CASE
        WHEN EXISTS (SELECT 1 FROM public.kf_external_clusters WHERE id = tantor_cluster_id)
            THEN 'EXTERNAL'
        ELSE (SELECT CASE WHEN upper(btrim(origin_type)) IN ('INTERNAL', 'EXTERNAL')
                    THEN upper(btrim(origin_type)) ELSE NULL END
              FROM public.kf_clusters WHERE id = tantor_cluster_id)
    END;
$$;

CREATE FUNCTION public.fill_add_service_cluster_origin()
RETURNS TRIGGER
LANGUAGE plpgsql
SET search_path = public, pg_temp
AS $$
BEGIN
    NEW.cluster_origin := public.resolve_add_service_cluster_origin(NEW.cluster_id);
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_fill_add_service_cluster_origin
BEFORE INSERT OR UPDATE OF cluster_id, cluster_origin ON public.kf_add_service
FOR EACH ROW EXECUTE FUNCTION public.fill_add_service_cluster_origin();

-- Classify existing service summaries only when actual inventory proves the
-- origin. Existing fields, native-ID snapshots and audit events are untouched.
UPDATE public.kf_add_service
SET cluster_origin = public.resolve_add_service_cluster_origin(cluster_id)
WHERE cluster_origin IS NULL
  AND public.resolve_add_service_cluster_origin(cluster_id) IS NOT NULL;
