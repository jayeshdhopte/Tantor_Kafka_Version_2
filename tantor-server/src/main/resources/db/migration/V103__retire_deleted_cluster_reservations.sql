-- A DELETED cluster is historical inventory, never a live service/port owner.
ALTER TABLE public.kf_data_service_connections DROP CONSTRAINT chk_dsc_status;
ALTER TABLE public.kf_data_service_connections ADD CONSTRAINT chk_dsc_status
    CHECK(status IN ('UNKNOWN','ONLINE','OFFLINE','ERROR','DELETED'));
-- Node-ID aggregation is derived inventory, not a new action by a system user.
-- In particular, retiring a child status must not overwrite the deletion requester's identity.
CREATE OR REPLACE FUNCTION public.refresh_cluster_node_ids() RETURNS TRIGGER LANGUAGE plpgsql AS $$
DECLARE affected UUID; owners UUID[]; observed JSONB;
BEGIN
    IF TG_OP='UPDATE' THEN
        IF NEW.cluster_id IS NOT DISTINCT FROM OLD.cluster_id AND NEW.node_id IS NOT DISTINCT FROM OLD.node_id THEN RETURN NEW;END IF;
        owners:=ARRAY[OLD.cluster_id,NEW.cluster_id];
    ELSIF TG_OP='INSERT' THEN owners:=ARRAY[NEW.cluster_id];
    ELSE owners:=ARRAY[OLD.cluster_id];END IF;
    FOR affected IN SELECT DISTINCT id FROM unnest(owners) AS changed(id) WHERE id IS NOT NULL LOOP
        SELECT COALESCE(jsonb_agg(ids.node_id ORDER BY ids.node_id),'[]'::jsonb) INTO observed
        FROM(SELECT DISTINCT node_id FROM public.kf_cluster_services WHERE cluster_id=affected AND node_id IS NOT NULL) ids;
        UPDATE public.kf_clusters SET node_ids=observed,updated_at=CURRENT_TIMESTAMP
          WHERE id=affected AND node_ids IS DISTINCT FROM observed;
    END LOOP;
    RETURN CASE WHEN TG_OP='DELETE' THEN OLD ELSE NEW END;
END;
$$;

CREATE FUNCTION public.retire_deleted_cluster_inventory(owner UUID) RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE actor TEXT;
BEGIN
    SELECT COALESCE(updated_by,created_by) INTO actor FROM public.kf_clusters WHERE id=owner;
    UPDATE public.kf_cluster_services SET status='DELETED' WHERE cluster_id=owner AND status IS DISTINCT FROM 'DELETED';
    UPDATE public.kf_external_services SET status='DELETED' WHERE cluster_id=owner AND status IS DISTINCT FROM 'DELETED';
    UPDATE public.kf_nodes SET status='DELETED',updated_at=now() WHERE cluster_id=owner AND status IS DISTINCT FROM 'DELETED';
    UPDATE public.kf_add_service SET status='DELETED',updated_by=COALESCE(actor,updated_by)
      WHERE cluster_id=owner AND status IS DISTINCT FROM 'DELETED';
    UPDATE public.kf_data_service_connections SET status='DELETED',is_active=FALSE,is_default=FALSE,
      updated_by=COALESCE(actor,updated_by),updated_at=now()
      WHERE cluster_id=owner AND (status IS DISTINCT FROM 'DELETED' OR is_active IS DISTINCT FROM FALSE OR is_default IS DISTINCT FROM FALSE);
    -- Host ONLINE describes agent connectivity, not membership of a deleted cluster.
    UPDATE public.kf_hosts SET cluster_id=NULL,status=CASE WHEN status='OCCUPIED' THEN 'ONLINE' ELSE status END
      WHERE cluster_id=owner;
END;
$$;
CREATE FUNCTION public.on_cluster_inventory_retired() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF TG_OP='DELETE' THEN
        PERFORM public.retire_deleted_cluster_inventory(OLD.id);RETURN OLD;
    END IF;
    IF NEW.status='DELETED' AND OLD.status IS DISTINCT FROM 'DELETED' THEN
        PERFORM public.retire_deleted_cluster_inventory(NEW.id);
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER trg_retire_deleted_cluster_inventory AFTER UPDATE OF status ON public.kf_clusters
    FOR EACH ROW EXECUTE FUNCTION public.on_cluster_inventory_retired();
CREATE TRIGGER trg_retire_removed_cluster_inventory BEFORE DELETE ON public.kf_clusters
    FOR EACH ROW EXECUTE FUNCTION public.on_cluster_inventory_retired();

-- Prevent a late heartbeat or deployment callback from resurrecting retired inventory.
CREATE FUNCTION public.guard_deleted_cluster_inventory() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS(SELECT 1 FROM public.kf_clusters WHERE id=NEW.cluster_id AND status='DELETED') THEN
        IF TG_TABLE_NAME='kf_hosts' THEN
            NEW.cluster_id:=NULL;IF NEW.status='OCCUPIED' THEN NEW.status:='ONLINE';END IF;RETURN NEW;
        END IF;
        NEW.status:='DELETED';
        IF TG_TABLE_NAME='kf_data_service_connections' THEN NEW.is_active:=FALSE;NEW.is_default:=FALSE;END IF;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER trg_guard_retired_host_membership BEFORE INSERT OR UPDATE ON public.kf_hosts
    FOR EACH ROW EXECUTE FUNCTION public.guard_deleted_cluster_inventory();
CREATE TRIGGER trg_guard_retired_nodes BEFORE INSERT OR UPDATE ON public.kf_nodes
    FOR EACH ROW EXECUTE FUNCTION public.guard_deleted_cluster_inventory();
CREATE TRIGGER trg_guard_retired_services BEFORE INSERT OR UPDATE ON public.kf_cluster_services
    FOR EACH ROW EXECUTE FUNCTION public.guard_deleted_cluster_inventory();
CREATE TRIGGER trg_guard_retired_external_services BEFORE INSERT OR UPDATE ON public.kf_external_services
    FOR EACH ROW EXECUTE FUNCTION public.guard_deleted_cluster_inventory();
CREATE TRIGGER trg_guard_retired_connections BEFORE INSERT OR UPDATE ON public.kf_data_service_connections
    FOR EACH ROW EXECUTE FUNCTION public.guard_deleted_cluster_inventory();
CREATE TRIGGER trg_guard_retired_service_mapping BEFORE INSERT OR UPDATE ON public.kf_add_service
    FOR EACH ROW EXECUTE FUNCTION public.guard_deleted_cluster_inventory();

-- Preserve historical rows and retire only inventory belonging to explicitly deleted parents.
SELECT public.retire_deleted_cluster_inventory(id) FROM public.kf_clusters WHERE status='DELETED';
COMMENT ON FUNCTION public.retire_deleted_cluster_inventory(UUID) IS 'Retire logical cluster inventory/reservations after deletion; does not stop any process or mark a connected agent offline. DELETING/DELETE_FAILED owners retain reservations.';
