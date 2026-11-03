-- Both internal inventory and external inventory mirrors use kf_clusters.id.
-- Keep history: deletion must not cascade or clear the original cluster UUID.
SET LOCAL lock_timeout = '5s';

CREATE INDEX idx_add_service_cluster_id ON public.kf_add_service(cluster_id);
ALTER TABLE public.kf_add_service
    ADD CONSTRAINT fk_add_service_cluster
    FOREIGN KEY (cluster_id) REFERENCES public.kf_clusters(id)
    ON UPDATE NO ACTION ON DELETE RESTRICT NOT VALID;
ALTER TABLE public.kf_add_service VALIDATE CONSTRAINT fk_add_service_cluster;

-- A retained DELETED parent is historical inventory. Re-registering the same
-- physical Kafka cluster uses a new Tantor UUID, while keeping the historical
-- native Kafka ID intact. Active registrations must remain unique.
DROP INDEX public.uq_clusters_kafka_cluster_id;
CREATE UNIQUE INDEX uq_clusters_kafka_cluster_id ON public.kf_clusters(kafka_cluster_id)
    WHERE kafka_cluster_id IS NOT NULL AND kafka_cluster_id <> ''
      AND status IS DISTINCT FROM 'DELETED';

COMMENT ON COLUMN public.kf_add_service.cluster_id IS
    'Foreign key to kf_clusters.id for internal clusters and external inventory mirrors; nullable when unassociated. Referenced deleted parents are retained as history.';
COMMENT ON CONSTRAINT fk_add_service_cluster ON public.kf_add_service IS
    'Validated cluster relationship; blocks orphan references and physical deletion of referenced history. Application purge retains referenced parents with DELETED status.';
