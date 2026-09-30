-- V46: Retarget legacy host foreign keys from hosts to kf_hosts.
-- After the kf_ table rename, these constraints can still point to the empty
-- legacy hosts table and block task/parcel creation for registered hosts.

ALTER TABLE IF EXISTS kf_tasks
    DROP CONSTRAINT IF EXISTS tasks_host_id_fkey;

ALTER TABLE IF EXISTS kf_tasks
    ADD CONSTRAINT tasks_host_id_fkey
    FOREIGN KEY (host_id) REFERENCES kf_hosts(id) ON DELETE CASCADE;

ALTER TABLE IF EXISTS kf_host_parcels
    DROP CONSTRAINT IF EXISTS host_parcels_host_id_fkey;

ALTER TABLE IF EXISTS kf_host_parcels
    ADD CONSTRAINT host_parcels_host_id_fkey
    FOREIGN KEY (host_id) REFERENCES kf_hosts(id) ON DELETE CASCADE;

ALTER TABLE IF EXISTS kf_cluster_services
    DROP CONSTRAINT IF EXISTS services_host_id_fkey;

ALTER TABLE IF EXISTS kf_cluster_services
    ADD CONSTRAINT services_host_id_fkey
    FOREIGN KEY (host_id) REFERENCES kf_hosts(id) ON DELETE RESTRICT;
