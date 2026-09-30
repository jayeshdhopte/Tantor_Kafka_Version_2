-- V3__revert_cluster_services_id_to_uuid.sql
-- Revert the ID type back to UUID because Java uses UUID.
ALTER TABLE cluster_services ALTER COLUMN id TYPE UUID USING id::uuid;
