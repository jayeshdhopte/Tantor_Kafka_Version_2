ALTER TABLE kf_clusters
    ADD COLUMN IF NOT EXISTS "user" VARCHAR(128),
    ADD COLUMN IF NOT EXISTS role VARCHAR(50),
    ADD COLUMN IF NOT EXISTS config_path VARCHAR(1024);

UPDATE kf_clusters
SET "user" = COALESCE(NULLIF("user", ''), NULLIF(created_by, ''), 'system')
WHERE "user" IS NULL OR "user" = '';

UPDATE kf_clusters c
SET role = service_roles.role
FROM (
    SELECT
        cluster_id,
        CASE
            WHEN bool_or(role = 'broker_controller') THEN 'broker_controller'
            WHEN bool_or(role = 'broker') THEN 'broker'
            WHEN bool_or(role = 'controller') THEN 'controller'
            WHEN bool_or(role = 'schema_registry') THEN 'schema_registry'
            WHEN bool_or(role = 'connect') THEN 'connect'
            ELSE min(role)
        END AS role
    FROM kf_cluster_services
    GROUP BY cluster_id
) service_roles
WHERE c.id = service_roles.cluster_id
  AND (c.role IS NULL OR c.role = '');

UPDATE kf_clusters
SET config_path = CASE
        WHEN LOWER(COALESCE(role, '')) = 'broker' THEN COALESCE(NULLIF(config_directory, ''), '/opt/kafka/config') || '/broker.properties'
        WHEN LOWER(COALESCE(role, '')) = 'controller' THEN COALESCE(NULLIF(config_directory, ''), '/opt/kafka/config') || '/controller.properties'
        WHEN LOWER(COALESCE(role, '')) IN ('broker_controller', 'broker+controller') THEN COALESCE(NULLIF(config_directory, ''), '/opt/kafka/config') || '/server.properties'
        ELSE config_path
    END
WHERE config_path IS NULL OR config_path = '';