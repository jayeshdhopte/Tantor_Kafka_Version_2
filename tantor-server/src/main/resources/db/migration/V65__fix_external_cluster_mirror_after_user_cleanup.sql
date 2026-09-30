-- Repair the external-cluster mirror trigger after kf_clusters."user" was removed.
--
-- V49 created sync_external_cluster_to_cluster() with an INSERT into kf_clusters."user".
-- V62 later dropped that column, so external cluster registration can fail at runtime
-- when kf_external_clusters is inserted or updated.

CREATE OR REPLACE FUNCTION sync_external_cluster_to_cluster()
RETURNS TRIGGER AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        UPDATE kf_clusters SET status = 'DELETED', deleted_at = now() WHERE id = OLD.id;
        RETURN OLD;
    END IF;

    INSERT INTO kf_clusters (
        id, cluster_name, origin_type, kafka_cluster_id, install_directory, log_directory,
        node_ids, created_by, updated_by, kafka_version, mode, environment,
        bootstrap_servers, config_json, created_at, updated_at, status
    )
    VALUES (
        NEW.id,
        COALESCE(NULLIF(NEW.cluster_name, ''), 'External cluster ' || left(NEW.id::text, 8)),
        'EXTERNAL',
        NULLIF(NEW.kafka_cluster_id, ''),
        NULLIF(NEW.install_path, ''),
        NULLIF(NEW.log_dirs, ''),
        '[]'::jsonb,
        COALESCE(NULLIF(NEW.created_by, ''), 'system'),
        COALESCE(NULLIF(NEW.updated_by, ''), NULLIF(NEW.created_by, ''), 'system'),
        COALESCE(NULLIF(NULLIF(NEW.kafka_version, ''), 'null'), 'Unknown'),
        'EXTERNAL',
        COALESCE(NULLIF(NEW.environment, ''), 'unknown'),
        NEW.bootstrap_servers,
        jsonb_build_object(
            'managementMode', 'BOOTSTRAP_ONLY',
            'kafkaMode', NEW.kafka_mode,
            'security', NEW.security,
            'listeners', NEW.listeners,
            'advertisedListeners', NEW.advertised_listeners,
            'processRoles', NEW.process_roles,
            'brokerCount', NEW.broker_count,
            'memoryUsedMb', NEW.memory_used_mb,
            'memoryTotalMb', NEW.memory_total_mb,
            'diskUsedGb', NEW.disk_used_gb,
            'diskTotalGb', NEW.disk_total_gb,
            'cpuUsagePct', NEW.cpu_usage_pct,
            'running', NEW.is_running
        )::text,
        COALESCE(NEW.created_at, now()),
        COALESCE(NEW.updated_at, now()),
        COALESCE(NULLIF(NEW.status, ''), 'SUCCESS')
    )
    ON CONFLICT (id) DO UPDATE SET
        cluster_name = EXCLUDED.cluster_name,
        origin_type = 'EXTERNAL',
        kafka_cluster_id = EXCLUDED.kafka_cluster_id,
        install_directory = EXCLUDED.install_directory,
        log_directory = EXCLUDED.log_directory,
        kafka_version = EXCLUDED.kafka_version,
        mode = 'EXTERNAL',
        environment = EXCLUDED.environment,
        bootstrap_servers = EXCLUDED.bootstrap_servers,
        config_json = EXCLUDED.config_json,
        updated_at = now(),
        status = EXCLUDED.status;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
