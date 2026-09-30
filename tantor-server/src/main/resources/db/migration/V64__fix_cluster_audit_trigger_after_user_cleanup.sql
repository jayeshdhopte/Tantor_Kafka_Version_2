-- Repair the cluster audit trigger after the kf_clusters."user" and
-- kf_cluster_audit_log.actor_user columns were removed.
--
-- V61 recreated sync_cluster_to_cluster_audit() with a reference to NEW."user".
-- V62 then dropped kf_clusters."user", so status updates started failing at
-- runtime with: record "new" has no field "user".

CREATE OR REPLACE FUNCTION sync_cluster_to_cluster_audit()
RETURNS TRIGGER AS $$
DECLARE
    cluster_action VARCHAR(100);
    bootstrap_ip VARCHAR(100);
    cluster_actor VARCHAR(128);
BEGIN
    IF TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;

    IF NEW.status IS DISTINCT FROM OLD.status AND upper(COALESCE(NEW.status, '')) = 'SUCCESS' THEN
        cluster_action := 'CLUSTER_DEPLOYED';
    ELSIF NEW.status IS DISTINCT FROM OLD.status AND upper(COALESCE(NEW.status, '')) = 'FAILED' THEN
        cluster_action := 'CLUSTER_DEPLOYMENT_FAILED';
    ELSE
        RETURN NEW;
    END IF;

    bootstrap_ip := split_part(split_part(COALESCE(NEW.bootstrap_servers, ''), ',', 1), ':', 1);
    cluster_actor := COALESCE(NULLIF(NEW.updated_by, ''), NULLIF(NEW.created_by, ''), 'system');

    INSERT INTO kf_cluster_audit_log (
        cluster_id,
        action,
        event,
        severity,
        title,
        description,
        status,
        origin,
        resource,
        resource_type,
        cluster_name,
        bootstrap_ip,
        env,
        kafka_version,
        mode,
        created_by,
        created_at,
        log_path,
        details
    )
    VALUES (
        NEW.id,
        cluster_action,
        cluster_action,
        CASE
            WHEN upper(COALESCE(NEW.status, '')) = 'FAILED'
                THEN 'ERROR'
            ELSE 'INFO'
        END,
        replace(initcap(replace(lower(cluster_action), '_', ' ')), ' ', ' '),
        'Cluster '
            || COALESCE(NEW.cluster_name, NEW.id::text)
            || ' status is '
            || COALESCE(NEW.status, 'UNKNOWN'),
        CASE
            WHEN upper(COALESCE(NEW.status, '')) = 'FAILED'
                THEN 'FAILED'
            ELSE 'SUCCESS'
        END,
        'MANAGEMENT_SERVER',
        COALESCE(NULLIF(NEW.cluster_name, ''), NEW.id::text),
        'CLUSTER',
        NEW.cluster_name,
        NULLIF(bootstrap_ip, ''),
        NEW.environment,
        NEW.kafka_version,
        NEW.mode,
        cluster_actor,
        now(),
        COALESCE(
            NULLIF(NEW.log_directory, ''),
            NULLIF(NEW.config_path, '')
        ),
        jsonb_build_object(
            'status', NEW.status,
            'kafkaVersion', NEW.kafka_version,
            'mode', NEW.mode,
            'nodeIds', NEW.node_ids,
            'bootstrapServers', NEW.bootstrap_servers,
            'installDirectory', NEW.install_directory,
            'configPath', NEW.config_path
        )
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sync_cluster_audit_to_global()
RETURNS TRIGGER AS $$
BEGIN
    INSERT INTO kf_audit_logs (
        id,
        action,
        resource_type,
        resource_id,
        resource,
        event_category,
        status,
        details,
        origin,
        created_time,
        cluster_id,
        host_ip,
        created_by
    )
    VALUES (
        NEW.id,
        NEW.action,
        COALESCE(NULLIF(NEW.resource_type, ''), 'CLUSTER'),
        NEW.cluster_id::text,
        COALESCE(NULLIF(NEW.cluster_name, ''), NEW.cluster_id::text),
        'CLUSTER',
        CASE
            WHEN upper(COALESCE(NEW.status, '')) = 'FAILED'
                THEN 'FAILED'
            ELSE 'SUCCESS'
        END,
        NEW.details,
        COALESCE(NULLIF(NEW.origin, ''), 'MANAGEMENT_SERVER'),
        NEW.created_at,
        NEW.cluster_id,
        NULLIF(NEW.bootstrap_ip, ''),
        COALESCE(NULLIF(NEW.created_by, ''), 'system')
    )
    ON CONFLICT (id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sync_host_to_global_audit()
RETURNS TRIGGER AS $$
DECLARE
    host_action VARCHAR(100);
    host_status VARCHAR(50);
    host_actor VARCHAR(255);
BEGIN
    IF TG_OP = 'INSERT' THEN
        host_action := COALESCE(NULLIF(NEW.action, ''), 'HOST_REGISTERED');
    ELSE
        host_action := COALESCE(NULLIF(NEW.action, ''), 'HOST_UPDATED');
    END IF;

    host_status := CASE
        WHEN COALESCE(NEW.removed, FALSE) THEN 'REMOVED'
        ELSE COALESCE(NULLIF(NEW.status, ''), 'UNKNOWN')
    END;

    host_actor := COALESCE(NULLIF(NEW.agent_name, ''), NULLIF(NEW.hostname, ''), NEW.id, 'system');
    IF NULLIF(NEW.host_ip, '') IS NOT NULL AND host_actor NOT LIKE '%(%' THEN
        host_actor := host_actor || ' (' || NEW.host_ip || ')';
    END IF;

    INSERT INTO kf_audit_logs (
        action,
        event_category,
        resource_type,
        resource_id,
        resource,
        status,
        origin,
        created_time,
        host_id,
        host_ip,
        host_name,
        cluster_id,
        created_by,
        details
    )
    VALUES (
        host_action,
        'AGENT',
        'HOST',
        NEW.id,
        COALESCE(NULLIF(NEW.hostname, ''), NULLIF(NEW.host_ip, ''), NEW.id),
        host_status,
        'AGENT',
        COALESCE(NEW.updated_at, NEW.created_at, now()),
        NEW.id,
        NULLIF(NEW.host_ip, ''),
        NULLIF(NEW.hostname, ''),
        NEW.cluster_id,
        host_actor,
        jsonb_build_object(
            'availability', CASE
                WHEN COALESCE(NEW.removed, FALSE)
                    OR upper(host_status) IN ('OCCUPIED', 'PENDING', 'OFFLINE', 'REMOVED')
                    THEN 'unavailable'
                ELSE 'available'
            END,
            'agentName', NEW.agent_name,
            'agentVersion', NEW.agent_version,
            'agentPath', NEW.agent_path,
            'javaVersion', NEW.java_version
        )
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION sync_artifact_audit_to_global()
RETURNS TRIGGER AS $$
DECLARE
    artifact_text TEXT;
    artifact_actor VARCHAR(128);
BEGIN
    artifact_text := NULLIF(NEW.artifact_id::text, '');
    artifact_actor := COALESCE(NULLIF(NEW.created_by, ''), NULLIF(NEW.user_name, ''), 'system');

    INSERT INTO kf_audit_logs (
        id,
        action,
        resource_type,
        resource_id,
        resource,
        event_category,
        status,
        details,
        origin,
        created_time,
        host_ip,
        host_name,
        created_by
    )
    VALUES (
        NEW.id,
        NEW.action,
        COALESCE(NULLIF(NEW.resource_type, ''), 'ARTIFACT'),
        artifact_text,
        'Artifact',
        COALESCE(NULLIF(NEW.event_category, ''), 'PACKAGE'),
        COALESCE(NULLIF(NEW.status, ''), 'SUCCESS'),
        NEW.details::jsonb,
        'ARTIFACT_REPO',
        NEW.created_at,
        NULLIF(NEW.host_ip, ''),
        NULLIF(NEW.host_name, ''),
        artifact_actor
    )
    ON CONFLICT (id) DO NOTHING;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;
