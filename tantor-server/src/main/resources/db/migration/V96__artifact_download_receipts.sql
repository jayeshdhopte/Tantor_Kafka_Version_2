-- A transport start does not prove receipt or checksum verification. Keep the
-- original rows, timestamps and actors, with an accurate event type/unknown result.
ALTER TABLE kf_artifact ADD COLUMN task_id UUID;
ALTER TABLE kf_artifact ADD COLUMN executor TEXT;
CREATE UNIQUE INDEX ux_artifact_download_receipt_task ON kf_artifact(task_id)
    WHERE action = 'DOWNLOAD' AND task_id IS NOT NULL;

UPDATE kf_artifact SET action = 'DOWNLOAD_STARTED', verified_checksum = NULL
WHERE action = 'DOWNLOAD' AND task_id IS NULL AND verified_checksum IS NOT TRUE;

-- Only server-persisted, authenticated task results are accepted as evidence.
-- An HTTP request, a successful unrelated task, or a missing checksum is insufficient.
CREATE FUNCTION record_parcel_download_receipt(task_uuid UUID)
RETURNS UUID LANGUAGE plpgsql AS $$
DECLARE
    task_row kf_tasks%ROWTYPE;
    source_row kf_artifact%ROWTYPE;
    request_row kf_artifact_audit_log%ROWTYPE;
    host_row kf_hosts%ROWTYPE;
    artifact_uuid UUID;
    receipt_uuid UUID;
    initiator TEXT;
    verified BOOLEAN;
    completed_at TIMESTAMPTZ;
BEGIN
    SELECT * INTO task_row FROM kf_tasks WHERE id = task_uuid;
    IF NOT FOUND OR task_row.command <> 'DISTRIBUTE_PARCEL'
       OR task_row.status NOT IN ('SUCCESS', 'FAILED') THEN RETURN NULL; END IF;
    IF COALESCE(task_row.parameters->>'artifact_id', '') !~*
       '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
       THEN RETURN NULL; END IF;
    artifact_uuid := (task_row.parameters->>'artifact_id')::UUID;
    SELECT * INTO source_row FROM kf_artifact WHERE id = artifact_uuid AND action = 'UPLOAD';
    IF NOT FOUND THEN RETURN NULL; END IF;

    IF concat_ws(E'\n', task_row.log_output, task_row.error_msg) ILIKE '%checksum verification failed%' THEN
        verified := FALSE;
    ELSIF COALESCE(task_row.log_output, '') LIKE '%Checksum verified successfully%'
       AND task_row.checksum ~* '^[0-9a-f]{64}$'
       AND lower(task_row.checksum) = lower(source_row.checksum) THEN
        verified := TRUE;
    ELSE RETURN NULL;
    END IF;

    SELECT * INTO request_row FROM kf_artifact_audit_log
    WHERE action = 'PARCEL_DISTRIBUTE_REQUESTED' AND artifact_id = artifact_uuid::TEXT
      AND details::jsonb->>'taskId' = task_uuid::TEXT
    ORDER BY created_at LIMIT 1;
    SELECT * INTO host_row FROM kf_hosts WHERE id = task_row.host_id;
    initiator := COALESCE(NULLIF(task_row.parameters->>'initiated_by', ''),
        NULLIF(request_row.user_name, ''), NULLIF(request_row.created_by, ''), 'unknown');
    SELECT min(created_at) INTO completed_at FROM kf_artifact_audit_log
    WHERE action IN ('PARCEL_DISTRIBUTE_SUCCESS', 'PARCEL_DISTRIBUTE_FAILED')
      AND details::jsonb->>'taskId' = task_uuid::TEXT;

    INSERT INTO kf_artifact
        (id, root_artifact_id, action, service_type, version_no, binary_file_name,
         path_of_tar, full_file_path, file_size_bytes, content_type, checksum,
         checksum_md5, status, created_by, updated_by, user_name, downloaded_by,
         created_time, update_time, downloaded_at, verified_checksum, hostname,
         host_ip, task_id, executor)
    VALUES
        (gen_random_uuid(), COALESCE(source_row.root_artifact_id, source_row.id),
         'DOWNLOAD', source_row.service_type, source_row.version_no, source_row.binary_file_name,
         source_row.path_of_tar, source_row.full_file_path, source_row.file_size_bytes,
         source_row.content_type, source_row.checksum, source_row.checksum_md5, source_row.status,
         initiator, initiator, initiator, initiator, now(), now(), COALESCE(completed_at, now()),
         verified, COALESCE(NULLIF(task_row.parameters->>'target_hostname', ''), host_row.hostname, task_row.host_id),
         COALESCE(NULLIF(task_row.parameters->>'target_host_ip', ''), request_row.details::jsonb->>'hostIp', host_row.host_ip),
         task_uuid, 'agent:' || task_row.host_id)
    ON CONFLICT (task_id) WHERE action = 'DOWNLOAD' AND task_id IS NOT NULL DO NOTHING
    RETURNING id INTO receipt_uuid;
    IF receipt_uuid IS NULL THEN
        SELECT id INTO receipt_uuid FROM kf_artifact WHERE task_id = task_uuid AND action = 'DOWNLOAD';
    END IF;
    RETURN receipt_uuid;
END;
$$;

-- Reconstruct only receipts supported by retained task checksum/log evidence.
SELECT record_parcel_download_receipt(id) FROM kf_tasks
WHERE command = 'DISTRIBUTE_PARCEL' AND status IN ('SUCCESS', 'FAILED');

-- Audit history is immutable. Append corrections linked to old completion IDs,
-- retaining the original actor and timestamp in the correction's evidence.
INSERT INTO kf_artifact_audit_log
    (id, user_name, created_by, event_category, action, resource_type, artifact_id,
     status, details, created_at, host_ip, host_name)
SELECT gen_random_uuid(), requested.user_name, requested.user_name, 'DISTRIBUTION',
       'PARCEL_ATTRIBUTION_CORRECTED', completed.resource_type, completed.artifact_id,
       completed.status,
       (completed.details::jsonb || jsonb_build_object(
           'correctsAuditId', completed.id, 'originalAction', completed.action,
           'originalActor', COALESCE(completed.user_name, completed.created_by),
           'originalCreatedAt', completed.created_at, 'initiatedBy', requested.user_name,
           'executor', 'agent:' || (completed.details::jsonb->>'hostId')) ),
       now(), requested.details::jsonb->>'hostIp', hosts.hostname
FROM kf_artifact_audit_log completed
JOIN LATERAL (
    SELECT a.* FROM kf_artifact_audit_log a
    WHERE a.action = regexp_replace(completed.action, '_(SUCCESS|FAILED)$', '_REQUESTED')
      AND a.artifact_id = completed.artifact_id
      AND a.details::jsonb->>'taskId' = completed.details::jsonb->>'taskId'
      AND NULLIF(a.user_name, '') IS NOT NULL AND a.user_name <> 'system'
    ORDER BY a.created_at LIMIT 1
) requested ON TRUE
LEFT JOIN kf_hosts hosts ON hosts.id = completed.details::jsonb->>'hostId'
WHERE completed.action ~ '^PARCEL_.*_(SUCCESS|FAILED)$'
  AND COALESCE(completed.user_name, completed.created_by, 'system') = 'system'
  AND NOT EXISTS (SELECT 1 FROM kf_artifact_audit_log correction
      WHERE correction.action = 'PARCEL_ATTRIBUTION_CORRECTED'
        AND correction.details::jsonb->>'correctsAuditId' = completed.id::TEXT);
