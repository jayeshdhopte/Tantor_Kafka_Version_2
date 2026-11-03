-- Mirror the specific onboarding approval into the immutable global ledger.
-- Reuse the activity UUID so backfill and subsequent inserts cannot duplicate it.
-- Existing registration PENDING/ONLINE rows are historical snapshots: never edit them.
CREATE OR REPLACE FUNCTION audit_host_onboarding_approval()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.event_type = 'APPROVAL' AND NEW.action = 'APPROVE'
       AND NEW.resource_type = 'HOST' AND NEW.message = 'Host onboarding approved' THEN
        INSERT INTO kf_audit_logs
            (id, action, event_category, resource_type, resource_id, resource,
             status, origin, created_time, host_id, host_ip, host_name,
             cluster_id, created_by, details)
        SELECT NEW.id, 'HOST_ONBOARDING_APPROVED', 'APPROVAL', 'HOST', NEW.resource_id,
               COALESCE(h.hostname, NEW.resource_id), COALESCE(NEW.event_status, 'SUCCESS'),
               'MANAGEMENT_SERVER', NEW.created_at, NEW.resource_id, h.host_ip, h.hostname,
               NEW.cluster_id, COALESCE(NEW.actor, 'system'),
               jsonb_build_object('message', NEW.message, 'oldStatus', NEW.old_value,
                   'newStatus', NEW.new_value, 'approvalStatus', NEW.approval_status,
                   'metadata', NEW.metadata, 'activityId', NEW.id)
        FROM (SELECT 1) anchor LEFT JOIN kf_hosts h ON h.id = NEW.resource_id
        ON CONFLICT (id) DO NOTHING;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_host_onboarding_approval_audit
AFTER INSERT ON kf_activity_logs
FOR EACH ROW EXECUTE FUNCTION audit_host_onboarding_approval();

INSERT INTO kf_audit_logs
    (id, action, event_category, resource_type, resource_id, resource,
     status, origin, created_time, host_id, host_ip, host_name,
     cluster_id, created_by, details)
SELECT a.id, 'HOST_ONBOARDING_APPROVED', 'APPROVAL', 'HOST', a.resource_id,
       COALESCE(h.hostname, a.resource_id), COALESCE(a.event_status, 'SUCCESS'),
       'MANAGEMENT_SERVER', a.created_at, a.resource_id, h.host_ip, h.hostname,
       a.cluster_id, COALESCE(a.actor, 'system'),
       jsonb_build_object('message', a.message, 'oldStatus', a.old_value,
           'newStatus', a.new_value, 'approvalStatus', a.approval_status,
           'metadata', a.metadata, 'activityId', a.id)
FROM kf_activity_logs a LEFT JOIN kf_hosts h ON h.id = a.resource_id
WHERE a.event_type = 'APPROVAL' AND a.action = 'APPROVE'
  AND a.resource_type = 'HOST' AND a.message = 'Host onboarding approved'
ON CONFLICT (id) DO NOTHING;

-- Only the legacy default on internal agents; preserve custom names, IDs,
-- hostnames and all pre-existing audit snapshots. AgentService applies the
-- same normalization on future registrations from older binaries.
UPDATE kf_hosts SET agent_name = 'tantor-agent'
WHERE lower(agent_name) = lower(hostname)
  AND agent_version LIKE '1.%'
  AND lower(id) NOT LIKE 'external-%' AND lower(id) NOT LIKE 'discovery-%';
