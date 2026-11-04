# Cluster deletion, reservations and data-service startup (point 83)

The failure had two paths. Internal cleanup marked the cluster DELETED and released host membership, but left cluster-scoped nodes, data-service assignments and saved connections active. External Add Service projected those historical internal assignments as physical-host port/path reservations without checking the parent cluster. Thus a deleted cluster could reserve 8081/8083 even when neither host port listened.

V103 retires node and internal/external service status to DELETED, deactivates saved connections (including default flags), updates Add Service mappings, and releases membership of hosts still assigned to that deleted parent. It preserves historical inventory rows and does not change unrelated active clusters. Host ONLINE describes agent connectivity; a connected agent remains ONLINE/available after its cluster membership is removed. DELETING and DELETE_FAILED parents retain their reservations because physical cleanup may still be incomplete.

The derived node-ID refresh trigger also preserves the parent actor and avoids unnecessary parent writes on child-status-only changes.

The retirement hook runs on final DELETED transitions and parent removal, with a one-time repair of existing deleted parents. Database guards prevent late heartbeat or callback writes from resurrecting their nodes, services, mappings or active connections. New connection saves reject a deleted owner. Reservation queries join live parents independently, so a historical assignment cannot block another cluster even if its child status had become stale. Real listeners and path ownership still undergo agent prerequisite checks; a physically busy port remains blocked.

Port-validation errors name the requested service and exact REST port. The API accepts `port` and the existing `rest_port` alias, rejects conflicting values, and preserves distinct Schema Registry and Kafka Connect port values. When both services are requested, a Schema Registry port failure is explicitly labelled instead of looking like a Kafka Connect error.

## Readiness and status

Prerequisites prove that installation can proceed, not that a new JVM REST API is serving yet. The existing agent has a 120-second local readiness window; a service may become reachable after that window. Previously an agent verification timeout or one refused connection-save probe immediately failed the deployment.

The backend now has a configurable native REST readiness window (default 300 seconds, polling every 2000 milliseconds). It requires two consecutive successful samples. Schema Registry must return its subjects array. Kafka Connect must return a worker version, connectors array and both bundled FileStream plugin classes; when Kafka cluster identity is reported it must match the requested cluster. A reported mismatch is rejected. Response headers and bodies share the remaining request budget, redirects are not followed, and a stalled response cannot indefinitely block deployment.

Only an INSTALL failure explicitly reporting the agent's post-start REST verification deadline is eligible for the extra readiness window. Checksum, permission, protected path, download and other install failures are not reclassified as success. Recovery only probes the existing process; it does not issue another install/start. The original failed agent result remains historical evidence, while the job step describes subsequently confirmed readiness.

The service remains DEPLOYING while waiting. SAVE performs readiness against the agent's current reachable address and configured port before persisting its connection. ONLINE is reported only after the connection is saved and its connectivity check succeeds. A permanent failure stays FAILED with endpoint and error details. These changes are backend/database orchestration changes; agent binaries are unchanged.

Settings: `tantor.data-services.readiness-timeout-seconds` and `tantor.data-services.readiness-poll-millis`.

```sql
-- Must be zero after final deletion; a live host agent is a separate inventory concept.
SELECT count(*) FROM kf_nodes n JOIN kf_clusters c ON c.id=n.cluster_id
WHERE c.status='DELETED' AND n.status IS DISTINCT FROM 'DELETED';
SELECT count(*) FROM kf_cluster_services s JOIN kf_clusters c ON c.id=s.cluster_id
WHERE c.status='DELETED' AND s.status IS DISTINCT FROM 'DELETED';
SELECT count(*) FROM kf_data_service_connections d JOIN kf_clusters c ON c.id=d.cluster_id
WHERE c.status='DELETED' AND (d.is_active OR d.is_default OR d.status<>'DELETED');
```
