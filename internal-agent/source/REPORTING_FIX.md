# prod.16-reporting.4

Fixes compatibility with task claim tokens required by the current management server.

- Decode claim_token (also accepting claimToken) on polled tasks.
- Echo the claim on RUNNING, Kafka progress and final result reports.
- Use the new claim on cached-result redelivery without executing the task again.
- Log successful HTTP acceptance of final reports.

Authentication remains none. No management-server source or configuration was changed.

Validation: full go test ./... passed on Linux amd64, including successful and failed port/prerequisite reporting and changed-claim redelivery regression cases.

Installed on 192.168.3.191, polling http://192.168.3.194:8443.
Server source: /opt/tantor-agent/source-prod16-reporting.4

Additional fix: accept UI-generated tantor-kafka-* and kafka-* service names in deployment and lifecycle validation. Preserve explicit systemd_service precedence. Added /srv/tantor-agent/artifacts to the deployed configuration allowed roots. Full Linux tests passed. Kafka deployment needs a UI retry.

reporting.3: validate the deployed systemd unit MainPID with kill -0 instead of global pgrep; attach claim_token to KRaft/ZooKeeper VALIDATING reports. Full Linux tests and opt-in read-only process check passed against running kafka PID 640220.

reporting.4: validate Kafka ports at the configured node address; inspect JMX attachment through the unit PID command line; persist deployment state before extraction using db_cluster_id; record configured Kafka exporter service. Full Linux suite and complete live KRaft validation passed on .191. Recovered missing deployment record after verifying symlink, metadata cluster ID and unit files. No Kafka data removed.

## prod.16-reporting.5
Validates only the task-configured KRaft metadata and log directories, including comma-separated log directories. Removes unrelated hardcoded legacy directory scans. Requires readable metadata with matching cluster and node IDs. Uses service_role consistently, with role as fallback. Full Linux tests and read-only live storage validation passed on all three nodes. Multi-node deployment still requires retry and end-to-end confirmation.

## prod.16-reporting.6
Controller-only tasks no longer try to start or report a broker exporter service. Controller service units attach the required JMX exporter on the configured controller JMX port, which allows the health check to validate the same process and port. Broker and combined-role tasks keep their own monitoring ports. The same Linux amd64 build is installed on .174, .191, and .229. The full Go test suite and configuration checks passed. A fresh controller-only UI deployment is needed for end-to-end confirmation.

## prod.16-reporting.7
The per-service jmx_port now takes precedence over the cluster-level controller_jmx_port default for controller-only tasks. This keeps the generated systemd unit, metrics health check, and port selection aligned when the UI uses a custom controller JMX port. Broker and combined roles continue to use their per-service jmx_port. Regression coverage includes conflicting controller and service values.
