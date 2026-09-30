Tantor External Kafka Discovery Agent v3.2.3
=============================================

Scope
-----
Install locally on an existing Kafka VM to discover active Kafka broker, KRaft controller, and ZooKeeper roles and report system-level information outbound to the Tantor backend. This bundle is not the internal managed-cluster deployment agent.

Production defaults
-------------------
- Air-gapped: no internet download or online package installation.
- Outbound HTTP only in the current project phase; no inbound agent listener.
- SELinux is preserved.
- Exact client scan roots, environment, and node identity are mandatory.
- Discovery policy defaults to running-only. Offline filesystem inventory is explicitly opt-in and is not connectable.
- Config write and service restart are disabled by default.
- Backend task polling is disabled by default; use --enable-tasks only when
  explicitly required and authorized.
- Runtime user should be pre-provisioned; creation requires --create-runtime-user.

Bundle validation
-----------------
  sha256sum -c SHA256SUMS

Generic installation
--------------------
  sudo ./install-agent.sh \
    --server-ip <backend-host> \
    --server-port <backend-port> \
    --run-user <approved-agent-user> \
    --run-group <approved-agent-group> \
    --node-name <approved-node-identity> \
    --environment <prod|dr|uat|dev> \
    --scan-paths <kafka-install-root>,<kafka-data-root> \
    --cluster-name <logical-cluster-name> \
    --discovery-policy running-only \
    --require-server-reachable

Optional JMX Exporter monitoring:

  --jmx-ip <jmx-exporter-host> --jmx-port <jmx-exporter-port>
  --controller-jmx-ip <controller-exporter-host> --controller-jmx-port <controller-exporter-port>

The broker and controller JMX Exporter ports are selected per discovered Kafka
process role and included in its discovery report. For separate KRaft processes,
configure both endpoint pairs. For a combined broker+controller JVM, configure
only the broker pair because one exporter represents that single JVM.

Both options must be supplied together. The endpoint must be the HTTP
Prometheus/JMX Exporter endpoint, not the native Java JMX/RMI registry port.
The installer validates /metrics before starting the agent. If the client has
no JMX Exporter, omit the corresponding option pair. A controller-only process
never reuses the broker endpoint when the controller endpoint is omitted.

Optional Kafka Exporter monitoring:

  --kafka-exporter-ip <kafka-exporter-host> --kafka-exporter-port <kafka-exporter-port>

Both options must be supplied together. Kafka Exporter is optional; when it is
omitted, broker/JVM and host monitoring continue, but consumer-group lag and
Kafka Exporter topic/partition series are reported as unavailable.

Production metrics payload:

The agent keeps the existing summary fields and also publishes controlled raw
metric samples from JMX Exporter and Kafka Exporter. It includes collector
availability, last attempt/success, scrape duration, sample count, truncation
and collection error so unavailable telemetry is not displayed as a real zero.
Only approved Kafka/JVM families and bounded labels are forwarded. The backend
remains responsible for fleet/cluster rollups, health scores, SLAs, forecasts,
correlation and recommendations.

Before rollout, the backend metrics DTO and persistence pipeline must accept the
v3.2.1 fields listed in METRICS-INTEGRATION-v3.2.1.md. The installer validates
the local collectors and Kafka protocol, but it cannot make an older backend
display fields that the backend does not yet store.

Kafka validation:

The installer locates kafka-broker-api-versions.sh below --scan-paths and runs
it as the selected runtime user against the active broker listener. Installation
fails if Kafka is stopped, the broker is unreachable, the endpoint is not Kafka,
or the supplied TLS/SASL client configuration is invalid. This is a Kafka
protocol check, not a TCP-only check.

For an authenticated broker listener such as SASL_SSL, supply the existing
client configuration:

  --kafka-client-properties <absolute-client-properties-path>

The path must be absolute. The selected runtime user must be able to read the
properties file and every truststore/keystore referenced by it. When existing
group permissions are insufficient, the installer applies a minimal per-user
ACL: read-only on the exact security file and traverse-only on its parent
directories. It never makes credential files world-readable. For production,
prefer a dedicated least-privilege monitoring identity instead of an
administrator credential.

For an unsecured PLAINTEXT broker, omit --kafka-client-properties completely.
The installer then runs the same Kafka ApiVersions protocol validation without
Kafka's --command-config option. No empty or dummy client properties file is
required.

The selection is strictly conditional:

  Secured Kafka   -> pass --kafka-client-properties /absolute/path/client.properties
  Unsecured Kafka -> do not pass --kafka-client-properties

Supplying the option makes the file mandatory and readable. Omitting the option
does not trigger any client-properties validation.

Use --service-name <name> when the client requires a custom systemd unit name.

Prechecks
---------
The startup precheck and runtime discovery use the same configured scan roots. A running Kafka JVM is the production source of truth. Under running-only policy, no active cluster is reported when no Kafka server JVM is visible.

Verification
------------
  systemctl status <service-name> --no-pager -l
  journalctl -u <service-name> -n 100 --no-pager
  sudo -u <agent-user> test -r <active-kafka-config> && echo READABLE

Multi-node clusters
-------------------
Install one agent on every VM requiring system-level visibility. The agent reports the local broker/controller node. The backend must aggregate unique node IDs for cluster-wide counts. The payload contains localBrokerCount for explicit semantics while retaining brokerCount for backward compatibility.

Lifecycle
---------
When a previously running node disappears from discovery, the agent reports the same node with isRunning=false. Backend heartbeat TTL/decommission policy must still be configured and tested.

Security inference
------------------
When listener security cannot be proven from Kafka properties, the agent reports UNKNOWN rather than assuming PLAINTEXT.
