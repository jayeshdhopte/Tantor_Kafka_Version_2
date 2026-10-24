# External cluster connection options (51)

Reference inspected: Kafbat UI v1.5.0, Configure new cluster, on
http://192.168.3.213:8086/ (24 October 2026).

## Included

- Repeatable bootstrap server rows, including bracketed IPv6. Stored as Kafka's
  comma-separated bootstrap list; duplicates are normalized.
- Optional Schema Registry and Kafka Connect named connections during external
  cluster registration. Existing data-service connection persistence, truststore
  inheritance, health checks and audit events are reused. These options connect
  to running services; they do not install anything.
- Cluster menu > Connection settings, available after connection. Bootstrap edits
  are verified with the saved Kafka security settings and cannot switch a known
  cluster identity to a different Kafka cluster. The mirrored bootstrap list and
  AdminClient cache are updated after a successful save.
- Metrics: keep discovery settings, override broker/controller HTTP exporter
  ports, or disable external exporter scraping. Both Prometheus service discovery
  and direct broker segment-metric target resolution use the persisted options.
  These are Prometheus/JMX Exporter HTTP endpoints. Native JMX/RMI collection is
  not added, and exporters are not installed by this form.
- Masking: MASK, REMOVE and REPLACE, with exact field names or a Java regex for
  field names, scoped independently to topic keys and values by topic regex.
  Nested JSON objects/arrays are handled. Redaction occurs before API responses
  and message-search matching; Kafka records and offsets are never changed.
  For applicable rules, non-JSON payloads are redacted as a whole rather than
  leaking undecodable data. Headers are outside these key/value masking rules.

SerDes and ksqlDB options are excluded, per the requested scope. Existing Kafka
authentication, truststore and keystore inputs remain. Certificate management
and removal of saved service connections remain in the respective service tabs;
removing a newly added unsaved connection row only discards that draft.

GET/PUT connection settings enforce existing CONFIGURATION_READ and
CONFIGURATION_CHANGE permissions. Saved settings contain metrics and masking
rules, not authentication secrets. Service certificates/passwords continue to
use the existing connection storage and safe response DTOs.

Migration V88 adds nullable JSONB connection_settings to kf_external_clusters.
Existing clusters keep their current discovered ports and have no masking rules
until explicitly configured. No agent protocol or agent binary change is needed.

## Point 40 investigation

The process creating discovery-audit-consumer-group on .194 was a manually
launched Kafka ConsoleConsumer in session-156.scope, reading test-topic from
192.168.3.174:9092. Its parent was PID 1; it was not a Tantor service dependency.
It received SIGTERM and subsequent process inspection confirmed it exited. Its
launch arguments were preserved in a root-readable-only file beneath
/root/tantor-diagnostic-backups/. No consumer group offsets or group metadata
were deleted. Kafka may retain an empty group until retention or explicit cleanup.

No deployment code references that group. Schema Registry and Connect worker
coordination groups remain required and are already classified separately by
their broker-reported protocol. Sink connector data-consumer groups are real
consumers and remain visible. Topic browsing uses manual assignment with auto
commit disabled; it now also explicitly removes group.id from its client
properties so browsing cannot join a consumer group.

## Verification

1. Open external cluster connection form. Add two bootstrap endpoints and optional
   named service connections. Test Kafka connectivity, then connect. Service tabs
   should offer the saved connections with their actual health status.
2. Open cluster menu > Connection settings. Reload after a save to verify settings
   persist. A different physical Kafka cluster's bootstrap address must be refused.
3. Configure custom exporter ports and inspect cluster-scoped Prometheus targets.
   Discovery mode should retain discovered ports; disabled mode should omit that
   external cluster's exporter targets.
4. Configure a password field rule for a disposable JSON topic. The messages API
   must return redacted values, including nested arrays. Searching for the raw
   secret must not return a match. An unrelated topic/key scope must be unaffected.
5. Monitor-only accounts must not be able to PUT settings. Saved connection
   certificates/passwords must not appear in GET responses.

The new application changes are not deployed by implementation/testing alone.
