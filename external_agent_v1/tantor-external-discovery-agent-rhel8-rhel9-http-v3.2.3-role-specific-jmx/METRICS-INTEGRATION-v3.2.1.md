# Production metrics integration v3.2.1

This source revision expands the external-cluster agent from two JMX counters
to controlled JMX Exporter and Kafka Exporter collection.

## Implemented

- Broker traffic, request, replication, thread-pool, connection, controller,
  KRaft, log-cleaner and JVM metric-family ingestion from JMX Exporter.
- Topic, partition, broker and consumer-group metric-family ingestion from
  Kafka Exporter.
- Explicit optional Kafka Exporter installer arguments.
- Collector status: configured, available, last attempt, last success, scrape
  duration, sample count, truncation and sanitized error.
- Collection timestamp and node ID in each metrics payload.
- Controlled metric-family and label allowlists.
- 16 MiB response limit, 10,000 sample scrape limit and 1,000 sample
  per-family limit.
- Non-finite value rejection.
- Correct broker-aggregate counter rate calculation without adding per-topic
  series to broker totals.
- Counter state keyed by host, Kafka cluster, node and exporter endpoint.
- Unit tests for filtering, controlled labels, aggregation, counter reset and
  Kafka Exporter families.

## Backend contract addition

The existing summary fields remain unchanged. The metrics endpoint must accept:

- `nodeId`
- `collectedAt`
- `jmxCollector`
- `kafkaExporter`
- `jmxMetrics[]`
- `kafkaExporterMetrics[]`

Each raw sample contains `name`, `labels`, `value` and optional `type`.
Backend aggregation, retention, health scores, SLAs, forecasts and
recommendations remain central control-plane responsibilities.

## Build provenance

The bundled amd64 and arm64 executables are freshly built from this source with
Go 1.22.2, `CGO_ENABLED=0`, `-trimpath`, and version `3.2.1`. The release
checksum manifest covers the binaries, installer, source, tests and documents.
