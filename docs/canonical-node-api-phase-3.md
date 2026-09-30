# Canonical Node Resolver and API — Phase 3

Status: implemented and committed in source only. It is not deployed, and the
frontend has not been migrated to this response yet.

## Endpoint

```http
GET /api/v1/clusters/{canonicalClusterUuid}/nodes
```

The path parameter is the canonical cluster UUID introduced in V72. Deleted
clusters are excluded.

The response has one cluster contract and one normalized node collection:

```json
{
  "cluster": {
    "clusterUuid": "734212c0-3bfc-42e1-928a-94b6858ee558",
    "kafkaClusterId": "Z4yJfDCjQfGewRMtOjkt8Q",
    "type": "EXTERNAL",
    "mode": "KRAFT"
  },
  "nodes": [
    {
      "identity": {
        "clusterUuid": "734212c0-3bfc-42e1-928a-94b6858ee558",
        "kafkaClusterId": "Z4yJfDCjQfGewRMtOjkt8Q",
        "nodeId": 1,
        "role": "BROKER"
      },
      "host": "192.168.3.229",
      "agentStatus": "ONLINE",
      "telemetryStatus": "LIVE"
    }
  ]
}
```

## Source selection

The resolver starts from exactly one non-deleted `kf_clusters` row selected by
`canonical_cluster_uuid`.

- `INTERNAL`: Kafka node identity comes from `kf_cluster_services`; agent and
  telemetry status come from the relationally bound `kf_hosts.host_id`.
- `EXTERNAL`: Kafka node identity and node telemetry come from
  `kf_external_cluster_nodes.canonical_cluster_uuid`; discovery-agent status is
  read only from agents whose `cluster_id` is that same canonical UUID.

Schema Registry, Kafka Connect, exporter and ZooKeeper service assignments are
not returned as Kafka nodes.

External discovery agents are currently cluster-scoped, not node-scoped. Their
online/offline status therefore applies to the cluster's node observations. A
node-specific agent association will require an explicit relational node key in
a later schema; the resolver will not infer it from an IP address or hostname.

## Identity and failure behavior

The resolver never searches for identity by cluster name, agent name, IP,
hostname or bootstrap server. These values remain display/discovery metadata.

- unknown/deleted canonical UUID: `404 Not Found`
- missing Kafka cluster id, node id, or role: `409 Conflict`
- duplicate `clusterUuid + kafkaClusterId + nodeId + role`: `409 Conflict`

This is deliberately fail-closed. Incorrect metadata is surfaced for repair
instead of silently associating a metric or agent with the wrong node.

## Phase boundary

Existing overview, brokers, monitoring and frontend pages are not rewired in
this phase. Deploying the backend by itself would change the JSON contract of
the existing nodes endpoint, so backend and the later frontend adaptation must
be released together after review.
