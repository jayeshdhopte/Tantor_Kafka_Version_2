# Kafka group classification

Both managed and external clusters use `KafkaGroupInventoryService` and the same
AdminClient connection/security settings. The backend Kafka client is 4.1.2 so it
can enumerate all group protocols through `listGroups`, rather than starting with
the consumer-only API. This does not upgrade or restart brokers or agents.

| Kafka metadata | Classification | UI |
| --- | --- | --- |
| Protocol `sr` | Schema Registry coordination | Schema Registry coordination panel |
| Protocol `connect` | Connect coordination | Kafka Connect coordination panel |
| Protocol `consumer`, consumer group type, or Kafka-reported simple consumer | Consumer | Consumers table, details and lag |
| Other known non-classic group type | Other | Separate Other/unclassified panel |
| Unrecognized protocol/type | Unknown | Separate Other/unclassified panel |

Names are never used to classify groups. An application named `tantor-sr-orders`
or a sink consumer named `connect-orders` stays visible when Kafka identifies it
as a consumer. Classic inactive/simple consumer groups remain eligible.

`GET /api/v1/clusters/{clusterId}/groups` returns group ID, actual protocol, Kafka
group type, state and classification. Coordination panels show all groups of the
selected service kind in the Kafka cluster, not only one saved REST connection.
Refresh reads metadata again; it never changes membership or committed offsets.

Consumer cache refresh covers internal deployments and external-only records,
deduplicates identical cluster IDs, and filters before fetching consumer offsets
or calculating lag/search/pagination totals. Coordination groups do not contribute
to consumer lag alerts. Consumer-group describe failures are isolated per group:
affected consumers stay visible with unavailable metadata and unavailable lag.
Partial offset/partition reads display unavailable lag instead of an invented zero.

Unknown groups are never silently discarded. Whole-inventory failures show a
metadata-unavailable message; existing cached consumer rows retain their previous
last-updated timestamp until a successful refresh.

Regression coverage includes custom group names, classic/new consumer protocols,
inactive consumers, service protocols, other/unknown types, denied metadata reads,
consumer counts and external-only refresh. No group deletion, client termination,
offset reset or service restart is part of this change.

Validation on 22 October 2026: all 309 backend tests passed; the UI production
build and lint checks passed. A read-only probe using the new Kafka client and
compiled classifier against the existing Kafka cluster returned one consumer,
one Schema Registry coordination group and two Connect coordination groups.
`discovery-audit-consumer-group` was correctly classified as an actual consumer.
The probe did not deploy or restart the Tantor application.
