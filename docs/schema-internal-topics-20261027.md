# Schema Registry internal topics

Basic fresh Schema Registry installations omit metadata encoder secrets and DEK Registry resource extensions inherited from the parcel. Authentication extensions and Kafka TLS/SASL properties are preserved. Both agents use the shared installer, covering cluster-form and Add Service deployment, including HA settings. Conflicting inherited JVM encryption options fail the basic service startup with a redacted configuration error.

Existing services retain their encryption/authentication configuration during port-only retry; this release does not automatically migrate existing encrypted services or delete any Kafka topics. Optional topics already present remain on Kafka.

The Topics API filters known platform topics before search, counts and pagination when includeInternal=false. When true, they appear alongside application topics and are marked internal. Exact names include _schemas, _schema_encoders, _dek_registry_keys, standard Connect topics and broker-reported internal topics. Ordinary customer topics beginning with an underscore remain visible. The UI defaults to hiding internal topics and ignores stale responses after toggling.

Validation: 119 UI tests; 380 backend tests (0 failures, 3 optional skips); complete Linux Go suites for both agents; Linux amd64/arm64 builds for both agents. Production UI build and systemd startup-guard syntax validation passed. No production service or topic was changed during implementation.

Acceptance checks after deployment:
1. With Show internal topics off, search for each optional topic: no row and no count contribution.
2. Turn it on: any existing optional topics appear with an internal badge. Quickly turn it off: no stale rows return.
3. On a clean Kafka test cluster, deploy a basic Schema Registry using each deployment flow. Register/read a schema; _schemas is available and the optional encryption topics are absent. Repeat single-node and HA deployments with both agent types.
4. Confirm configured TLS/SASL and authentication extensions remain intact. Existing encrypted installations require a reviewed migration rather than automatic removal.
