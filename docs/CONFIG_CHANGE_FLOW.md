# Kafka configuration change flow

This implementation uses the existing `kf_clusters`, `kf_external_clusters`, `kf_jobs`, `kf_job_steps`, configuration versions, and agent task records. The handover ZIP's `kafka_ops` SQL is a standalone example schema and is not applied on top of these existing tables.

## Entry and targeting

On **Clusters**, open **⋮ → Configuration change**. The popup loads the current service topology and shows the Kafka cluster ID, version, source, status, bootstrap address, broker count, controller count, and nodes. Continue to choose a role and either all or specific nodes. Combined broker/controller processes are excluded from role-only rolling restarts.

## Internal clusters

- **Dynamic broker properties:** the management server reads live Kafka broker configuration, checks the selected property is writable on every target, previews each old/new value, runs cluster health checks, then queues a durable job. The job applies one broker at a time through Kafka AdminClient and reads the value back. It does not restart Kafka.
- **Static broker and controller properties:** preview dispatches `READ_KAFKA_PROPERTY` to each selected internal agent, so the displayed old value comes from the active node file. The management server requires at least two dedicated brokers or three dedicated controllers, checks Kafka health, and orders controller followers before the active leader. The durable job dispatches `PATCH_KAFKA_CONFIG` per node. The agent checks the expected value, backs up the original file, changes only the requested property, restarts that Kafka service, and verifies the active file. The server verifies broker values or KRaft quorum and waits for cluster health before moving to the next node.
- The existing per-node versioned editor now uses the same targeted patch agent command for file changes. It still provides immutable version history and explicit rollback-version creation.

## External clusters

- Dynamic broker changes use the same Kafka AdminClient preview and job as internal clusters.
- Static broker and controller changes open the agent-backed editor limited to the selected nodes. The editor reads each active file, stages additions, edits or removals, and sends one rolling job. The server validates each discovered node/file, blocks mixed roles and combined processes, and orders the active controller last. The external agent compares expected file values under a file lock, creates backups before writing, and the server verifies node values and cluster health after each restart.

## Safety and deployment notes

- Topology and credential-bearing property names are blocked from this flow. Cluster jobs sharing `resourceKey=cluster:<id>` execute serially. A failed step stops later nodes; automatic restart rollback is not attempted. Inspect the checkpoint and use an explicit recovery change.
- The new static flow requires the updated internal and external agent binaries in this repository to be installed on their corresponding nodes. Old internal agents do not understand `READ_KAFKA_PROPERTY` or `PATCH_KAFKA_CONFIG`.
- Internal static preview waits up to 60 seconds for each agent read. If an agent is offline or its active file differs from preview, the change stops before editing that node.
- Broker/controller rolling changes require a healthy multi-node KRaft cluster. Single-node and combined-process role-only rolling changes are intentionally blocked.
- External agent tasks currently use the management server's in-memory dispatch queue. The job and its steps persist, but a management-server restart while an external task is in flight may require inspecting the node and creating a recovery change before retrying.
