# External Kafka management release

Version: `3.4.0-dataservices.2-configchange.8-rollingrestart.4`.

## Included

- Static configuration preview, multiple property updates and removals, expected
  old-value checks, atomic writes, backup before mutation, and rolling jobs.
- Dynamic configuration uses the backend Kafka Admin API and its validation;
  the agent captures file and dynamic checkpoints for the managed workflow.
- Backups use the existing visible `config-backup/<filename>/` directory,
  `<filename>.bak.<IST timestamp>` naming, and seven-day retention.
- Rolling restart supports brokers, controllers, all, or selected processes.
  Combined processes require acknowledgement because both roles restart together.
  Existing cluster health gates, controller ordering, process identity checks,
  failure stop and Retry / Resume are reused.
- Multiple Schema Registry and Connect instances use distinct directories,
  REST ports and systemd units. Instances for the same logical service share its
  coordination group and internal Kafka topics.
- External clusters expose Add Service, Configuration Change, Rolling Restart
  and Delete in the cluster menu. Existing job and configuration history flows
  are reused, with discovered target identity.

## Upgrade order and prerequisites

1. Deploy the matching backend and UI, including Flyway migration V85.
2. Install the matching agent on each external Kafka host participating in
   management. Refresh discovery to populate active configuration paths, exact
   systemd units and capability flags. Older discovery records cannot be used
   for these managed operations.
3. Enable only the required permissions. Configuration backup/write and service
   deployment require root. Restart requires the exact discovered unit in the
   explicit service allowlist. Reads and verification still require tasks.
4. Managed Kafka tasks require bearer authentication with a valid backend ADMIN
   token. Existing discovery authentication behavior is unchanged. Keep the
   token file private and refresh the configured token before expiry. A missing
   or expired token prevents task polling; it does not bypass API authorization.

Installer flags for an operator-approved managed host:

```bash
sudo ./install-agent.sh --server-url http://MANAGEMENT_HOST:PORT \
  --node-name "$(hostname -f)" --environment test --scan-paths /opt/kafka \
  --run-user root --run-group root --enable-tasks \
  --auth-type bearer --token-file /secure/path/backend-admin-token \
  --enable-config-write --enable-service-restart \
  --allowed-services kafka-broker.service,kafka-controller.service \
  --enable-service-deployment
```

Use the host's actual discovered units and paths. Each service instance on the
same host must have a free REST port and non-overlapping directories. Deployment
still checks Java, time synchronization, storage, artifact and plugin access,
Kafka reachability and internal topic compatibility.

## Limits and recovery

This release does not install/upgrade/remove Kafka itself. New Schema Registry /
Connect deployment supports PLAINTEXT Kafka only. Static configuration changes
to combined-role processes retain the existing internal workflow restriction;
combined-role rolling restart is supported. Protected and sensitive properties
remain excluded. Broker availability and ISR health can legitimately block jobs.

Tasks are bound to the discovered node, Kafka cluster identity, active file and
systemd service. Agent journals prevent replay of an uncertain completed
mutation. After interruption, inspect the reported process and checkpoint before
retrying. The agent does not automatically roll back a restart-only operation.

## Verification for this build

All 273 backend tests passed. The UI production build and strict lint passed.
Linux tests on an isolated workspace on `.213` passed for shared config backup,
Kafka patching, data service deployment and the external agent; amd64 and arm64
binaries were built. UI actions were checked using a local mocked external
cluster. These checks did not execute a live external Kafka restart or service
deployment, or apply V85 to the production database. No deployment is included.
