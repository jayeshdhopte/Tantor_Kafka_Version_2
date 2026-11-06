Tantor Internal Agent

Add Node build: 1.0.0-prod.17-dataservices.4-configchange.10-rollingrestart.1-addnode.5
Existing enrolled nodes: drain tasks, then use
  sudo bash upgrade-add-node.sh --tasks-drained
This preserves agent.yaml and the enrolled identity, backs up the old executable,
and restarts only the internal agent. Use agent.sh for fresh enrollment.
Broker Add Node mutations in this adapter require a Linux root agent. Deployment
and current Kafka-version acceptance status: docs/add-node-deployment-194.md.

Config backup integrity (configchange.10-backupintegrity.1):
Physical Kafka config backups are read back and SHA-256 compared with captured
source bytes before mutation. Task receipts allow the matching backend to store
checksums and actual verification times. See docs/config-backup-integrity.md in
the repository. Upgrade the binary and backend together; keep existing node
configuration, identities, and stopped/running service states during upgrades.

Go source: source/
Linux amd64 executable: bin/tantor-agent-linux-amd64
Linux arm64 executable: bin/tantor-agent-linux-arm64
Simple node installer: agent.sh

The agent deploys Kafka, Schema Registry, and Kafka Connect through management
server tasks. Schema Registry and Kafka Connect use the shared plaintext
prechecks in source/internal/deploy/dataservice/precheck.sh. Build the binary
from source/ and install it on the selected Kafka host before starting a
service deployment job.

For the simple HTTP setup, copy this whole internal-agent folder to a Linux
node and run `sudo bash agent.sh` inside it. The script asks only for the
management server IPv4 address, uses HTTP port 8080, installs the included
binary, and starts the tantor-agent systemd service. If the matching binary
is absent, Go 1.22+ is needed on the node to build from source. The optional
TANTOR_SERVER_PORT environment variable overrides 8080 without an extra prompt.

See ../docs/data-service-deployment.md for the UI and job flow.

Kafka Connect unified-installation support (dataservices.3):
Leave managed Connect directory fields blank to reuse the host's Kafka runtime,
config/connect-distributed.properties, and connect-plugins. Existing properties
are backed up; unrelated settings and plugin JARs are retained. This release
requires the matching backend/UI changes. Updating the agent alone does not
update the management UI. See ../docs/data-service-deployment.md.

Schema Registry unified-installation support (dataservices.4):
Leave managed Schema Registry install/config fields blank to reuse Kafka's
bin/schema-registry-start, share/java/schema-registry libraries, and
etc/schema-registry/schema-registry.properties. Shared configuration is backed
up and unrelated settings preserved. No separate distribution is extracted.
Existing standalone services require an explicit migration; they are not moved
by an installation retry. Requires the matching backend/UI changes.

Kafka configuration backup update (configchange.4):
Static and dynamic managed Kafka configuration changes require a successful
node-side backup before mutation. Backups live beside the active properties
file under .tantor-backups/<filename>/. Dynamic changes also save live value
and source metadata as .dynamic.json. A root agent cleans managed checkpoints
older than seven days at startup and hourly; active config files are preserved.
Requires the matching updated backend. This package is not deployed automatically.

Standalone rolling restart support (rollingrestart.1):
The matching backend/UI offers Kafka process scope selection, sequential health
checks, and Retry / Resume without configuration rollback. VERIFY_KAFKA_SERVICE
reads systemd state, PID and process start time without starting or stopping a
service. Completed restart tasks are preserved across retries. Combined Kafka
processes restart both roles together; Connect and Schema Registry are excluded.
This agent release requires deployment of the matching backend/UI to expose the
new cluster menu. Upgrading agents alone does not update the UI.

Configuration backups: visible config-backup/<config filename>/ next to the active config file. Seven-day retention remains enabled, including registered legacy .tantor-backups checkpoints.

Backup filenames: broker.properties.bak.<UTC timestamp> (likewise controller.properties/server.properties). Dynamic snapshots add .dynamic.json to that same filename. Existing version/job-prefixed filenames remain eligible for seven-day cleanup.

Configuration removal (configchange.7): reviewed deletions remove active property lines, back up file/dynamic state first, and use one health-checked restart per target. Updated property files end with a newline.

Broker plan v2: new operations use broker.properties. Legacy v1 operations retain
server.properties and their original receipts for safe resume; no live file is renamed.
Controller plans in this increment are read-only; this agent does not execute controller addition.
