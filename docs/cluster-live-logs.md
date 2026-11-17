# Cluster Live Logs

The **Live Logs** tab under internal cluster navigation reads actual systemd journals. It uses managed service assignments for brokers and controllers. Combined broker/controller assignments appear in both categories and use the same journal.

## Implementation and rollout plan

1. Build the server and UI from this branch. For the Linux agent, use the supplied Add Node 9 source (baseline commit d3803252320080a4c0dc778ddecb928f03e9b14a) with the Live Logs client, task-engine startup hook, and version marker applied. The resulting pilot package is Tantor_Internal_Agent_addnode.9_Live_Logs.zip. It includes the modified source, checksums, and Linux amd64 and arm64 binaries. The package version ends in livelogs.1-addnode.9 to preserve the Add Node provenance. The repository agent tree differs from the supplied source package.
2. Configure the backend with a JSON object in `TANTOR_LIVE_LOGS_AGENT_TOKENS` keyed by registered agent host ID. Generate a distinct 32-byte random value per host and encode it as 64 lowercase hex characters. Put the matching value in that agent's `TANTOR_LIVE_LOG_TOKEN` via a root-owned, mode-0600 systemd environment file. Use a server HTTPS URL and an agent configuration that verifies the server CA. The agent only enables this poller when those conditions are met.
3. Roll out to one internal node first. Open its **Live Logs** view, select the correct service and node, and compare the latest entries with the unit's journal on that node. Check preset and custom IST date ranges, level and text filtering, pause and resume, and download. Repeat for separate and combined Kafka roles before enabling more nodes.
4. For an agent rollback, remove its live-log token and restore its previous binary. Removing the backend's matching token disables new reads for that host. Existing deployment task polling and Deployment Logs use their original path.

## How queries run

The authenticated UI selects a cluster service assignment. The server verifies ownership, resolves the unit from the assignment, and holds one in-memory request per host for up to 12 seconds. The token-authenticated agent polls for requests every three seconds and invokes `journalctl` with a fixed argument list for an allowlisted service unit. The agent returns up to 200 journal records within 512 KiB. No logs are written to the server database. Follow mode refreshes snapshots at the selected interval. Text and level filters and downloads operate on the displayed snapshot. Custom date ranges are interpreted in IST and limited to seven days.

This covers systemd journal output on internal nodes. File-only logs, Kafka Connect and Schema Registry journals, external agents, all-node fanout, and the prototype's simulated terminal are outside this implementation. The in-memory exchange assumes the UI and agent requests reach the same backend instance.
