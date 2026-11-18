# Internal agent: Add Node 10, Live Logs and offline Java upgrade

Linux x86_64/amd64 and ARM64 binaries are included. No Go installation is required.
Built from the Version 2 repository Add Node 10 source with broker/controller Live Logs and offline Java upgrade. The matching Tantor backend and UI are required.

## 1. Copy and extract

Copy Tantor_Internal_Agent_addnode.10_Java_Upgrade.zip to the desired Linux node (for example with scp), then:

```bash
unzip Tantor_Internal_Agent_addnode.10_Java_Upgrade.zip
cd internal-agent
sha256sum -c SHA256SUMS
uname -m
```

Use your own registered node identity and management server. Do not copy another node's agent.yaml or credentials.

## 2A. Fresh node only

First confirm /etc/tantor-agent/agent.yaml is absent and this node is not already enrolled. agent.sh writes that configuration, so use the upgrade path below on an existing agent.

```bash
sudo env TANTOR_SERVER_PORT=8443 bash agent.sh
```

When prompted for management server IP, enter 192.168.3.194 for this lab, or your desired management server. Set TANTOR_SERVER_PORT to its actual HTTP port. The script selects the architecture, installs the binary, creates a host-specific configuration and starts the service. It uses the node's hostname as its initial host ID; host IDs must be unique.

## 2B. Existing enrolled node

Drain active agent tasks and cluster jobs in Tantor first. Keep the existing agent.yaml, agent data and operation receipts.

```bash
sudo bash upgrade-add-node.sh --tasks-drained
```

This helper is for the standard /opt/tantor-agent/bin/tantor-agent installation and tantor-agent.service. It backs up the previous binary, preserves enrollment/configuration and restores the old binary if restart fails. Custom/symlink installations require reviewing their actual unit and paths first; do not rerun fresh enrollment to change the binary.

## 3. Verify

```bash
/opt/tantor-agent/bin/tantor-agent -version
sudo systemctl is-active tantor-agent.service
sudo journalctl -u tantor-agent.service -n 50 --no-pager
```

Confirm addnode.10.java-upgrade.1 and a fresh heartbeat in Tantor → Hosts. For controller/separate-process additions, candidate and all existing participants need the compatible agent. Run fresh Add Node checks after upgrade. Starting an agent does not make a Kafka-populated host eligible as a fresh Add Node candidate.

This package does not create a Kafka cluster, enable server execution gates or authorize storage cleanup. The build manifest records build-time provenance; this build has not been installed or verified on a live Kafka VM.

## Enable Live Logs

Use a verified HTTPS management-server URL and configure its trusted CA in agent.yaml. Set TANTOR_LIVE_LOG_TOKEN in a root-owned systemd environment file for tantor-agent.service. The backend must configure the corresponding per-host token in TANTOR_LIVE_LOGS_AGENT_TOKENS. See the branch rollout guide. Existing agents without this token continue ordinary operation.

## Offline Java upgrade agent task

Distribute a JAVA parcel first. Distribution stages the catalog-verified archive
under the parcel directory's .downloads folder and leaves Java unchanged.

The JAVA_UPGRADE task requires the catalog SHA-256 in Task.checksum and these
parameters: artifact_id (UUID), target_version (exact dotted version),
parcel_dir, service_name (Kafka systemd unit), service_role (broker, controller,
or broker_controller), kafka_home, and health_endpoint (Kafka host:port).
Use a broker endpoint for broker and combined roles and a controller endpoint
for controller-only roles. The probes currently expect PLAINTEXT KRaft.

Schedule only on dedicated Kafka VMs, one host at a time. The task changes
/usr/bin/java when it is a symlink, keeps the prior JDK, and preserves a
stopped Kafka service. Explicit per-service JAVA_HOME can prevent the running
JVM from adopting the VM default. The task treats that as a verification
failure and attempts to restore the previous Java link and Kafka service.
The result includes diagnostics. Validate restart, health, and recovery on
a lab Kafka VM before deploying.
