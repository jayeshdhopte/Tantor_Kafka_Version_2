# External Java upgrade verification — 19 November 2026

## Result

The local external-agent workflow completed a real Java 17.0.21 to 21.0.13
upgrade on Linux/WSL. These were real processes and packages, not demo responses.
The production server at 192.168.3.194 was not accessed or changed.

| Check | Observed result |
| --- | --- |
| JDK distribution | Approved Linux AMD64 archive downloaded, SHA-256 verified and staged; distribution did not install Java. |
| Actual upgrade | Job `239fc9ab-d152-4e06-ba56-86f4a0d20d9c` succeeded over HTTPS. |
| Running JVM | Both `/usr/bin/java` and Kafka's `/proc/<MainPID>/exe` resolved to the installed Java 21.0.13 JDK. |
| Kafka health | Kafka 4.1.0 remained active; KRaft leader and voters were present and follower lag was zero. |
| Controlled failure | A temporary service JAVA_HOME override held Kafka on Java 17, intentionally failing Java 21 JVM verification. |
| Recovery | Job `49536e22-0f9b-4f60-b9b1-081fb529cdb2` failed correctly, restored the previous Java default, restarted Kafka and verified its old JVM and health. The temporary override was removed before the successful final upgrade. |
| Recovery correction | Recovery now waits for Kafka startup and health instead of checking immediately after systemctl start. |
| HTTPS | Real agent polling, JDK distribution and upgrade completed against native backend TLS with a trusted local lab CA. Untrusted certificates and wrong hostnames were rejected. |
| Backend regression | 11 focused tests passed: job routing, external eligibility/receipts and parcel handling. |
| Linux Go regression | External-agent, internal deploy and shared Java upgrade suites passed, including checksum, service identity and recovery health-wait guards. |
| Frontend | Production TypeScript/Vite build and focused lint passed. External Actions & Restarts is accessible to management roles; unsupported external monitoring/Kafka operations remain hidden. |

## Workflow

Upload an approved Linux JDK in Artifacts, distribute it to eligible enrolled
hosts, wait for DISTRIBUTED, then select the JDK in the cluster's Upgrade Java
action. The backend locks and tracks a sequential job. The external discovery
agent verifies the target identity and package, upgrades locally, and returns
logs and the result. Failure stops the rollout and attempts restoration.

Internal upgrades use the internal agent task queue. External upgrades use the
existing external Kafka task journal. Both adapters use the shared Java engine.
Ansible and internet access on the Kafka node are not required.

## Scope and deployment limits

- Live verification used one local Linux AMD64 combined broker/controller,
  KRaft and PLAINTEXT Kafka health probes. It does not prove multi-node rolling
  availability, every Kafka/JDK combination, or secure Kafka listener support.
- ARM64 is built for release; no ARM64 runtime was available for live testing.
- The production Nginx routes were inspected. The live HTTPS test used native
  Spring TLS, not the deployed production Nginx instance. Verify the production
  certificate and proxy routes separately before installing on .194. The
  artifact route upload ceiling is explicitly 512 MiB, matching the artifact
  service; this avoids Nginx's default 1 MiB limit rejecting a JDK upload.
- Root, bearer ADMIN task access, enabled tasks/config writes/restarts, the
  exact allowed systemd unit, a dedicated Kafka host and valid credentials are
  required. Shared hosts and SASL/TLS-only health probes are intentionally rejected.
- No new database migration is introduced by this integration. No local lab
  credentials, private keys, JDK archives or simulated/demo data belong in the
  release package.
