# Tantor Platform Implementation Plan

This document outlines the architecture and implementation plan for the Tantor Kafka management platform, picking up from the completed Phase 1 (Artifact Repository).

## Current State
- **Phase 1 (Artifact Repository)** is completed and located at `tantor-artifact-repository`. It's a Spring Boot application using Java 21 and PostgreSQL 16.

## Overall Architecture
The platform is designed to run in highly secure, air-gapped BFSI environments with passwordless sudo (but NO passwordless SSH). 
1. **Tantor UI**: React 19 + TypeScript frontend for users to manage the platform.
2. **Tantor Management Server**: Spring Boot 3.x backend handling business logic, cluster management, RBAC, and task scheduling. Connects to PostgreSQL.
3. **Artifact Repository**: Source of truth for all deployable binaries (.tar.gz). Already built.
4. **Tantor Agents (Golang)**: Lightweight Go 1.24 agents installed on each infrastructure node. They pull tasks from the management server, download artifacts, verify checksums, and perform local deployments via `sudo`.

---

## Phase 2: Agent Development (Next Step)

We will build the **Tantor Agent** in Golang (Go 1.24). 
The agent will communicate with the Management Server via HTTPS/mTLS. It uses a pull-based model (polling for tasks) to avoid needing inbound open ports or SSH access from the server.

### Proposed Agent Architecture & Structure
Project Directory: `tantor-agent`

```text
tantor-agent/
├── cmd/
│   └── agent/
│       └── main.go                 # Entry point
├── configs/
│   └── agent.yaml                  # Configuration template
├── internal/
│   ├── config/                     # Configuration loading
│   ├── client/                     # HTTP client to talk to Tantor Server & Artifact Repo
│   ├── collect/                    # System metrics collection (CPU, Mem, Disk, OS, Java)
│   ├── deploy/                     # Deployment engine (Kafka, Connect, etc.)
│   ├── executor/                   # Command execution wrapper (sudo)
│   ├── task/                       # Task polling and execution engine
│   └── server/                     # Local HTTP server (for prometheus metrics if needed)
├── pkg/
│   ├── api/                        # Shared API DTOs
│   ├── checksum/                   # SHA-256 verification utilities
│   └── logger/                     # Structured logging
├── go.mod
└── go.sum
```

### Agent Capabilities to Implement
1. **Host Registration**: Register itself with the management server upon startup.
2. **Heartbeat & Metrics**: Periodically send CPU, memory, disk, OS info, and Java versions.
3. **Task Polling & Execution**: Poll the management server for pending tasks (e.g., install Kafka).
4. **Artifact Management**: Securely download `.tar.gz` from Artifact Repo and verify checksums before extraction.
5. **Service Management**: Start/stop/restart systemd services, generate configs.

---

## Future Phases (Brief Outline)

- **Phase 3 (Server Development)**: Build the `tantor-server` (Spring Boot 3.x) with RBAC, Inventory, Cluster, and Deployment modules.
- **Phase 4 (Database Design)**: Complete PostgreSQL 16 schema design for `users`, `roles`, `hosts`, `clusters`, `tasks`, etc.
- **Phase 5-9 (Deployment Engines)**: Logic in the management server and agent to deploy KRaft clusters, Connect, Schema Registry, ksqlDB.
- **Phase 10 (Monitoring)**: Prometheus + Grafana provisioning.
- **Phase 11 (UI Development)**: React 19 + TypeScript dashboard.
- **Phase 12 (Installer)**: `install-agent.sh` bash script.
- **Phase 13 (HA Design)**: High availability guidelines and configurations.

---

> [!IMPORTANT]
> ## User Review Required
> Please review this implementation plan for the Golang Agent (Phase 2). Once approved, I will generate the complete source code, directories, and files for the agent module by module, followed by Phase 3 and Phase 4.

## Open Questions
1. Should the Go agent be placed in `C:\Users\Translab\Downloads\tantor-kfka\tantor-agent`?
2. Do you have a preferred logging library for Go (e.g., `slog` or `zap`)? I recommend the standard library's `log/slog` (introduced in Go 1.21).
3. Should we use a specific framework for the Management Server (Phase 3) beyond Spring Boot? (e.g., specific security implementations for JWT?)
