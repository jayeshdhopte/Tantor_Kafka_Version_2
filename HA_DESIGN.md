# Tantor Platform High Availability (HA) Design

This document outlines the high-availability (HA) architecture for the Tantor platform to ensure it meets strict enterprise SLAs.

## 1. HA Management Server (`tantor-server`)

The `tantor-server` Spring Boot application is entirely stateless. State is managed via PostgreSQL and the JWT tokens.

**Implementation**:
- Deploy at least **3 instances** of `tantor-server` across different availability zones (AZs) or fault domains.
- Place them behind an enterprise Load Balancer (e.g., HAProxy, F5, or AWS ALB).
- Configure the Load Balancer to perform mTLS termination for agents OR TCP pass-through if mTLS is terminated at the Spring Boot layer.
- Ensure sticky sessions are **NOT** required, as all requests contain the full JWT state and agent requests are fully stateless CRUD operations on the DB.

## 2. PostgreSQL HA

The database is the single source of truth for the platform (hosts, tasks, clusters, RBAC).

**Implementation**:
- Use **Patroni** or **Pgpool-II** to manage PostgreSQL streaming replication.
- Deploy a minimum of 1 Primary node and 2 Replica nodes (Sync/Async).
- Use an odd number of `etcd` or `Consul` nodes for leader election quorum.
- Configure `tantor-server` with a connection pool (HikariCP) pointing to the Pgpool VIP (Virtual IP) or a DNS endpoint managed by Patroni so failovers are transparent.

## 3. Repository HA (`tantor-artifact-repository`)

The artifact repository handles large binaries (`.tar.gz`) which are written to disk. It requires a shared persistence layer to survive container or host failure.

**Implementation**:
- **Option A (Kubernetes/NFS)**: Deploy the Spring Boot application as a Deployment with `replicas: 3`. Back the `/var/lib/tantor/repository` path with a `ReadWriteMany` (RWX) Persistent Volume, such as NFS, CephFS, or EFS.
- **Option B (S3 Object Store)**: Update the `StorageService` interface to implement an AWS S3/MinIO driver instead of local FS. This natively offloads HA to the object store provider.
- Put the repository instances behind a Load Balancer. The agents will request `https://tantor-repo:8081` which load balances across healthy instances.

## 4. Agent Failover Handling

Tantor agents operate in a pull-based polling architecture, making failover highly resilient.

**Implementation**:
- **Server Unavailability**: If the Load Balancer IP for `tantor-server` goes down, the agent's `PollInterval` logic catches the timeout. It implements exponential backoff and keeps retrying indefinitely without dropping any local metrics.
- **Task Resilience**: If an agent dies mid-task (`IN_PROGRESS`), the server uses a `stale_task_reaper` background job to detect if the agent's `last_heartbeat` has exceeded 5 minutes. The task is marked as `FAILED (AGENT_DIED)`, allowing the operator to cleanly restart it once the agent recovers.
- **DNS Failover**: The agent config (`configs/agent.yaml`) should specify the `server_url` via DNS rather than raw IPs, allowing network teams to update DNS records to point to disaster recovery (DR) sites transparently.
