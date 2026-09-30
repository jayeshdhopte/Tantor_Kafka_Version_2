# Tantor Artifact Repository — Phase 1

The artifact repository is the source of truth for every binary the Tantor
platform installs onto customer hosts (Kafka, Kafka Connect, Schema Registry,
ksqlDB, Cruise Control, Prometheus, Grafana). Tantor agents pull artifacts from
here over HTTPS/mTLS and **verify the SHA-256 checksum before extracting
anything** — critical for the BFSI, air-gapped deployments this platform targets.

> Authentication, RBAC and mTLS termination are delivered in **Phase 3**. Phase 1
> deliberately keeps the repository self-contained so it can be tested in
> isolation; uploads are attributed to `system` until the security module lands.

---

## Architecture

```
  Tantor UI / CLI                  Tantor Management Server
        |                                    |
        |  upload / curl                     |  registers artifact coordinates
        v                                    v
  +-----------------------------------------------------------+
  |              Artifact Repository (this service)           |
  |                                                           |
  |   Controllers  ->  Services  ->  PostgreSQL (index)       |
  |                         |                                 |
  |                         +----->  Filesystem (binaries)    |
  +-----------------------------------------------------------+
        ^                                    |
        | download (X-Checksum-SHA256)       |  air-gap bundle (.tar.gz)
        |                                    v
  Tantor Agents (Golang)              physical media -> isolated estate
```

Two stores are kept in lockstep by `ArtifactService`:

| Store        | Holds                              | Technology         |
|--------------|------------------------------------|--------------------|
| Index        | metadata, checksums, manifest, audit | PostgreSQL 16.14 (JSONB) |
| Object store | the actual `.tar.gz` binaries       | local FS / PVC     |

Every mutating operation is transactional across both: a failed checksum or
write rolls the row back and removes the partial file.

## On-disk layout

```
{base-path}/artifacts/
  kafka/3.7.0/kafka_2.13-3.7.0.tgz
  kafka/3.7.0/manifest.json
  connect/3.7.0/...
  schema-registry/7.6.0/...
  ksqldb/0.29.0/...
  cruise-control/2.5.137/...
  prometheus/2.54.0/prometheus-2.54.0.linux-amd64.tar.gz
  grafana/11.2.0/...
```

`manifest.json` sits beside each binary so an air-gap bundle is self-describing
without a database.

## Database schema

`V1__init_artifact_schema.sql` (Flyway). Two tables:

- `artifact` — one row per (service_type, version, classifier), with the manifest
  stored as `JSONB`, the SHA-256/MD5 checksums, status lifecycle, and an
  optimistic-lock `version_lock` column.
- `artifact_download_log` — append-only audit of every download (who, when,
  source IP, whether the checksum was verified).

```
 artifact (1) ----< (many) artifact_download_log
```

Status lifecycle: `UPLOADING -> AVAILABLE -> {CORRUPTED | QUARANTINED | DELETED}`.

## REST API

Base path `/api/v1`. Swagger UI at `/swagger-ui.html`.

| Method | Path                         | Purpose                                  |
|--------|------------------------------|------------------------------------------|
| POST   | `/artifacts`                 | Upload (multipart)                       |
| PUT    | `/artifacts` (octet-stream)  | Upload large file via raw streaming      |
| GET    | `/artifacts`                 | List with `serviceType` / `status` filters, paged |
| GET    | `/artifacts/{id}`            | Metadata                                 |
| GET    | `/artifacts/{id}/manifest`   | Manifest JSON                            |
| GET    | `/artifacts/{id}/download`   | Stream binary + `X-Checksum-SHA256` header |
| POST   | `/artifacts/{id}/verify`     | Re-verify on-disk integrity              |
| DELETE | `/artifacts/{id}`            | Soft-delete                              |
| GET    | `/bundles/export`            | Export `.tar.gz` air-gap bundle          |
| POST   | `/bundles/import`            | Import a bundle (re-verifies checksums)  |

### Upload example

```bash
curl -X POST http://localhost:8081/api/v1/artifacts \
  -F serviceType=KAFKA \
  -F version=3.7.0 \
  -F classifier=2.13 \
  -F 'sha256=2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824' \
  -F 'attributesJson={"minJavaVersion":"17","kraft":"true"}' \
  -F file=@kafka_2.13-3.7.0.tgz
```

If `sha256` is supplied and `tantor.repository.enforce-checksum=true`, an upload
whose computed digest disagrees is rejected (HTTP 422) and the partial file is
deleted.

### Download (agent-side contract)

The agent reads `X-Checksum-SHA256` from the response headers, hashes the bytes
it received, and aborts the deployment on mismatch. The `ETag` carries the same
value for HTTP cache validation.

## Air-gapped workflow

1. On an internet-connected staging server, populate the repository, then
   `GET /api/v1/bundles/export` → `tantor-bundle-YYYYMMDD-HHmmss.tar.gz`.
2. Move the bundle across the air-gap on approved media.
3. On the isolated customer server, `POST /api/v1/bundles/import` with the file.
   Each binary is re-checksummed against the SHA-256 in its manifest before being
   admitted; a tampered bundle fails the import.

## Build & run

```bash
# Requires Java 21, a reachable PostgreSQL 16.14, Maven Central (or a mirror)
export TANTOR_DB_URL=jdbc:postgresql://localhost:5432/tantor
export TANTOR_DB_USER=tantor_dev
export TANTOR_DB_PASSWORD='<generated-local-only-password>'
export TANTOR_REPO_PATH=/var/lib/tantor/repository

mvn clean package
java -jar target/tantor-artifact-repository-1.0.0.jar
```

All three database variables are mandatory. There is intentionally no default
JDBC host, username, or password. In the Podman deployment the URL is
`jdbc:postgresql://database:5432/tantor`; credentials come from environment
variables in the developer composition and mounted secret files in production.
The management readiness endpoint includes both database health and the
read-only `artifactSchema` indicator. That indicator requires
`public.kf_artifact` plus a successful Flyway V67 history entry. V67 is the
latest `tantor-server` migration defining the current artifact service-type
contract; the Artifact Repository verifies it but never runs migrations.

### Tests

- `ChecksumServiceTest`, `ArtifactServiceTest` — pure unit tests, no Docker.
- `ArtifactControllerIT` — full upload/download round-trip against PostgreSQL 16.14
  via Testcontainers (needs a Docker daemon; auto-skips otherwise).

### Container / Kubernetes

```bash
docker build -t registry.translab.io/tantor/artifact-repository:1.0.0 .
kubectl apply -f deploy/k8s/artifact-repository.yaml
```

The PVC backs `/var/lib/tantor/repository`. For an HA repository, use a
`ReadWriteMany` volume (NFS/CephFS) so multiple replicas share one object store
behind the PostgreSQL index — the full HA design is Phase 13.
```
```
