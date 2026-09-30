# Tantor Kafka Platform

Tantor Kafka is a Kafka operations platform with:

- `tantor-server`: Spring Boot management server on port `8443`
- `tantor-artifact-repository`: Spring Boot artifact repository on port `8081`
- `tantor-ui`: React/Vite frontend
- `internal-agent/source`: Go agent source for managed Kafka hosts
- `tantor-discovery-agent`: Go agent for external Kafka cluster discovery

Schema Registry and Kafka Connect deployment from managed and external clusters
is described in [Data service deployment](docs/data-service-deployment.md).

The server uses Flyway migrations to create and update the PostgreSQL schema.

## Prerequisites

Install these before running the project:

- Java 21
- PostgreSQL 16.14
- Node.js 20 or newer
- npm
- PowerShell
- Go, only if you need to rebuild agents

On Linux/VM deployment hosts, the Kafka agent expects:

- Java 17 or Java 21
- systemd
- network access from agent host to Tantor server and artifact repository
- enough disk space under the selected Kafka install/data directories

## 1. Clone The Repository

```powershell
git clone https://github.com/divyatranslab/Tantor_kafka.git
cd Tantor_kafka
```

## 2. Create PostgreSQL Database

Create one database named `tantor`.

Using `psql`:

```sql
CREATE DATABASE tantor;
```

Or from terminal:

```powershell
psql -U postgres -c "CREATE DATABASE tantor;"
```

The tables are created automatically by Flyway when `tantor-server` starts.

## 3. Create `.env`

Copy the example file:

```powershell
Copy-Item .env.example .env
```

Then edit `.env` for your machine.

Configuration ownership, supported profiles, production requirements, and
safe startup diagnostics are documented in
[Runtime configuration](docs/configuration.md).

Minimum local development values are shown below. Generate a random,
development-only password rather than copying a shared or production secret.
When Compose is used, PostgreSQL is reachable from the host only through
`127.0.0.1:5432`.

```properties
TANTOR_DB_URL=jdbc:postgresql://localhost:5432/tantor
TANTOR_DB_USER=tantor_dev
TANTOR_DB_PASSWORD=<generated-local-only-password>
TANTOR_REPO_INTERNAL_URL=http://localhost:8081
TANTOR_REPO_PUBLIC_URL=https://localhost:8443
TANTOR_PUBLIC_ORIGIN=https://localhost:8443
TANTOR_REPO_PATH=./.runtime/repository
TANTOR_MONITORING_MODE=direct
TANTOR_PROMETHEUS_URL=http://127.0.0.1:9090
TANTOR_MONITORING_EXPORTER_HOST=127.0.0.1
```

For SIT, UAT, and production, supply the internal repository service URL,
public HTTPS origin, monitoring endpoint, and agent endpoints through the
deployment environment. Do not copy local addresses into a production profile.

Do not commit real passwords or production secrets in `.env`. Containerized
services connect privately through `database:5432`; production does not publish
the database port on the host.

See [Repository credential and history security](docs/repository-security.md)
for rotation, secret scanning, prohibited artifacts, and coordinated history
cleanup requirements.

### Podman composition

Start the repository composition after setting the required values in `.env`:

```bash
podman-compose --env-file .env --file podman-compose.yml up --detach --build
```

PostgreSQL must pass `pg_isready` before `tantor-server` runs the Flyway
migrations. Production `start.sh` enforces this sequence explicitly with
`up --no-deps` and health polling; correctness does not depend on the Compose
provider honoring `depends_on`. The Artifact Repository uses the explicit
`jdbc:postgresql://database:5432/tantor` URL and becomes ready only when its
database is connected, `public.kf_artifact` exists, and server-owned Flyway
migration V67 is recorded as successful. Missing database settings fail
startup; there is no localhost database fallback.

To validate the same sequence against a fresh, isolated project and volumes:

```bash
bash scripts/test-h01-deployment.sh
```

The validator deliberately starts the Artifact Repository once with PostgreSQL
unavailable and once with an empty database. It verifies bounded failure and a
503 readiness response until `tantor-server` migrates the schema, then exercises
the production file-backed secret/config-tree path and restart persistence. It
removes its uniquely named test containers and volumes when complete.

## 4. Build Backend And Agents

From the repository root:

```powershell
.\build.ps1
```

This builds:

- `tantor-artifact-repository/target/tantor-artifact-repository-1.0.0.jar`
- `tantor-server/target/tantor-server-1.0.0.jar`
- `internal-agent/bin/tantor-agent-linux-amd64`
- `tantor-discovery-agent/tantor-discovery-agent-linux-amd64`
- `tantor-discovery-agent/tantor-discovery-agent-linux-arm64`

## 5. Start Backend Locally

For foreground logs:

```powershell
.\start-backend-dev.ps1
```

For background services:

```powershell
.\start-backend.ps1 -Restart
```

Stop background services:

```powershell
.\stop-backend.ps1
```

Expected backend URLs:

- Management server: `http://localhost:8443`
- Artifact repository: `http://localhost:8081`

Health checks:

```powershell
curl http://localhost:8443/api/v1/monitoring/health
curl http://localhost:8081/actuator/health
```

## 6. Start Frontend

```powershell
cd tantor-ui
npm install
npm run dev
```

Open the URL printed by Vite, usually:

```text
http://localhost:5173
```

Authentication is disabled by default. For local Vite development only, enable
Keycloak with development environment values:

```properties
VITE_AUTH_ENABLED=true
VITE_KEYCLOAK_URL=https://identity.development.internal
VITE_KEYCLOAK_REALM=Gatekeeper
VITE_KEYCLOAK_CLIENT_ID=apb-kafka
```

Production does not consume compiled `VITE_*` identity settings. Release
packaging generates `ui-runtime-config.js` and validates its public origin,
OIDC values, API routes, Nginx routes, CORS, and CSP together.

## 7. First Data Setup In UI

After backend and UI are running:

1. Open the UI.
2. Upload a Kafka `.tgz` binary in Artifacts.
3. Upload the JMX exporter `.jar` in Artifacts using service type `JMX Exporter`.
4. Register or start a Tantor agent on the target Kafka host.
5. Deploy a cluster from the UI.

The JMX exporter artifact is auto-selected from `kf_artifact` where:

```text
service_type = JMX_EXPORTER
status = AVAILABLE
```

You do not need to manually put the JMX artifact ID in `.env` unless you want to force a specific artifact.

## 8. Monitoring Setup

Current monitoring flow:

1. Kafka is deployed by Tantor agent.
2. The agent attaches JMX exporter to Kafka on port `7071`.
3. Tantor server starts one `kafka_exporter` systemd service per internal cluster on the Tantor server host.
4. Tantor server exposes scrape targets at:

```text
/internal/prometheus/targets
```

5. Prometheus scrapes those targets.
6. UI calls Tantor monitoring APIs.
7. Tantor server queries Prometheus and returns metrics to the UI.

The service-discovery endpoint accepts only loopback clients. Keep Prometheus on
the Tantor server host; remote and proxied requests to `/internal/prometheus/**`
are denied even when they carry a user JWT.

Required monitoring components:

- `kafka_exporter` installed on the Tantor server host at `/usr/local/bin/kafka_exporter`
- Prometheus running and configured to scrape Tantor service discovery endpoint
- Target Kafka hosts must allow access to JMX exporter port `7071`

Example Prometheus scrape config:

```yaml
scrape_configs:
  - job_name: tantor-sd
    http_sd_configs:
      - url: http://127.0.0.1:8443/internal/prometheus/targets
        refresh_interval: 15s
```

For a local demo Prometheus on the Tantor server host:

```properties
TANTOR_MONITORING_MODE=direct
TANTOR_PROMETHEUS_URL=http://127.0.0.1:9090
```

## 9. VM Deployment Notes

Typical backend deploy on a Linux VM:

```bash
cd /opt/Tantor_kafka
git pull

cd /opt/Tantor_kafka/tantor-server
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH=$JAVA_HOME/bin:$PATH
mvn clean package -DskipTests
sudo systemctl restart tantor-server
sudo systemctl status tantor-server --no-pager
```

Typical UI deploy:

```bash
cd /opt/Tantor_kafka/tantor-ui
npm install
npm run build
sudo rm -rf /usr/share/nginx/html/*
sudo cp -r /opt/Tantor_kafka/tantor-ui/dist/* /usr/share/nginx/html/
sudo systemctl reload nginx
```

Typical artifact repository deploy:

```bash
cd /opt/Tantor_kafka/tantor-artifact-repository
export JAVA_HOME=/usr/lib/jvm/java-21-openjdk
export PATH=$JAVA_HOME/bin:$PATH
mvn clean package -DskipTests
sudo systemctl restart tantor-artifact
sudo systemctl status tantor-artifact --no-pager
```

## Troubleshooting

### `JAR not found`

Run:

```powershell
.\build.ps1
```

Then start backend again.

### UI returns `502 Bad Gateway`

The backend is not running or Nginx cannot reach it. Check:

```bash
sudo systemctl status tantor-server --no-pager
sudo journalctl -u tantor-server -n 100 --no-pager
```

### Artifact upload fails

Check repository path permissions:

```bash
sudo mkdir -p /var/lib/tantor/repository/artifacts
sudo chown -R root:root /var/lib/tantor/repository
sudo chmod -R 775 /var/lib/tantor/repository
sudo systemctl restart tantor-artifact
```

Also verify:

```bash
curl -i http://127.0.0.1:8081/api/v1/artifacts
```

### Monitoring shows `kafka_exporter required`

Check kafka_exporter and Prometheus:

```bash
systemctl status tantor-kafka-exporter-<cluster-id> --no-pager
curl http://127.0.0.1:<exporter-port>/metrics | head
curl http://127.0.0.1:9090/api/v1/targets
```

### Monitoring shows `JMX required`

On the Kafka node:

```bash
ps -ef | grep jmx_prometheus_javaagent | grep -v grep
curl http://127.0.0.1:7071/metrics | head
```

From the Tantor server:

```bash
curl http://<kafka-node-ip>:7071/metrics | head
```

### Database starts empty

That is normal for a fresh clone. Start `tantor-server`; Flyway will create the schema. Then upload artifacts and register hosts from the UI.
