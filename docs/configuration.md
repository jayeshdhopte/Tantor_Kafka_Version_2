# Runtime configuration

Environment-specific values are owned by deployment, bound into typed
component configuration, validated before work begins, and reported through
non-secret startup diagnostics. Production-like environments never inherit the
`dev` profile.

| Value | Deployment input | Consumer | Required outside development |
|---|---|---|---|
| Environment/profile | `SPRING_PROFILES_ACTIVE`, agent `environment` | Spring and Go validators | Yes |
| Database endpoint | `TANTOR_DB_URL` | Spring datasource | Yes |
| Internal Artifact endpoint | `TANTOR_REPO_INTERNAL_URL` | `ArtifactRepositoryProperties.internalUrl` | Yes |
| Public Artifact/agent endpoint | `TANTOR_REPO_PUBLIC_URL` | `ArtifactRepositoryProperties.publicUrl` | Yes; public HTTPS authority |
| Public control-plane origin | `TANTOR_PUBLIC_ORIGIN` | `ControlPlaneProperties` and generated UI config | Yes; same authority as public Artifact URL |
| Browser origins | `TANTOR_CORS_ALLOWED_ORIGINS` | `CorsProperties` | Yes; HTTPS in production |
| OIDC issuer/audience | `TANTOR_OIDC_ISSUER_URI`, `TANTOR_OIDC_AUDIENCE` | `OidcProperties` | Yes; HTTPS in production |
| Monitoring mode/endpoints | `TANTOR_MONITORING_MODE`, explicit Prometheus or Grafana values | `MonitoringProperties` | Yes |
| Malware scanner | `TANTOR_MALWARE_SCAN_*` | `MalwareScanProperties` | Host/port required when enabled |
| UI identity provider | generated `ui-runtime-config.js` | UI runtime validator | Yes |
| UI proxy/CSP | generated `nginx.conf` | production Nginx | Yes |
| Agent control plane | YAML `server_url` | Go configuration structs | Yes; HTTPS/mTLS |

Supported Spring profiles are `dev`, `sit`, `uat`, and `production`. Only
`application-dev.yml` contains loopback defaults. SIT, UAT, and production
values must be supplied by their deployment environment. Missing placeholders,
malformed URLs, invalid ports, insecure OIDC, production loopback endpoints,
and a mismatched profile/environment stop startup.

Production UI configuration is generated during release packaging rather than
compiled into React. API and artifact paths are same-origin paths routed by
Nginx and consumed by a single runtime fetch-routing layer. The packaging
command requires the public origin, OIDC issuer/audience, Keycloak origin/realm,
and an explicit monitoring mode and endpoint. It verifies UI/backend identity,
CORS, runtime/Nginx routes, and CSP before writing checksummed runtime files.

Startup diagnostics contain environment/profile, endpoint schemes and hosts,
ports, feature modes, and boolean “configured” indicators. They never contain
passwords, tokens, private keys, authorization headers, database users, or
secret-file contents.

Secrets remain supplied by Podman secret/config-tree files. Do not move them
into these non-secret runtime settings.

## Hardcoding inventory and classification

The repository inventory classifies the discovered values as follows:

| Classification | Findings and disposition |
|---|---|
| Application configuration | Database, Artifact Repository, OIDC, CORS, monitoring, Kafka deployment, control-plane, discovery, and browser routes now flow from deployment/task input into typed validated configuration. |
| Safe immutable product constants | Internal health/listener ports, local exporter/JMX bindings, standard filesystem layout, protocol keys, and API route names remain in source where co-located operation or the packaged runtime contract requires them. |
| Test fixtures | Loopback hosts, documentation-domain hosts, RFC 1918 addresses, and fixed ports under test sources remain isolated test data. |
| Development examples | Loopback database, proxy, CORS, and monitoring values remain only in `application-dev.yml`, Vite development configuration, sample agent files, or explicitly development-scoped examples. |
| Deployment configuration | Compose service DNS names, container paths, published edge ports, profile selection, generated UI configuration, and secret file mounts are owned by deployment files. |
| Accidental environment hardcoding | Developer IPs, compiled Keycloak/Artifact URLs, production-like monitoring defaults, a fixed producer broker/topic, a VM target default, and Kafka Connect/Schema Registry/ksqlDB dependency fallbacks were removed or made required runtime inputs. |

Kafka bootstrap servers for managed/external clusters are cluster/task data,
not a single server-startup endpoint. They are validated and passed to agents
with the deployment task. Localhost references retained for JMX/Prometheus
scraping are process-local bindings and are not remote environment endpoints.
