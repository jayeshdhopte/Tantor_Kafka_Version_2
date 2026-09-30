#!/usr/bin/env bash
set -Eeuo pipefail

readonly POSTGRES_IMAGE='docker.io/library/postgres:16.14@sha256:95206741a5b214807675e14165369d05b93a9cf692223b616d07cca227e74b0b'
repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
development_compose="$repository_root/podman-compose.yml"
production_compose="$repository_root/podman-compose.production.yml"
run_suffix="${GITHUB_RUN_ID:-local}-$$-${RANDOM}"
development_project="tantor-h01-dev-$run_suffix"
production_project="tantor-h01-prod-$run_suffix"
work_directory="$(mktemp -d "${TMPDIR:-/tmp}/tantor-h01.XXXXXX")"
development_environment="$work_directory/development.env"
production_environment="$work_directory/production.env"
secrets_directory="$work_directory/secrets"
ui_runtime_config="$work_directory/ui-runtime-config.js"
ui_nginx_config="$work_directory/nginx.conf"
database_password="$(od -An -N24 -tx1 /dev/urandom | tr -d ' \n')"

if command -v podman-compose >/dev/null 2>&1; then
  compose=(podman-compose)
elif command -v podman >/dev/null 2>&1 && podman compose version >/dev/null 2>&1; then
  compose=(podman compose)
else
  echo 'Podman with a Compose provider is required.' >&2
  exit 1
fi

development_compose_run() {
  COMPOSE_PROJECT_NAME="$development_project" "${compose[@]}" \
    --env-file "$development_environment" --file "$development_compose" "$@"
}

production_compose_run() {
  COMPOSE_PROJECT_NAME="$production_project" "${compose[@]}" \
    --env-file "$production_environment" --file "$production_compose" "$@"
}

cleanup() {
  exit_code=$?
  development_compose_run down --volumes --remove-orphans >/dev/null 2>&1 || true
  if [[ -f "$production_environment" ]]; then
    production_compose_run down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  case "$work_directory" in
    "${TMPDIR:-/tmp}"/tantor-h01.*) rm -rf -- "$work_directory" ;;
    *) echo "Refusing to remove unexpected test directory: $work_directory" >&2 ;;
  esac
  exit "$exit_code"
}
trap cleanup EXIT

service_id() {
  local environment="$1"
  local service="$2"
  if [[ "$environment" == development ]]; then
    development_compose_run ps --quiet "$service"
  else
    production_compose_run ps --quiet "$service"
  fi
}

wait_for_health() {
  local environment="$1"
  local service="$2"
  local attempts="${3:-60}"
  local container_id status

  container_id="$(service_id "$environment" "$service")"
  [[ -n "$container_id" ]] || { echo "No $environment container for $service" >&2; return 1; }
  for ((attempt=1; attempt<=attempts; attempt++)); do
    status="$(podman inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container_id")"
    case "$status" in
      healthy) return 0 ;;
      exited|dead|unhealthy)
        echo "$environment/$service entered terminal state: $status" >&2
        return 1
        ;;
    esac
    sleep 2
  done
  echo "$environment/$service did not become healthy" >&2
  return 1
}

wait_for_bounded_failure() {
  local container_id="$1"
  local attempts="${2:-60}"
  local status restart_count health_status

  for ((attempt=1; attempt<=attempts; attempt++)); do
    status="$(podman inspect --format '{{.State.Status}}' "$container_id")"
    restart_count="$(podman inspect --format '{{.RestartCount}}' "$container_id")"
    health_status="$(podman inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{end}}' "$container_id")"
    [[ "$health_status" != healthy ]] || {
      echo 'Artifact Repository became ready while PostgreSQL was unavailable.' >&2
      return 1
    }
    if [[ "$status" == exited && "$restart_count" == 5 ]]; then
      return 0
    fi
    sleep 1
  done
  echo "Artifact failure was not bounded at five restarts (state=$status restarts=$restart_count)." >&2
  return 1
}

readiness_status_line() {
  local container_id="$1"
  podman exec "$container_id" bash -ec \
    'exec 3<>/dev/tcp/127.0.0.1/8081; printf "GET /actuator/health/readiness HTTP/1.0\r\nHost: localhost\r\n\r\n" >&3; head -n 1 <&3'
}

wait_for_schema_unready() {
  local container_id="$1"
  local status_line
  for _ in $(seq 1 60); do
    if status_line="$(readiness_status_line "$container_id" 2>/dev/null)"; then
      case "$status_line" in
        *' 503 '*) return 0 ;;
        *' 200 '*)
          echo 'Artifact Repository reported ready before the schema existed.' >&2
          return 1
          ;;
      esac
    fi
    [[ "$(podman inspect --format '{{.State.Status}}' "$container_id")" == running ]] || {
      echo 'Artifact Repository exited before the schema-readiness assertion.' >&2
      return 1
    }
    sleep 1
  done
  echo 'Artifact Repository never exposed an unready schema state.' >&2
  return 1
}

mkdir -p "$secrets_directory"
chmod 700 "$secrets_directory"
test_encryption_key='0123456789abcdef0123456789abcdef' # gitleaks:allow -- generated test environment only
cat >"$development_environment" <<EOF
TANTOR_DB_USER=tantor_h01
TANTOR_DB_PASSWORD=$database_password
TANTOR_ENCRYPTION_KEY=$test_encryption_key
TANTOR_JWT_SECRET=tantor-h01-validation-only-jwt-secret-not-for-production
TANTOR_KEYSTORE_PASSWORD=tantor-h01-validation-only
TANTOR_DB_CONNECTION_TIMEOUT_MS=500
TANTOR_DB_INITIALIZATION_FAIL_TIMEOUT_MS=2000
EOF
chmod 600 "$development_environment"

echo 'Validating development Compose and building application images...'
development_compose_run config >/dev/null
development_compose_run build tantor-server tantor-artifact-repository

echo 'Proving bounded failure while PostgreSQL is unavailable...'
development_compose_run up --detach --no-deps tantor-artifact-repository
failed_artifact_id="$(service_id development tantor-artifact-repository)"
wait_for_bounded_failure "$failed_artifact_id"
podman rm --force "$failed_artifact_id" >/dev/null

echo 'Starting PostgreSQL without the schema migrator...'
development_compose_run up --detach --no-deps database
wait_for_health development database
development_compose_run up --detach --no-deps tantor-artifact-repository
artifact_id="$(service_id development tantor-artifact-repository)"
wait_for_schema_unready "$artifact_id"

schema_before_migration="$(development_compose_run exec -T database psql -U tantor_h01 -d tantor -Atc \
  "SELECT to_regclass('public.kf_artifact') IS NULL;")"
[[ "$schema_before_migration" == t ]] || { echo 'Artifact schema existed before the migrator ran.' >&2; exit 1; }

echo 'Running the sole Flyway owner and waiting for Artifact readiness...'
development_compose_run up --detach --no-deps tantor-server
wait_for_health development tantor-server
wait_for_health development tantor-artifact-repository

database_and_owner="$(development_compose_run exec -T database psql -U tantor_h01 -d tantor -Atc \
  "SELECT current_database() || ':' || current_user || ':' || count(*) FROM pg_tables WHERE schemaname = 'public' AND tablename IN ('kf_artifact', 'flyway_schema_history') AND tableowner = current_user;")"
[[ "$database_and_owner" == 'tantor:tantor_h01:2' ]] || {
  echo "Unexpected development schema ownership result: $database_and_owner" >&2
  exit 1
}

server_image="$(podman inspect --format '{{.Image}}' "$(service_id development tantor-server)")"
artifact_image="$(podman inspect --format '{{.Image}}' "$(service_id development tantor-artifact-repository)")"
development_compose_run down --volumes --remove-orphans

printf '%s\n' 'tantor_h01_prod' >"$secrets_directory/TANTOR_DB_USER"
printf '%s\n' "$database_password" >"$secrets_directory/TANTOR_DB_PASSWORD"
printf '%s\n' '0123456789abcdef0123456789abcdef' >"$secrets_directory/TANTOR_ENCRYPTION_KEY"
printf '%s\n' 'tantor-h01-production-path-test-only-jwt-secret' >"$secrets_directory/TANTOR_JWT_SECRET"
printf '%s\n' 'test-certificate-not-used' >"$secrets_directory/tls.crt"
printf '%s\n' 'test-private-key-not-used' >"$secrets_directory/tls.key"
chmod 444 "$secrets_directory"/*

cat >"$ui_runtime_config" <<'EOF'
window.__TANTOR_CONFIG__ = Object.freeze({
  environment: 'production',
  publicOrigin: 'https://tantor.h01.internal',
  authEnabled: true,
  keycloakUrl: 'https://keycloak.h01.internal',
  keycloakRealm: 'tantor',
  keycloakClientId: 'tantor-ui',
  apiBasePath: '/api',
  artifactApiBasePath: '/api/v1/artifacts'
});
EOF
printf '%s\n' 'server { listen 8080; location / { return 204; } }' >"$ui_nginx_config"
chmod 444 "$ui_runtime_config" "$ui_nginx_config"

cat >"$production_environment" <<EOF
POSTGRES_IMAGE=$POSTGRES_IMAGE
TANTOR_SERVER_IMAGE=$server_image
TANTOR_ARTIFACT_REPOSITORY_IMAGE=$artifact_image
TANTOR_UI_IMAGE=$artifact_image
TANTOR_SECRETS_DIR=$secrets_directory
TANTOR_UI_RUNTIME_CONFIG_FILE=$ui_runtime_config
TANTOR_UI_NGINX_CONFIG_FILE=$ui_nginx_config
TANTOR_CORS_ALLOWED_ORIGINS=https://tantor.h01.internal
TANTOR_OIDC_ISSUER_URI=https://keycloak.h01.internal/realms/tantor
TANTOR_OIDC_AUDIENCE=tantor-ui
TANTOR_PUBLIC_ORIGIN=https://tantor.h01.internal
TANTOR_REPO_PUBLIC_URL=https://tantor.h01.internal
TANTOR_MONITORING_MODE=direct
TANTOR_PROMETHEUS_URL=http://prometheus:9090
TANTOR_KAFKA_SECURITY_MODE=PLAINTEXT
TANTOR_DB_CONNECTION_TIMEOUT_MS=500
TANTOR_DB_INITIALIZATION_FAIL_TIMEOUT_MS=2000
EOF
chmod 600 "$production_environment"

echo 'Validating the production secret/config-tree deployment path...'
production_compose_run config >/dev/null
production_compose_run up --detach --no-deps database
wait_for_health production database
database_id="$(service_id production database)"
podman exec "$database_id" test -s /run/secrets/TANTOR_DB_USER
podman exec "$database_id" test -s /run/secrets/TANTOR_DB_PASSWORD

production_compose_run up --detach --no-deps tantor-server
wait_for_health production tantor-server
production_compose_run up --detach --no-deps tantor-artifact-repository
wait_for_health production tantor-artifact-repository
artifact_id="$(service_id production tantor-artifact-repository)"
podman exec "$artifact_id" test -s /run/secrets/TANTOR_DB_USER
podman exec "$artifact_id" test -s /run/secrets/TANTOR_DB_PASSWORD

artifact_environment="$(podman inspect --format '{{range .Config.Env}}{{println .}}{{end}}' "$artifact_id")"
grep -Fxq 'TANTOR_DB_URL=jdbc:postgresql://database:5432/tantor' <<<"$artifact_environment"
if grep -Eq '^TANTOR_DB_(USER|PASSWORD)=' <<<"$artifact_environment"; then
  echo 'Production database credentials leaked into the Artifact Repository environment.' >&2
  exit 1
fi

production_identity="$(production_compose_run exec -T database sh -ec \
  'psql -U "$(cat /run/secrets/TANTOR_DB_USER)" -d tantor -Atc "SELECT current_user || chr(58) || current_database();"')"
[[ "$production_identity" == 'tantor_h01_prod:tantor' ]] || {
  echo "Production config-tree identity mismatch: $production_identity" >&2
  exit 1
}

migration_count_before="$(production_compose_run exec -T database sh -ec \
  'psql -U "$(cat /run/secrets/TANTOR_DB_USER)" -d tantor -Atc "SELECT count(*) FROM flyway_schema_history WHERE success;"')"

echo 'Restarting production services and proving persistence...'
production_compose_run restart database
wait_for_health production database
production_compose_run restart tantor-server
wait_for_health production tantor-server
production_compose_run restart tantor-artifact-repository
wait_for_health production tantor-artifact-repository

restart_result="$(production_compose_run exec -T database sh -ec \
  'psql -U "$(cat /run/secrets/TANTOR_DB_USER)" -d tantor -Atc "SELECT to_regclass('"'"'public.kf_artifact'"'"') || chr(58) || (SELECT count(*) FROM flyway_schema_history WHERE success);"')"
[[ "$restart_result" == "kf_artifact:$migration_count_before" ]] || {
  echo "Schema changed or disappeared after restart: $restart_result" >&2
  exit 1
}

if production_compose_run logs --no-color tantor-artifact-repository | grep -Fq 'jdbc:postgresql://localhost'; then
  echo 'Artifact Repository attempted an unintended localhost database connection.' >&2
  exit 1
fi

echo 'H-01 clean-environment deployment validation passed.'
