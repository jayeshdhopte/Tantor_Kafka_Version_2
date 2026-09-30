#!/usr/bin/env bash
set -Eeuo pipefail

readonly POSTGRES_16_IMAGE='docker.io/library/postgres:16.14@sha256:95206741a5b214807675e14165369d05b93a9cf692223b616d07cca227e74b0b'
readonly EXPECTED_MIGRATION_COUNT=74
readonly EXPECTED_HIGHEST_VERSION='76'

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_suffix="${GITHUB_RUN_ID:-local}-$$-${RANDOM}"
database_container="tantor-h02-pg16-${run_suffix}"
network_name="tantor-h02-pg16-net-${run_suffix}"
work_directory="$(mktemp -d "${TMPDIR:-/tmp}/tantor-h02-fresh.XXXXXX")"
server_pid=''
artifact_pid=''
database_password="$(openssl rand -hex 24)"
encryption_key="$(openssl rand -hex 16)"
jwt_secret="$(openssl rand -base64 48 | tr -d '\r\n')"
keystore_password="$(openssl rand -hex 16)"
maven_command="${MAVEN_COMMAND:-mvn}"
java_command="${JAVA_COMMAND:-java}"

cleanup() {
  exit_code=$?
  if [[ -n "$artifact_pid" ]] && kill -0 "$artifact_pid" 2>/dev/null; then
    kill "$artifact_pid" 2>/dev/null || true
    wait "$artifact_pid" 2>/dev/null || true
  fi
  if [[ -n "$server_pid" ]] && kill -0 "$server_pid" 2>/dev/null; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  if (( exit_code != 0 )); then
    [[ -f "$work_directory/server.log" ]] && { echo '--- tantor-server.log'; tail -n 200 "$work_directory/server.log"; }
    [[ -f "$work_directory/artifact.log" ]] && { echo '--- artifact-repository.log'; tail -n 200 "$work_directory/artifact.log"; }
    docker logs "$database_container" 2>/dev/null || true
  fi
  docker rm --force "$database_container" >/dev/null 2>&1 || true
  docker network rm "$network_name" >/dev/null 2>&1 || true
  case "$work_directory" in
    "${TMPDIR:-/tmp}"/tantor-h02-fresh.*) rm -rf -- "$work_directory" ;;
    *) echo "Refusing to remove unexpected temporary path: $work_directory" >&2 ;;
  esac
  exit "$exit_code"
}
trap cleanup EXIT

require_command() {
  command -v "$1" >/dev/null 2>&1 || { echo "Required command is unavailable: $1" >&2; exit 1; }
}

wait_for_http() {
  url=$1
  process_id=$2
  log_file=$3
  for _ in $(seq 1 180); do
    if curl --fail --silent --show-error "$url" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$process_id" 2>/dev/null; then
      echo "Process exited before becoming ready: $url" >&2
      tail -n 200 "$log_file" >&2 || true
      return 1
    fi
    sleep 1
  done
  echo "Timed out waiting for $url" >&2
  tail -n 200 "$log_file" >&2 || true
  return 1
}

stop_process() {
  process_id=$1
  if kill -0 "$process_id" 2>/dev/null; then
    kill "$process_id"
    wait "$process_id" 2>/dev/null || true
  fi
}

start_server() {
  log_file=$1
  TANTOR_DB_URL="jdbc:postgresql://127.0.0.1:${database_port}/tantor" \
  TANTOR_DB_USER=tantor_test \
  TANTOR_DB_PASSWORD="$database_password" \
  TANTOR_ENCRYPTION_KEY="$encryption_key" \
  TANTOR_JWT_SECRET="$jwt_secret" \
  TANTOR_KEYSTORE_PASSWORD="$keystore_password" \
  TANTOR_REPO_URL=http://127.0.0.1:8081 \
  TANTOR_MONITORING_MODE=direct \
  TANTOR_PROMETHEUS_URL=http://127.0.0.1:19090 \
  "$java_command" -jar "$repository_root/tantor-server/target/tantor-server-1.0.0.jar" >"$log_file" 2>&1 &
  server_pid=$!
}

verify_migration_history() {
  actual_count="$(docker exec -e PGPASSWORD="$database_password" "$database_container" \
    psql -U tantor_test -d tantor -Atc 'SELECT count(*) FROM flyway_schema_history WHERE success')"
  [[ "$actual_count" == "$EXPECTED_MIGRATION_COUNT" ]] || {
    echo "Expected $EXPECTED_MIGRATION_COUNT successful Flyway migrations, found $actual_count" >&2
    return 1
  }

  highest_version="$(docker exec -e PGPASSWORD="$database_password" "$database_container" \
    psql -U tantor_test -d tantor -Atc 'SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank DESC LIMIT 1')"
  [[ "$highest_version" == "$EXPECTED_HIGHEST_VERSION" ]] || {
    echo "Expected highest Flyway version $EXPECTED_HIGHEST_VERSION, found $highest_version" >&2
    return 1
  }

  expected_versions="$(find "$repository_root/tantor-server/src/main/resources/db/migration" -maxdepth 1 -type f -name 'V*__*.sql' \
    -printf '%f\n' | sed -E 's/^V([0-9]+(_[0-9]+)?)__.*/\1/; s/_/./g' | sort -V)"
  actual_versions="$(docker exec -e PGPASSWORD="$database_password" "$database_container" \
    psql -U tantor_test -d tantor -Atc 'SELECT version FROM flyway_schema_history WHERE success ORDER BY installed_rank')"
  diff -u <(printf '%s\n' "$expected_versions") <(printf '%s\n' "$actual_versions")
}

require_command docker
require_command curl
require_command openssl
require_command "$maven_command"
require_command "$java_command"

docker info >/dev/null
docker buildx imagetools inspect "$POSTGRES_16_IMAGE" >/dev/null
docker network create "$network_name" >/dev/null
docker run --detach --name "$database_container" --network "$network_name" \
  --publish 127.0.0.1::5432 \
  --env POSTGRES_DB=tantor \
  --env POSTGRES_USER=tantor_test \
  --env POSTGRES_PASSWORD="$database_password" \
  "$POSTGRES_16_IMAGE" >/dev/null

for _ in $(seq 1 90); do
  docker exec "$database_container" pg_isready -U tantor_test -d tantor >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$database_container" pg_isready -U tantor_test -d tantor >/dev/null
database_port="$(docker port "$database_container" 5432/tcp | awk -F: 'NR == 1 { print $NF }')"
[[ "$database_port" =~ ^[0-9]+$ ]]

(cd "$repository_root/tantor-server" && "$maven_command" -B -Dmaven.test.skip=true package)
(cd "$repository_root/tantor-artifact-repository" && "$maven_command" -B -Dmaven.test.skip=true package)

start_server "$work_directory/server.log"
wait_for_http 'http://127.0.0.1:8443/api/v1/monitoring/health' "$server_pid" "$work_directory/server.log"
verify_migration_history

extension_count="$(docker exec -e PGPASSWORD="$database_password" "$database_container" \
  psql -U tantor_test -d tantor -Atc "SELECT count(*) FROM pg_extension WHERE extname = 'pgcrypto'")"
[[ "$extension_count" == '1' ]] || { echo 'pgcrypto was not installed' >&2; exit 1; }

# A second startup against the completed schema exercises strict Flyway validation.
stop_process "$server_pid"
server_pid=''
start_server "$work_directory/server-validation.log"
wait_for_http 'http://127.0.0.1:8443/api/v1/monitoring/health' "$server_pid" "$work_directory/server-validation.log"
verify_migration_history

TANTOR_DB_URL="jdbc:postgresql://127.0.0.1:${database_port}/tantor" \
TANTOR_DB_USER=tantor_test \
TANTOR_DB_PASSWORD="$database_password" \
TANTOR_REPO_PATH="$work_directory/repository" \
TANTOR_MINIMUM_FREE_SPACE_BYTES=1 \
"$java_command" -jar "$repository_root/tantor-artifact-repository/target/tantor-artifact-repository-1.0.0.jar" \
  >"$work_directory/artifact.log" 2>&1 &
artifact_pid=$!
wait_for_http 'http://127.0.0.1:8081/actuator/health/readiness' "$artifact_pid" "$work_directory/artifact.log"

curl --fail --silent --show-error 'http://127.0.0.1:8443/api/v1/monitoring/health' >/dev/null
curl --fail --silent --show-error 'http://127.0.0.1:8081/actuator/health/readiness' >/dev/null
echo 'PostgreSQL 16.14 fresh migration, Flyway validation, application startup, and smoke tests passed.'
