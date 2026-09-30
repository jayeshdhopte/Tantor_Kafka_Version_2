#!/usr/bin/env bash
set -Eeuo pipefail

readonly POSTGRES_13_IMAGE='docker.io/library/postgres:13.23@sha256:4689940c683801b4ab839ab3b0a0a3555a5fe425371422310944e89eca7d8068'
readonly POSTGRES_16_IMAGE='docker.io/library/postgres:16.14@sha256:95206741a5b214807675e14165369d05b93a9cf692223b616d07cca227e74b0b'
readonly EXPECTED_MIGRATION_COUNT=74

repository_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
run_suffix="${GITHUB_RUN_ID:-local}-$$-${RANDOM}"
source_container="tantor-h02-pg13-source-${run_suffix}"
target_container="tantor-h02-pg16-target-${run_suffix}"
network_name="tantor-h02-restore-net-${run_suffix}"
work_directory="$(mktemp -d "${TMPDIR:-/tmp}/tantor-h02-restore.XXXXXX")"
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
  for process_id in "$artifact_pid" "$server_pid"; do
    if [[ -n "$process_id" ]] && kill -0 "$process_id" 2>/dev/null; then
      kill "$process_id" 2>/dev/null || true
      wait "$process_id" 2>/dev/null || true
    fi
  done
  if (( exit_code != 0 )); then
    for log_file in "$work_directory"/*.log; do
      [[ -f "$log_file" ]] && { echo "--- $log_file"; tail -n 200 "$log_file"; }
    done
    docker logs "$source_container" 2>/dev/null || true
    docker logs "$target_container" 2>/dev/null || true
  fi
  docker rm --force "$source_container" "$target_container" >/dev/null 2>&1 || true
  docker network rm "$network_name" >/dev/null 2>&1 || true
  case "$work_directory" in
    "${TMPDIR:-/tmp}"/tantor-h02-restore.*) rm -rf -- "$work_directory" ;;
    *) echo "Refusing to remove unexpected temporary path: $work_directory" >&2 ;;
  esac
  exit "$exit_code"
}
trap cleanup EXIT

wait_for_database() {
  container_name=$1
  for _ in $(seq 1 90); do
    docker exec "$container_name" pg_isready -U tantor_test -d tantor >/dev/null 2>&1 && return 0
    sleep 1
  done
  return 1
}

wait_for_http() {
  url=$1
  process_id=$2
  log_file=$3
  for _ in $(seq 1 180); do
    curl --fail --silent --show-error "$url" >/dev/null 2>&1 && return 0
    if ! kill -0 "$process_id" 2>/dev/null; then
      tail -n 200 "$log_file" >&2 || true
      return 1
    fi
    sleep 1
  done
  tail -n 200 "$log_file" >&2 || true
  return 1
}

stop_process() {
  process_id=$1
  if [[ -n "$process_id" ]] && kill -0 "$process_id" 2>/dev/null; then
    kill "$process_id"
    wait "$process_id" 2>/dev/null || true
  fi
}

start_server() {
  database_port=$1
  log_file=$2
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

psql_in() {
  container_name=$1
  shift
  docker exec -e PGPASSWORD="$database_password" "$container_name" psql -U tantor_test -d tantor "$@"
}

for command_name in docker curl openssl "$maven_command" "$java_command"; do
  command -v "$command_name" >/dev/null 2>&1 || { echo "Required command is unavailable: $command_name" >&2; exit 1; }
done
docker info >/dev/null
docker network create "$network_name" >/dev/null

docker run --detach --name "$source_container" --network "$network_name" --network-alias pg13-source \
  --publish 127.0.0.1::5432 \
  --env POSTGRES_DB=tantor --env POSTGRES_USER=tantor_test --env POSTGRES_PASSWORD="$database_password" \
  "$POSTGRES_13_IMAGE" >/dev/null
wait_for_database "$source_container"
source_port="$(docker port "$source_container" 5432/tcp | awk -F: 'NR == 1 { print $NF }')"

(cd "$repository_root/tantor-server" && "$maven_command" -B -Dmaven.test.skip=true package)
(cd "$repository_root/tantor-artifact-repository" && "$maven_command" -B -Dmaven.test.skip=true package)

start_server "$source_port" "$work_directory/source-server.log"
wait_for_http 'http://127.0.0.1:8443/api/v1/monitoring/health' "$server_pid" "$work_directory/source-server.log"
source_migration_count="$(psql_in "$source_container" -Atc 'SELECT count(*) FROM flyway_schema_history WHERE success')"
[[ "$source_migration_count" == "$EXPECTED_MIGRATION_COUNT" ]]
stop_process "$server_pid"
server_pid=''

psql_in "$source_container" -v ON_ERROR_STOP=1 -c \
  "CREATE TABLE h02_upgrade_probe (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), sequence_no BIGINT GENERATED ALWAYS AS IDENTITY, payload JSONB NOT NULL); INSERT INTO h02_upgrade_probe(payload) VALUES ('{\"case\":\"postgresql-13-to-16\",\"valid\":true}'::jsonb);" >/dev/null

source_tables="$(psql_in "$source_container" -Atc "SELECT count(*) FROM pg_tables WHERE schemaname='public'")"
source_functions="$(psql_in "$source_container" -Atc "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public'")"
source_triggers="$(psql_in "$source_container" -Atc "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal")"

docker run --rm --network "$network_name" -e PGPASSWORD="$database_password" \
  -v "$work_directory:/work" "$POSTGRES_16_IMAGE" \
  pg_dump -h pg13-source -U tantor_test -d tantor --format=custom --quote-all-identifiers --file=/work/tantor.dump
docker run --rm -v "$work_directory:/work" "$POSTGRES_16_IMAGE" pg_restore --list /work/tantor.dump >"$work_directory/tantor.dump.list"
test -s "$work_directory/tantor.dump"
test -s "$work_directory/tantor.dump.list"
sha256sum "$work_directory/tantor.dump" >"$work_directory/tantor.dump.sha256"

docker run --detach --name "$target_container" --network "$network_name" --network-alias pg16-target \
  --publish 127.0.0.1::5432 \
  --env POSTGRES_DB=tantor --env POSTGRES_USER=tantor_test --env POSTGRES_PASSWORD="$database_password" \
  "$POSTGRES_16_IMAGE" >/dev/null
wait_for_database "$target_container"
target_port="$(docker port "$target_container" 5432/tcp | awk -F: 'NR == 1 { print $NF }')"

docker run --rm --network "$network_name" -e PGPASSWORD="$database_password" \
  -v "$work_directory:/work" "$POSTGRES_16_IMAGE" \
  pg_restore -h pg16-target -U tantor_test -d tantor --exit-on-error --no-owner --no-privileges /work/tantor.dump

target_tables="$(psql_in "$target_container" -Atc "SELECT count(*) FROM pg_tables WHERE schemaname='public'")"
target_functions="$(psql_in "$target_container" -Atc "SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='public'")"
target_triggers="$(psql_in "$target_container" -Atc "SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='public' AND NOT t.tgisinternal")"
[[ "$source_tables" == "$target_tables" ]]
[[ "$source_functions" == "$target_functions" ]]
[[ "$source_triggers" == "$target_triggers" ]]
[[ "$(psql_in "$target_container" -Atc "SELECT count(*) FROM pg_extension WHERE extname='pgcrypto'")" == '1' ]]
[[ "$(psql_in "$target_container" -Atc "SELECT count(*) FROM h02_upgrade_probe WHERE payload->>'case'='postgresql-13-to-16' AND (payload->>'valid')::boolean")" == '1' ]]
[[ "$(psql_in "$target_container" -Atc 'SELECT count(*) FROM flyway_schema_history WHERE success')" == "$EXPECTED_MIGRATION_COUNT" ]]

# Startup on the restored database performs strict Flyway validation with no repair/baseline workaround.
start_server "$target_port" "$work_directory/target-server.log"
wait_for_http 'http://127.0.0.1:8443/api/v1/monitoring/health' "$server_pid" "$work_directory/target-server.log"

TANTOR_DB_URL="jdbc:postgresql://127.0.0.1:${target_port}/tantor" \
TANTOR_DB_USER=tantor_test TANTOR_DB_PASSWORD="$database_password" \
TANTOR_REPO_PATH="$work_directory/repository" TANTOR_MINIMUM_FREE_SPACE_BYTES=1 \
"$java_command" -jar "$repository_root/tantor-artifact-repository/target/tantor-artifact-repository-1.0.0.jar" \
  >"$work_directory/artifact.log" 2>&1 &
artifact_pid=$!
wait_for_http 'http://127.0.0.1:8081/actuator/health/readiness' "$artifact_pid" "$work_directory/artifact.log"

curl --fail --silent --show-error 'http://127.0.0.1:8443/api/v1/monitoring/health' >/dev/null
curl --fail --silent --show-error 'http://127.0.0.1:8081/actuator/health/readiness' >/dev/null
echo 'PostgreSQL 13.23 to 16.14 dump/restore, verification, Flyway validation, and application smoke tests passed.'
