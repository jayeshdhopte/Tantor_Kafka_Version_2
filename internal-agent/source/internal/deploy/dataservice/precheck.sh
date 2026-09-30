#!/usr/bin/env bash
# Embedded, fixed program. Task values are positional arguments, never shell code.
# Implements deploy/prechecks/*_precheck_plaintext.sh with bounded commands,
# explicit deferred checks during new-cluster Preview, and no shared /tmp files.
set -u
kind=$1; port=$2; bootstrap=$3; kafka_bin=$4; plugin=$5; working=$6
min_disk=$7; deferred=$8; ha_peers=$9; shift 9
topics=("$@")
failed=0
pass() { printf '[PASS] %s\n' "$*"; }
fail() { printf '[FAIL] %s\n' "$*"; failed=$((failed+1)); }
warn() { printf '[WARN] %s\n' "$*"; }
defer_check() { printf '[DEFERRED] %s\n' "$*"; }
printf '===== %s precheck (PLAINTEXT) =====\n' "$kind"
soft=$(ulimit -Sn); hard=$(ulimit -Hn)
limit_ok() { [[ "$1" == unlimited ]] || { [[ "$1" =~ ^[0-9]+$ ]] && (( "$1" >= 1024000 )); }; }
if limit_ok "$soft" && limit_ok "$hard"; then pass "Open file limits $soft/$hard"; else fail "Open file limits $soft/$hard; require soft and hard >=1024000"; fi
java_output=$(java -version 2>&1); java_status=$?
if (( java_status == 0 )) && [[ "$java_output" =~ version\ \"17\. ]]; then pass 'Java 17'; else fail "Java 17 required: $java_output"; fi
if systemctl is-active --quiet ntpd || systemctl is-active --quiet chronyd; then pass 'NTP/chronyd active'; else fail 'Start ntpd or chronyd'; fi
if sockets=$(ss -H -ltn 2>&1); then
 if awk '{print $4}' <<< "$sockets" | grep -Eq ":${port}$"; then fail "REST port $port is occupied"; else pass "REST port $port is free"; fi
else fail "Cannot inspect listening ports: $sockets"; fi
if [[ "$kind" == kafka-connect ]]; then
 if [[ -d "$plugin" ]]; then
  jars=$(find -L "$plugin" -mindepth 1 -maxdepth 2 -type f -name '*.jar' -print 2>/dev/null); find_status=$?
  if (( find_status == 0 )) && [[ -n "$jars" ]]; then pass "Connector JARs staged in $plugin"; else fail "Stage readable connector JARs in $plugin (or repair broken plugin links)"; fi
 else fail "Plugin directory $plugin does not exist; pre-stage connector JARs"; fi
 ancestor=$working
 while [[ ! -d "$ancestor" && "$ancestor" != / ]]; do ancestor=$(dirname -- "$ancestor"); done
 free_mb=$(df -Pm -- "$ancestor" 2>/dev/null | awk 'NR==2 {print $4}')
 if [[ "$free_mb" =~ ^[0-9]+$ ]] && (( free_mb >= min_disk )); then pass "Working disk: $free_mb MiB available at $ancestor"; else fail "Working disk: need $min_disk MiB for $working; available ${free_mb:-unknown}"; fi
fi
probe() {
 local endpoint=$1 host=${1%:*} target_port=${1##*:}
 host=${host#[}; host=${host%]}
 if [[ "$host" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ || "$host" == *:* ]]; then
  pass "IP endpoint: $host"
 elif getent hosts "$host" >/dev/null 2>&1; then pass "DNS resolution: $host"
 else fail "DNS resolution: $host"; return
 fi
 if [[ "$deferred" == true ]]; then defer_check "Reachability $endpoint: required after Kafka starts"; return; fi
 if timeout 5 bash -c 'exec 3<>"/dev/tcp/$1/$2"' -- "$host" "$target_port" 2>/dev/null; then pass "Reachability: $endpoint"; else fail "Unreachable endpoint: $endpoint"; fi
}
IFS=',' read -ra endpoints <<< "$bootstrap"
for endpoint in "${endpoints[@]}"; do probe "$endpoint"; done
if [[ -n "$ha_peers" ]]; then
 IFS=',' read -ra peers <<< "$ha_peers"
 for endpoint in "${peers[@]}"; do probe "$endpoint"; done
fi
for topic in "${topics[@]}"; do
 if [[ "$deferred" == true ]]; then defer_check "Topic $topic: verify after Kafka starts"
 elif [[ ! -x "$kafka_bin/kafka-topics.sh" ]]; then warn "Topic $topic: kafka-topics.sh unavailable at $kafka_bin"
 elif description=$(timeout 15 "$kafka_bin/kafka-topics.sh" --bootstrap-server "$bootstrap" --describe --topic "$topic" 2>&1); then
  if [[ "$kind" != schema-registry || "$description" == *cleanup.policy=compact* ]]; then pass "Topic $topic exists"; else warn "Topic $topic exists; verify cleanup.policy=compact"; fi
 else warn "Topic $topic could not be described; backend must create/verify it before service installation"
 fi
done
if (( failed > 0 )); then printf '[FAIL] %s checks failed\n' "$failed"; exit 1; fi
pass 'Host prechecks passed (any DEFERRED items are mandatory after Kafka starts)'
