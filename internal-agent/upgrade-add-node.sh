#!/usr/bin/env bash
# Upgrade an enrolled internal agent after the management release; preserve its identity/config.
set -Eeuo pipefail
[[ ${EUID:-$(id -u)} -eq 0 ]] || { echo 'Run as root.' >&2; exit 1; }
[[ ${1:-} == --tasks-drained ]] || { echo 'First verify no agent tasks or cluster jobs are running, then use: bash upgrade-add-node.sh --tasks-drained' >&2; exit 1; }
package_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
case "$(uname -m)" in x86_64|amd64) architecture=amd64;; aarch64|arm64) architecture=arm64;; *) echo 'Unsupported CPU architecture.' >&2;exit 1;; esac
relative_binary="bin/tantor-agent-linux-$architecture"
source_binary="$package_dir/$relative_binary"
binary=/opt/tantor-agent/bin/tantor-agent
config=/etc/tantor-agent/agent.yaml
service=tantor-agent.service
[[ -f "$binary" && -f "$config" && -f "$source_binary" ]] || { echo 'Existing enrollment/config and candidate binary are required. Use agent.sh only for a fresh host.' >&2; exit 1; }
[[ ! -L "$binary" && ! -L "$config" ]] || { echo 'Resolve the installed symlink layout before this upgrade.' >&2; exit 1; }
expected=$(awk -v path="$relative_binary" '{ sub(/\r$/, ""); if ($2==path) print $1 }' "$package_dir/SHA256SUMS")
[[ "$expected" =~ ^[a-f0-9]{64}$ ]] || { echo 'Missing or ambiguous binary checksum.' >&2; exit 1; }
(cd "$package_dir" && printf '%s  %s\n' "$expected" "$relative_binary" | sha256sum -c -)
chmod 0755 "$source_binary"
"$source_binary" -version | grep -q -- '-addnode\.' || { echo 'Candidate lacks Add Node support.' >&2; exit 1; }
systemctl show "$service" -p ExecStart --value | grep -Fq -- "$binary" || { echo 'Installed service uses a different binary path; review its unit before upgrading.' >&2;exit 1; }
config_hash=$(sha256sum "$config" | cut -d' ' -f1)
"$source_binary" -config "$config" -check-config >/dev/null
backup="$binary.before-addnode.$(date -u +%Y%m%dT%H%M%SZ).$$"
cp -p -- "$binary" "$backup"
staged=$(mktemp /opt/tantor-agent/bin/.tantor-agent-update.XXXXXX)
trap 'rm -f -- "$staged"' EXIT
install -m 0755 -- "$source_binary" "$staged"
mv -f -- "$staged" "$binary"
if ! systemctl restart "$service" || ! systemctl is-active --quiet "$service"; then
  install -m 0755 -- "$backup" "$binary"
  systemctl restart "$service" || true
  echo "Agent restart failed; previous binary restored from $backup. Kafka service units were not changed." >&2
  exit 1
fi
[[ $(sha256sum "$config" | cut -d' ' -f1) == "$config_hash" ]] || { echo 'Agent config changed concurrently; inspect before testing.' >&2;exit 1; }
"$binary" -version
printf 'Internal agent upgraded. Config/identity preserved. Previous binary: %s\n' "$backup"
printf 'Verify this host heartbeat and existing task compatibility before upgrading the next host.\n'
