#!/usr/bin/env bash
set -Eeuo pipefail
umask 027

# -----------------------------------------------------------------------------
# Tantor Kafka Discovery Agent - RHEL 8/9 HTTP-only offline installer
#
# Assumptions:
#   1. This script and the prebuilt agent binaries are in the same directory.
#   2. Nothing is downloaded from the internet.
#   3. The agent connects OUTBOUND to the central backend over plain HTTP.
#   4. The agent runs locally as a limited Linux user.
#   5. Target OS is Red Hat Enterprise Linux 8 or 9.
#   6. SELinux is left enabled; the installer restores default file contexts.
#   7. No firewall rule is added because the agent only initiates outbound HTTP.
#
# Expected bundle files:
#   install-agent.sh
#   tantor-discovery-agent-linux-amd64
#   tantor-discovery-agent-linux-arm64
# -----------------------------------------------------------------------------

SERVICE_NAME="tantor-discovery-agent"
CREATE_RUNTIME_USER="false"
INSTALL_DIR="/opt/tantor-agent"
CONFIG_DIR="/etc/tantor-agent"
STATE_DIR="/var/lib/tantor-agent"
LOG_DIR="/var/log/tantor/discovery-agent"

SERVER_URL=""
SERVER_HOST=""
SERVER_PORT=""

RUN_USER="tantor-agent"
RUN_GROUP="tantor-agent"
RUN_GROUP_EXPLICIT="false"
KAFKA_GROUPS=""
KAFKA_ACCESS_GROUPS=""
KAFKA_SUPPLEMENTARY_GROUPS=""

HOST_ID=""
AGENT_NAME=""
NODE_NAME=""
CLUSTER_NAME=""
ENVIRONMENT=""
SCAN_PATHS=""
DISCOVERY_POLICY="running-only"

DISCOVERY_INTERVAL="10s"
TASK_POLL_INTERVAL="5s"
METRICS_INTERVAL="10s"
HTTP_TIMEOUT="20s"
HTTP_RETRIES="3"
JMX_METRICS_URL=""
JMX_IP=""
JMX_PORT=""
CONTROLLER_JMX_METRICS_URL=""
CONTROLLER_JMX_IP=""
CONTROLLER_JMX_PORT=""
KAFKA_EXPORTER_METRICS_URL=""
KAFKA_EXPORTER_IP=""
KAFKA_EXPORTER_PORT=""
KAFKA_CONNECT_TIMEOUT="5s"
KAFKA_CLIENT_PROPERTIES=""
LOG_LEVEL="info"
RUN_PRECHECK="true"

ENABLE_TASKS="false"
ALLOW_CONFIG_WRITE="false"
ALLOW_SERVICE_RESTART="false"
RESTART_WITH_SUDO="false"
ALLOWED_SERVICES=""

# Backend/API authentication is optional and separate from the Linux runtime user.
# Because this installer is intentionally HTTP-only, credentials sent using Basic
# or Bearer authentication travel over plaintext HTTP. Use only where the client
# network/security policy explicitly permits this temporary design.
AUTH_TYPE="none"
AUTH_USERNAME=""
TOKEN_FILE_SOURCE=""
PASSWORD_FILE_SOURCE=""

BINARY_SOURCE=""
REQUIRE_SERVER_REACHABLE="false"

RHEL_ID=""
RHEL_VERSION=""
RHEL_MAJOR=""
SELINUX_MODE="Unknown"
SYSTEMD_VERSION="Unknown"

log()  { printf '[INFO] %s\n' "$*"; }
warn() { printf '[WARN] %s\n' "$*" >&2; }
die()  { printf '[ERROR] %s\n' "$*" >&2; exit 1; }

usage() {
  cat <<'USAGE'
Tantor Kafka Discovery Agent - RHEL 8/9 HTTP-only offline installer

Supported OS:
  Red Hat Enterprise Linux 8.x and 9.x

Usage:
  sudo ./install-agent.sh --server-ip <IP-or-hostname> --server-port <PORT> [options]

Or:
  sudo ./install-agent.sh --server-url http://<IP-or-hostname>:<PORT> [options]

Required connection options:
  --server-ip HOST                 Backend IP/hostname, e.g. backend.example.internal
  --server-port PORT               Backend HTTP port selected for this deployment.

  OR

  --server-url URL                 Full HTTP URL, e.g. http://<backend-host>:<backend-port>
                                   HTTPS is intentionally rejected by this installer.

Local runtime identity:
  --run-user USER                  Linux user used to run the agent.
                                   Default: tantor-agent
  --run-group GROUP                Linux group used to run the agent.
                                   Default: same as --run-user
  --create-runtime-user             Create a dedicated system account if absent.
  --kafka-groups CSV               Optional additional existing groups to add the runtime user to,
                                   e.g. kafka,kafkaops. The installer also automatically
                                   detects Kafka groups from running Kafka services and
                                   Kafka configuration files.

Agent identity and discovery:
  --host-id ID                     Optional stable host ID. Auto-generated if empty.
  --agent-name NAME                Optional agent name. Auto-generated if empty.
  --node-name NAME                 Required approved Kafka VM identity.
  --cluster-name NAME              Optional cluster name override.
  --environment NAME               Required explicit environment label.
  --discovery-policy POLICY       running-only (default), running-with-offline-inventory, filesystem-only
  --scan-paths CSV                 Required exact Kafka scan roots; no broad default.

Optional backend/API authentication over HTTP:
  --auth-type TYPE                 none | basic | bearer. Default: none
  --auth-username USER             Required for basic auth.
  --password-file FILE             File containing basic-auth password.
  --token-file FILE                File containing bearer token.

Task permissions:
  --enable-tasks                   Enable backend task polling. Disabled by
                                   default for this external monitoring agent.
  --disable-tasks                  Explicitly keep backend task polling disabled.
  --enable-config-write            Allow updates/restores of the exact discovered
                                   Kafka config file. Disabled by default.
  --enable-service-restart         Allow restart of allowlisted discovered services.
  --restart-with-sudo              Use sudo -n for approved systemd restarts and
                                   create a least-privilege sudoers rule.
  --allowed-services CSV           Explicit systemd service allowlist.

Intervals and monitoring:
  --discovery-interval DURATION    Default: 10s
  --task-poll-interval DURATION    Default: 5s
  --metrics-interval DURATION      Default: 10s
  --http-timeout DURATION          Default: 20s
  --http-retries N                 Default: 3, allowed: 0-10
  --jmx-metrics-url URL            Optional JMX exporter URL.
  --jmx-ip HOST                    Optional JMX Exporter IP/host; use together
                                   with --jmx-port.
  --jmx-port PORT                  Optional JMX Exporter HTTP port. If both JMX
                                   options are omitted, Kafka-only mode is used.
  --controller-jmx-metrics-url URL Optional controller-only JMX exporter URL.
  --controller-jmx-ip HOST         Optional controller JMX Exporter IP/host; use
                                   together with --controller-jmx-port.
  --controller-jmx-port PORT       Optional controller JMX Exporter HTTP port.
                                   Controller-only nodes do not reuse --jmx-port.
  --kafka-exporter-metrics-url URL Optional Kafka Exporter /metrics URL.
  --kafka-exporter-ip HOST         Optional Kafka Exporter IP/host; use together
                                   with --kafka-exporter-port.
  --kafka-exporter-port PORT       Optional Kafka Exporter HTTP port.
  --kafka-connect-timeout DURATION Real Kafka protocol check timeout. Default: 5s
  --kafka-client-properties FILE   Optional Kafka client properties used by the
                                   protocol check for SSL/SASL listeners. The
                                   runtime user must be able to read this file
                                   and every referenced truststore/keystore.
  --skip-precheck                  Skip local Kafka precheck on agent startup.
  --log-level LEVEL                debug | info | warn | error

Installation paths:
  --service-name NAME              systemd service base name. Default: tantor-discovery-agent
  --install-dir DIR                Default: /opt/tantor-agent
  --config-dir DIR                 Default: /etc/tantor-agent
  --state-dir DIR                  Default: /var/lib/tantor-agent
  --binary FILE                    Explicit prebuilt agent binary to install.

Validation:
  --require-server-reachable       Fail installation if backend TCP host:port is
                                   not reachable during installation.
  -h, --help                       Show this help.

Generic example:
  sudo ./install-agent.sh \
    --server-ip <backend-host> \
    --server-port <backend-port> \
    --run-user <approved-agent-user> \
    --run-group <approved-agent-group> \
    --node-name <approved-node-identity> \
    --environment <environment> \
    --scan-paths <kafka-install-root>,<kafka-data-root> \
    --cluster-name <logical-cluster-name>

Important for RHEL 8/9:
  The Linux --run-user belongs to the local agent VM. It is NOT the username used
  to log in to the backend VM. The agent makes an outbound HTTP API connection and
  does not SSH into the backend machine.

  SELinux is not disabled. Default labels are restored with restorecon when available.
  The installer does not modify firewalld because the agent does not listen on an
  inbound port; it initiates an outbound HTTP connection to the configured backend.

  The installer automatically detects non-privileged Kafka Linux groups from
  running Kafka services and Kafka configuration ownership. The selected
  --run-user is added to those groups before the agent starts. If group access is
  still insufficient, the installer applies a minimal per-user ACL when setfacl
  is available. It never adds the runtime user to root, wheel, or similar groups.
USAGE
}

# Remove accidental leading/trailing whitespace from values received through
# command-line options. This makes values with accidental surrounding spaces safe
# all internal characters unchanged.
trim_whitespace() {
  local value="${1:-}"
  value="${value#"${value%%[![:space:]]*}"}"
  value="${value%"${value##*[![:space:]]}"}"
  printf '%s' "$value"
}

# Confirm that an option has a real following value. Also reject a new --option
# accidentally being consumed as the value of the previous option.
require_value() {
  local option="${1:-}"
  local value="${2:-}"

  [[ $# -ge 2 ]] || die "Missing value for $option"
  value="$(trim_whitespace "$value")"
  [[ -n "$value" ]] || die "Missing value for $option"
  [[ "$value" != --* ]] || die "Missing value for $option. Found another option instead: $value"
}

is_valid_linux_name() {
  [[ "$1" =~ ^[a-z_][a-z0-9_-]*[$]?$ ]]
}

contains_newline() {
  [[ "$1" == *$'\n'* || "$1" == *$'\r'* ]]
}

validate_single_line() {
  local name="$1"
  local value="$2"
  if contains_newline "$value"; then
    die "$name must not contain newline characters."
  fi
  return 0
}

validate_duration() {
  local name="$1"
  local value="$2"
  [[ "$value" =~ ^[0-9]+([.][0-9]+)?(ns|us|µs|ms|s|m|h)$ ]] || \
    die "$name has invalid Go duration format: $value (examples: 5s, 30s, 2m)"
}

validate_absolute_csv_paths() {
  local csv="$1"
  local item
  local valid_count=0
  IFS=',' read -r -a _paths <<< "$csv"
  [[ ${#_paths[@]} -gt 0 ]] || die "--scan-paths must contain at least one path."
  for item in "${_paths[@]}"; do
    item="${item#"${item%%[![:space:]]*}"}"
    item="${item%"${item##*[![:space:]]}"}"
    [[ -n "$item" ]] || die "--scan-paths must not contain empty entries."
    [[ "$item" == /* ]] || die "Scan path must be absolute: $item"
    [[ "$(readlink -m -- "$item")" != "/" ]] || die "Scan path / is not allowed."
    valid_count=$((valid_count + 1))
  done
  (( valid_count > 0 )) || die "--scan-paths must contain at least one valid absolute path."
}

normalize_service_csv() {
  local csv="$1"
  local output=""
  local service
  IFS=',' read -r -a _services <<< "$csv"
  for service in "${_services[@]}"; do
    service="${service//[[:space:]]/}"
    [[ -z "$service" ]] && continue
    [[ "$service" =~ ^[A-Za-z0-9_.@:-]+\.service$ ]] || die "Invalid systemd service name: $service"
    [[ -n "$output" ]] && output+=","
    output+="$service"
  done
  printf '%s' "$output"
}

# Return success for privileged groups that must never be granted to the agent.
# In particular, adding a service user to root or wheel would violate the
# least-privilege design of this installer.
is_privileged_group() {
  case "$1" in
    root|wheel|sudo|adm|disk|kmem|shadow|systemd-journal)
      return 0
      ;;
    *)
      return 1
      ;;
  esac
}

# Append a Linux group to a comma-separated list without creating duplicates.
append_unique_csv() {
  local current="$1"
  local item="$2"
  local existing

  [[ -n "$item" ]] || {
    printf '%s' "$current"
    return 0
  }

  IFS=',' read -r -a _existing_items <<< "$current"
  for existing in "${_existing_items[@]:-}"; do
    [[ "$existing" == "$item" ]] && {
      printf '%s' "$current"
      return 0
    }
  done

  if [[ -n "$current" ]]; then
    printf '%s,%s' "$current" "$item"
  else
    printf '%s' "$item"
  fi
}

# Record a Kafka access group after validating that it exists and is safe.
record_kafka_group() {
  local group
  local source="${2:-detected Kafka resource}"

  group="$(trim_whitespace "${1:-}")"
  [[ -n "$group" ]] || return 0
  [[ "$group" != "UNKNOWN" ]] || return 0
  is_valid_linux_name "$group" || {
    warn "Ignoring invalid Kafka group name [$group] discovered from $source."
    return 0
  }
  getent group "$group" >/dev/null 2>&1 || {
    warn "Ignoring Kafka group [$group] from $source because it does not exist locally."
    return 0
  }
  if is_privileged_group "$group"; then
    warn "Not adding $RUN_USER to privileged group [$group] discovered from $source."
    return 0
  fi
  [[ "$group" != "$RUN_GROUP" ]] || return 0

  KAFKA_ACCESS_GROUPS="$(append_unique_csv "$KAFKA_ACCESS_GROUPS" "$group")"
}

# Collect running Kafka broker/controller configuration paths. These paths are
# later used both for automatic group detection and for access verification.
discover_running_kafka_configs() {
  RUNNING_KAFKA_CONFIGS=()

  command -v ps >/dev/null 2>&1 || return 0

  mapfile -t RUNNING_KAFKA_CONFIGS < <(
    ps -eo args= 2>/dev/null \
      | grep -E 'kafka\.Kafka|kafka-server-start(\.sh)?' \
      | grep -Ev 'ConnectDistributed|ConnectStandalone|SchemaRegistry|grep -E' \
      | grep -oE '/[^[:space:]]+/(server|broker|controller)\.properties' \
      | sort -u || true
  )
}

# Collect likely Kafka configuration files below the exact scan roots supplied
# by the caller. The maximum depth and file names keep this bounded and avoid a
# broad filesystem crawl.
discover_scan_kafka_configs() {
  SCAN_KAFKA_CONFIGS=()

  command -v find >/dev/null 2>&1 || return 0

  local root
  IFS=',' read -r -a _scan_roots_for_detection <<< "$SCAN_PATHS"

  for root in "${_scan_roots_for_detection[@]}"; do
    root="$(trim_whitespace "$root")"
    [[ -n "$root" && -e "$root" ]] || continue

    while IFS= read -r -d '' _cfg; do
      SCAN_KAFKA_CONFIGS+=("$_cfg")

      # Do not use: (( condition )) && return 0
      # With set -e, a false arithmetic condition returns status 1 and can make
      # the whole function terminate the installer even though nothing failed.
      if (( ${#SCAN_KAFKA_CONFIGS[@]} >= 100 )); then
        return 0
      fi
    done < <(
      find -L "$root" -maxdepth 6 -type f \
        \( -name server.properties -o -name broker.properties -o -name controller.properties \) \
        -print0 2>/dev/null || true
    )
  done

  # Discovery with fewer than 100 files is a successful result.
  return 0
}

# Detect the Linux groups used by running Kafka systemd services. A unit may
# explicitly set Group=, or it may inherit the primary group of its User=.
detect_kafka_service_groups() {
  local service exec_start service_user service_group service_supp_groups service_lc exec_lc group

  command -v systemctl >/dev/null 2>&1 || return 0

  while read -r service _rest; do
    [[ -n "$service" ]] || continue

    exec_start="$(systemctl show "$service" -p ExecStart --value 2>/dev/null || true)"
    service_lc="${service,,}"
    exec_lc="${exec_start,,}"

    if [[ "$service_lc" != *kafka* &&
          "$service_lc" != *broker* &&
          "$service_lc" != *controller* &&
          "$exec_lc" != *kafka* ]]; then
      continue
    fi

    service_group="$(systemctl show "$service" -p Group --value 2>/dev/null || true)"
    service_user="$(systemctl show "$service" -p User --value 2>/dev/null || true)"
    service_supp_groups="$(systemctl show "$service" -p SupplementaryGroups --value 2>/dev/null || true)"

    if [[ -z "$service_group" && -n "$service_user" && "$service_user" != "root" ]]; then
      service_group="$(id -gn "$service_user" 2>/dev/null || true)"
    fi

    record_kafka_group "$service_group" "systemd service $service"
    for group in $service_supp_groups; do
      record_kafka_group "$group" "SupplementaryGroups of systemd service $service"
    done
  done < <(
    systemctl list-units --type=service --state=running --no-legend --no-pager 2>/dev/null || true
  )

  return 0
}

# Detect Kafka groups from running and scanned configuration files/directories.
detect_kafka_file_groups() {
  local path group owner owner_group

  for path in "${RUNNING_KAFKA_CONFIGS[@]:-}" "${SCAN_KAFKA_CONFIGS[@]:-}"; do
    [[ -n "$path" && -e "$path" ]] || continue

    group="$(stat -Lc '%G' "$path" 2>/dev/null || true)"
    record_kafka_group "$group" "Kafka config $path"

    owner="$(stat -Lc '%U' "$path" 2>/dev/null || true)"
    if [[ -n "$owner" && "$owner" != "root" && "$owner" != "UNKNOWN" ]]; then
      owner_group="$(id -gn "$owner" 2>/dev/null || true)"
      record_kafka_group "$owner_group" "owner of Kafka config $path"
    fi
  done

  return 0
}

# Add the selected runtime user to all automatically detected and explicitly
# requested Kafka groups. usermod -aG preserves every existing supplementary
# group and therefore does not remove any prior client configuration.
apply_kafka_group_membership() {
  local group

  if [[ -n "$KAFKA_GROUPS" ]]; then
    IFS=',' read -r -a _explicit_groups <<< "$KAFKA_GROUPS"
    for group in "${_explicit_groups[@]}"; do
      group="$(trim_whitespace "$group")"
      [[ -n "$group" ]] || continue
      getent group "$group" >/dev/null 2>&1 || die "Kafka group does not exist: $group"
      is_privileged_group "$group" && die "Refusing to add $RUN_USER to privileged group: $group"
      record_kafka_group "$group" "--kafka-groups"
    done
  fi

  [[ -n "$KAFKA_ACCESS_GROUPS" ]] || {
    log "No non-privileged Kafka Linux group was detected. Exact config-file access will be verified separately."
    return 0
  }

  IFS=',' read -r -a _detected_groups <<< "$KAFKA_ACCESS_GROUPS"
  for group in "${_detected_groups[@]}"; do
    [[ -n "$group" ]] || continue

    if id -nG "$RUN_USER" 2>/dev/null | tr ' ' '\n' | grep -Fxq "$group"; then
      log "Runtime user $RUN_USER is already a member of Kafka group: $group"
    else
      log "Automatically adding runtime user $RUN_USER to Kafka group: $group"
      usermod -a -G "$group" "$RUN_USER"
    fi
  done

  # Convert CSV to the space-separated format accepted by systemd's
  # SupplementaryGroups= directive.
  KAFKA_SUPPLEMENTARY_GROUPS="${KAFKA_ACCESS_GROUPS//,/ }"

  log "Effective groups for $RUN_USER: $(id -nG "$RUN_USER" 2>/dev/null || true)"
  return 0
}

# Grant minimal per-user ACL access only when group membership is insufficient.
# The installer never adds the runtime user to root/wheel. For a config file, the
# user receives read permission on that exact file and traverse permission on its
# parent directories. Existing owner/group/mode bits are not changed.
grant_minimal_config_acl() {
  local path="$1"
  local resolved parent
  local -a parents=()
  local index

  command -v setfacl >/dev/null 2>&1 || return 1
  command -v getfacl >/dev/null 2>&1 || return 1
  [[ -e "$path" ]] || return 1

  resolved="$(readlink -m -- "$path")"
  parent="$(dirname -- "$resolved")"

  while [[ "$parent" != "/" && -n "$parent" ]]; do
    parents+=("$parent")
    parent="$(dirname -- "$parent")"
  done

  for (( index=${#parents[@]}-1; index>=0; index-- )); do
    ensure_user_acl_permissions "${parents[$index]}" "--x" || return 1
  done

  ensure_user_acl_permissions "$resolved" "r--" || return 1
  return 0
}

# Add only missing ACL permission bits. This preserves any stronger ACL already
# assigned to an existing runtime user instead of replacing it with --x or r--.
ensure_user_acl_permissions() {
  local path="$1"
  local required="$2"
  local current merged=""
  local index current_char required_char

  command -v setfacl >/dev/null 2>&1 || return 1
  command -v getfacl >/dev/null 2>&1 || return 1

  current="$(
    getfacl -cp -- "$path" 2>/dev/null |
      awk -F: -v user="$RUN_USER" '$1 == "user" && $2 == user { print $3; exit }'
  )"
  [[ "$current" =~ ^[r-][w-][x-]$ ]] || current="---"

  for index in 0 1 2; do
    current_char="${current:index:1}"
    required_char="${required:index:1}"
    if [[ "$current_char" != "-" ]]; then
      merged+="$current_char"
    else
      merged+="$required_char"
    fi
  done

  setfacl -m "u:${RUN_USER}:${merged}" "$path" >/dev/null 2>&1
}

# Verify that the final runtime identity can read every relevant Kafka config.
# If group membership alone is not enough, apply a minimal ACL automatically.
verify_kafka_config_access() {
  local path
  local -a configs_to_check=()
  declare -A seen_configs=()

  for path in "${RUNNING_KAFKA_CONFIGS[@]:-}" "${SCAN_KAFKA_CONFIGS[@]:-}"; do
    [[ -n "$path" && -e "$path" ]] || continue
    [[ -n "${seen_configs[$path]:-}" ]] && continue
    seen_configs["$path"]=1
    configs_to_check+=("$path")
  done

  for path in "${configs_to_check[@]:-}"; do
    [[ -n "$path" ]] || continue

    if runuser -u "$RUN_USER" -- test -r "$path" 2>/dev/null; then
      log "Runtime user can read Kafka config: $path"
      continue
    fi

    warn "Runtime user $RUN_USER still cannot read Kafka config $path after Kafka-group assignment."

    if grant_minimal_config_acl "$path"; then
      log "Granted minimal read/traverse ACL to $RUN_USER for Kafka config: $path"
    else
      die "Unable to grant automatic read access to $path. Group membership was insufficient and setfacl is unavailable or failed. Install the RHEL acl package offline or align the Kafka file group."
    fi

    runuser -u "$RUN_USER" -- test -r "$path" 2>/dev/null || \
      die "Kafka config remains unreadable by $RUN_USER after automatic access configuration: $path"
  done

  return 0
}

# Read the active broker endpoint and use Kafka's own ApiVersions tool. This
# proves protocol-level connectivity; an open TCP port alone is not sufficient.
verify_kafka_protocol_connectivity() {
  local config="" listeners="" controller_names="" endpoint="" entry="" name="" address=""
  local kafka_api_tool="" root="" output_file="" referenced_file=""
  local -a kafka_client_args=()

  for config in "${RUNNING_KAFKA_CONFIGS[@]:-}"; do
    [[ -r "$config" ]] && break
    config=""
  done
  [[ -n "$config" ]] || die "Kafka is not running or its active configuration could not be found; agent onboarding cannot be verified."

  listeners="$(awk -F= '
    /^[[:space:]]*advertised\.listeners[[:space:]]*=/ {sub(/^[^=]*=/,""); print; found=1; exit}
    END {if (!found) exit 1}
  ' "$config" 2>/dev/null || true)"
  if [[ -z "$listeners" ]]; then
    listeners="$(awk -F= '/^[[:space:]]*listeners[[:space:]]*=/ {sub(/^[^=]*=/,""); print; exit}' "$config" 2>/dev/null || true)"
  fi
  controller_names="$(awk -F= '/^[[:space:]]*controller\.listener\.names[[:space:]]*=/ {sub(/^[^=]*=/,""); gsub(/[[:space:]]/,""); print; exit}' "$config" 2>/dev/null || true)"

  IFS=',' read -r -a _listener_entries <<< "$listeners"
  for entry in "${_listener_entries[@]:-}"; do
    entry="$(trim_whitespace "$entry")"
    [[ "$entry" == *"://"* ]] || continue
    name="${entry%%://*}"
    if [[ ",${controller_names}," == *",${name},"* ]]; then
      continue
    fi
    address="${entry#*://}"
    if [[ "$address" == :* || "$address" == 0.0.0.0:* || "$address" == \[::\]:* ]]; then
      address="$NODE_NAME:${address##*:}"
    fi
    endpoint="$address"
    break
  done
  [[ -n "$endpoint" ]] || die "Could not derive a broker endpoint from $config for Kafka connectivity validation."

  IFS=',' read -r -a _scan_roots_for_api <<< "$SCAN_PATHS"
  for root in "${_scan_roots_for_api[@]}"; do
    root="$(trim_whitespace "$root")"
    [[ -d "$root" ]] || continue
    kafka_api_tool="$(find -L "$root" -maxdepth 5 -type f -name kafka-broker-api-versions.sh -perm /111 -print -quit 2>/dev/null || true)"
    [[ -n "$kafka_api_tool" ]] && break
  done
  [[ -n "$kafka_api_tool" ]] || die "kafka-broker-api-versions.sh was not found below --scan-paths; cannot perform the required Kafka protocol check."

  if [[ -n "$KAFKA_CLIENT_PROPERTIES" ]]; then
    if ! runuser -u "$RUN_USER" -- test -r "$KAFKA_CLIENT_PROPERTIES" 2>/dev/null; then
      warn "Kafka client properties are not readable by $RUN_USER: $KAFKA_CLIENT_PROPERTIES"
      if grant_minimal_config_acl "$KAFKA_CLIENT_PROPERTIES"; then
        log "Granted minimal read/traverse ACL to $RUN_USER for Kafka client properties: $KAFKA_CLIENT_PROPERTIES"
      else
        die "Unable to grant automatic access to Kafka client properties. Install the RHEL acl package offline, align the file group, or grant $RUN_USER read access to the exact file."
      fi
    fi
    runuser -u "$RUN_USER" -- test -r "$KAFKA_CLIENT_PROPERTIES" 2>/dev/null || \
      die "Kafka client properties remain unreadable by $RUN_USER after automatic access configuration: $KAFKA_CLIENT_PROPERTIES"

    while IFS= read -r referenced_file; do
      referenced_file="$(trim_whitespace "$referenced_file")"
      [[ -n "$referenced_file" ]] || continue
      [[ "$referenced_file" == /* ]] || \
        die "Kafka client properties references a non-absolute credential path: $referenced_file"
      [[ -f "$referenced_file" ]] || \
        die "Kafka client credential file does not exist: $referenced_file"
      if ! runuser -u "$RUN_USER" -- test -r "$referenced_file" 2>/dev/null; then
        warn "Kafka client credential file is not readable by $RUN_USER: $referenced_file"
        if grant_minimal_config_acl "$referenced_file"; then
          log "Granted minimal read/traverse ACL to $RUN_USER for Kafka credential file: $referenced_file"
        else
          die "Unable to grant automatic access to Kafka credential file $referenced_file. Install the RHEL acl package offline, align the file group, or grant $RUN_USER read access to the exact file."
        fi
      fi
      runuser -u "$RUN_USER" -- test -r "$referenced_file" 2>/dev/null || \
        die "Kafka client credential file remains unreadable by $RUN_USER after automatic access configuration: $referenced_file"
    done < <(
      awk -F= '
        /^[[:space:]]*ssl\.(truststore|keystore)\.location[[:space:]]*=/ {
          sub(/^[^=]*=/, "")
          gsub(/^[[:space:]]+|[[:space:]]+$/, "")
          print
        }
      ' "$KAFKA_CLIENT_PROPERTIES"
    )
    kafka_client_args=(--command-config "$KAFKA_CLIENT_PROPERTIES")
  fi

  log "Validating Kafka protocol connectivity as $RUN_USER: $endpoint"
  output_file="$(mktemp)"
  if command -v timeout >/dev/null 2>&1; then
    if ! runuser -u "$RUN_USER" -- timeout "$KAFKA_CONNECT_TIMEOUT" \
      "$kafka_api_tool" --bootstrap-server "$endpoint" \
      "${kafka_client_args[@]}" >"$output_file" 2>&1; then
      warn "Kafka protocol validation output:"
      sed -n '1,40p' "$output_file" >&2
      rm -f "$output_file"
      die "Kafka protocol check failed for $endpoint. Check reachability and the supplied TLS/SASL client settings."
    fi
  else
    rm -f "$output_file"
    die "timeout is required for the bounded Kafka protocol connectivity check."
  fi
  rm -f "$output_file"
  log "Kafka ApiVersions protocol check passed: $endpoint"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --server-ip|--server-host) require_value "$@"; SERVER_HOST="$2"; shift 2 ;;
    --server-port) require_value "$@"; SERVER_PORT="$2"; shift 2 ;;
    --server-url) require_value "$@"; SERVER_URL="$2"; shift 2 ;;

    --run-user) require_value "$@"; RUN_USER="$2"; shift 2 ;;
    --create-runtime-user) CREATE_RUNTIME_USER="true"; shift ;;
    --run-group) require_value "$@"; RUN_GROUP="$2"; RUN_GROUP_EXPLICIT="true"; shift 2 ;;
    --kafka-groups) require_value "$@"; KAFKA_GROUPS="$2"; shift 2 ;;

    --host-id) require_value "$@"; HOST_ID="$2"; shift 2 ;;
    --agent-name) require_value "$@"; AGENT_NAME="$2"; shift 2 ;;
    --node-name) require_value "$@"; NODE_NAME="$2"; shift 2 ;;
    --cluster-name) require_value "$@"; CLUSTER_NAME="$2"; shift 2 ;;
    --environment) require_value "$@"; ENVIRONMENT="$2"; shift 2 ;;
    --discovery-policy) require_value "$@"; DISCOVERY_POLICY="${2,,}"; shift 2 ;;
    --scan-paths) require_value "$@"; SCAN_PATHS="$2"; shift 2 ;;

    --auth-type) require_value "$@"; AUTH_TYPE="${2,,}"; shift 2 ;;
    --auth-username) require_value "$@"; AUTH_USERNAME="$2"; shift 2 ;;
    --password-file) require_value "$@"; PASSWORD_FILE_SOURCE="$2"; shift 2 ;;
    --token-file) require_value "$@"; TOKEN_FILE_SOURCE="$2"; shift 2 ;;

    --enable-tasks) ENABLE_TASKS="true"; shift ;;
    --disable-tasks) ENABLE_TASKS="false"; shift ;;
    --enable-config-write) ALLOW_CONFIG_WRITE="true"; shift ;;
    --enable-service-restart) ALLOW_SERVICE_RESTART="true"; shift ;;
    --restart-with-sudo) RESTART_WITH_SUDO="true"; shift ;;
    --allowed-services) require_value "$@"; ALLOWED_SERVICES="$2"; shift 2 ;;

    --discovery-interval) require_value "$@"; DISCOVERY_INTERVAL="$2"; shift 2 ;;
    --task-poll-interval) require_value "$@"; TASK_POLL_INTERVAL="$2"; shift 2 ;;
    --metrics-interval) require_value "$@"; METRICS_INTERVAL="$2"; shift 2 ;;
    --http-timeout) require_value "$@"; HTTP_TIMEOUT="$2"; shift 2 ;;
    --http-retries) require_value "$@"; HTTP_RETRIES="$2"; shift 2 ;;
    --jmx-metrics-url) require_value "$@"; JMX_METRICS_URL="$2"; shift 2 ;;
    --jmx-ip) require_value "$@"; JMX_IP="$2"; shift 2 ;;
    --jmx-port) require_value "$@"; JMX_PORT="$2"; shift 2 ;;
    --controller-jmx-metrics-url) require_value "$@"; CONTROLLER_JMX_METRICS_URL="$2"; shift 2 ;;
    --controller-jmx-ip) require_value "$@"; CONTROLLER_JMX_IP="$2"; shift 2 ;;
    --controller-jmx-port) require_value "$@"; CONTROLLER_JMX_PORT="$2"; shift 2 ;;
    --kafka-exporter-metrics-url) require_value "$@"; KAFKA_EXPORTER_METRICS_URL="$2"; shift 2 ;;
    --kafka-exporter-ip) require_value "$@"; KAFKA_EXPORTER_IP="$2"; shift 2 ;;
    --kafka-exporter-port) require_value "$@"; KAFKA_EXPORTER_PORT="$2"; shift 2 ;;
    --kafka-connect-timeout) require_value "$@"; KAFKA_CONNECT_TIMEOUT="$2"; shift 2 ;;
    --kafka-client-properties) require_value "$@"; KAFKA_CLIENT_PROPERTIES="$2"; shift 2 ;;
    --skip-precheck) RUN_PRECHECK="false"; shift ;;
    --log-level) require_value "$@"; LOG_LEVEL="${2,,}"; shift 2 ;;

    --service-name) require_value "$@"; SERVICE_NAME="$2"; shift 2 ;;
    --install-dir) require_value "$@"; INSTALL_DIR="$2"; shift 2 ;;
    --config-dir) require_value "$@"; CONFIG_DIR="$2"; shift 2 ;;
    --state-dir) require_value "$@"; STATE_DIR="$2"; shift 2 ;;
    --binary) require_value "$@"; BINARY_SOURCE="$2"; shift 2 ;;
    --require-server-reachable) REQUIRE_SERVER_REACHABLE="true"; shift ;;

    -h|--help) usage; exit 0 ;;
    *) die "Unknown option: $1. Use --help for usage." ;;
  esac
done

# -----------------------------------------------------------------------------
# Normalize values supplied at runtime.
#
# Bash preserves quoted or escaped whitespace exactly. For example,
# a --run-user value containing a trailing space would otherwise fail
# Linux username validation. Only surrounding whitespace is removed here.
# -----------------------------------------------------------------------------
for _var in \
  SERVER_URL SERVER_HOST SERVER_PORT \
  RUN_USER RUN_GROUP KAFKA_GROUPS \
  HOST_ID AGENT_NAME NODE_NAME CLUSTER_NAME ENVIRONMENT SCAN_PATHS DISCOVERY_POLICY \
  AUTH_TYPE AUTH_USERNAME TOKEN_FILE_SOURCE PASSWORD_FILE_SOURCE \
  ALLOWED_SERVICES DISCOVERY_INTERVAL TASK_POLL_INTERVAL METRICS_INTERVAL \
  KAFKA_CLIENT_PROPERTIES \
  HTTP_TIMEOUT HTTP_RETRIES JMX_METRICS_URL JMX_IP JMX_PORT \
  CONTROLLER_JMX_METRICS_URL CONTROLLER_JMX_IP CONTROLLER_JMX_PORT \
  KAFKA_EXPORTER_METRICS_URL KAFKA_EXPORTER_IP KAFKA_EXPORTER_PORT \
  KAFKA_CONNECT_TIMEOUT LOG_LEVEL \
  SERVICE_NAME INSTALL_DIR CONFIG_DIR STATE_DIR BINARY_SOURCE; do
  printf -v "$_var" '%s' "$(trim_whitespace "${!_var}")"
done
unset _var

LOG_FILE="$LOG_DIR/${SERVICE_NAME}.log"

# -----------------------------------------------------------------------------
# Validate installer environment and input.
# -----------------------------------------------------------------------------
[[ ${EUID:-$(id -u)} -eq 0 ]] || die "Run this installer as root, for example: sudo ./install-agent.sh ..."

# RHEL 8/9 validation. No package installation is attempted because the client is air-gapped.
[[ -r /etc/os-release ]] || die "Cannot read /etc/os-release; unable to verify RHEL version."
# shellcheck disable=SC1091
source /etc/os-release
RHEL_ID="${ID:-unknown}"
RHEL_VERSION="${VERSION_ID:-unknown}"
RHEL_MAJOR="${RHEL_VERSION%%.*}"

if [[ "$RHEL_ID" != "rhel" ]]; then
  die "This installer is intended for Red Hat Enterprise Linux 8 or 9. Detected ID=$RHEL_ID VERSION_ID=$RHEL_VERSION"
fi
case "$RHEL_MAJOR" in
  8|9) ;;
  *) die "Unsupported RHEL major version: $RHEL_VERSION. Supported: RHEL 8 and RHEL 9." ;;
esac

for cmd in uname getent id install systemctl groupadd useradd usermod mktemp runuser \
  stat readlink dirname grep sort tr awk chown chmod mv journalctl sleep; do
  command -v "$cmd" >/dev/null 2>&1 || die "Required command not found: $cmd. Ensure the standard RHEL base utilities are installed offline."
done

if command -v getenforce >/dev/null 2>&1; then
  SELINUX_MODE="$(getenforce 2>/dev/null || printf 'Unknown')"
fi
SYSTEMD_VERSION="$(systemctl --version 2>/dev/null | awk 'NR==1 {print $2}' || true)"
[[ -n "$SYSTEMD_VERSION" ]] || SYSTEMD_VERSION="Unknown"

log "Detected Red Hat Enterprise Linux $RHEL_VERSION"
log "Detected systemd version: $SYSTEMD_VERSION"
log "SELinux mode: $SELINUX_MODE"

if [[ -n "$SERVER_URL" ]]; then
  [[ -z "$SERVER_HOST" && -z "$SERVER_PORT" ]] || die "Use either --server-url OR --server-ip/--server-port, not both."
  [[ "$SERVER_URL" == http://* ]] || die "This installer is HTTP-only. --server-url must start with http://"
  [[ "$SERVER_URL" != https://* ]] || die "HTTPS is intentionally disabled in this installer."
  [[ "$SERVER_URL" != *'?'* && "$SERVER_URL" != *'#'* ]] || die "Server URL must not contain a query string or fragment."
  SERVER_URL="${SERVER_URL%/}"

  # Extract host and port for optional TCP reachability check.
  _authority="${SERVER_URL#http://}"
  _authority="${_authority%%/*}"
  if [[ "$_authority" =~ ^\[([^]]+)\]:([0-9]+)$ ]]; then
    SERVER_HOST="${BASH_REMATCH[1]}"
    SERVER_PORT="${BASH_REMATCH[2]}"
  elif [[ "$_authority" =~ ^([^:]+):([0-9]+)$ ]]; then
    SERVER_HOST="${BASH_REMATCH[1]}"
    SERVER_PORT="${BASH_REMATCH[2]}"
  else
    die "--server-url must explicitly contain a port, e.g. http://<backend-host>:<backend-port>"
  fi
else
  [[ -n "$SERVER_HOST" ]] || die "--server-ip/--server-host is required."
  [[ -n "$SERVER_PORT" ]] || die "--server-port is required."
  [[ "$SERVER_HOST" != *'/'* && "$SERVER_HOST" != *' '* ]] || die "Invalid server host: $SERVER_HOST"
  if [[ "$SERVER_HOST" == *:* && "$SERVER_HOST" != \[*\] ]]; then
    SERVER_URL="http://[$SERVER_HOST]:$SERVER_PORT"
  else
    SERVER_URL="http://$SERVER_HOST:$SERVER_PORT"
  fi
fi

[[ -n "$SERVER_HOST" ]] || die "Could not determine backend host."
[[ "$SERVER_HOST" != *'/'* && "$SERVER_HOST" != *' '* && "$SERVER_HOST" != *$'\n'* && "$SERVER_HOST" != *$'\r'* ]] || die "Invalid server host: $SERVER_HOST"

[[ "$SERVER_PORT" =~ ^[0-9]+$ ]] || die "Server port must be numeric: $SERVER_PORT"
(( SERVER_PORT >= 1 && SERVER_PORT <= 65535 )) || die "Server port must be between 1 and 65535."

is_valid_linux_name "$RUN_USER" || die "Invalid --run-user: [$RUN_USER]"
if [[ "$RUN_GROUP_EXPLICIT" != "true" && "$RUN_USER" != "tantor-agent" ]]; then
  # Make the default group follow a custom runtime user unless --run-group was explicitly supplied.
  RUN_GROUP="$RUN_USER"
fi
is_valid_linux_name "$RUN_GROUP" || die "Invalid --run-group: [$RUN_GROUP]"

# Show the final values that will be used. Brackets make accidental whitespace
# visible during client onboarding and confirm that command-line overrides won.
log "Parsed runtime parameters:"
log "  Runtime user     : [$RUN_USER]"
log "  Runtime group    : [$RUN_GROUP]"
log "  Backend URL      : [$SERVER_URL]"
log "  Node name        : [$NODE_NAME]"
log "  Cluster name     : [$CLUSTER_NAME]"
log "  Environment      : [$ENVIRONMENT]"
log "  Scan paths       : [$SCAN_PATHS]"
log "  Discovery policy : [$DISCOVERY_POLICY]"

case "$AUTH_TYPE" in
  none) ;;
  basic)
    [[ -n "$AUTH_USERNAME" ]] || die "Basic auth requires --auth-username."
    [[ -n "$PASSWORD_FILE_SOURCE" ]] || die "Basic auth requires --password-file."
    warn "Basic authentication is being used over plaintext HTTP. Credentials are not encrypted in transit."
    ;;
  bearer)
    [[ -n "$TOKEN_FILE_SOURCE" ]] || die "Bearer auth requires --token-file."
    warn "Bearer authentication is being used over plaintext HTTP. The token is not encrypted in transit."
    ;;
  *) die "Invalid --auth-type: $AUTH_TYPE. Allowed: none, basic, bearer." ;;
esac

[[ "$HTTP_RETRIES" =~ ^[0-9]+$ ]] || die "--http-retries must be an integer."
(( HTTP_RETRIES >= 0 && HTTP_RETRIES <= 10 )) || die "--http-retries must be between 0 and 10."

validate_duration "--discovery-interval" "$DISCOVERY_INTERVAL"
validate_duration "--task-poll-interval" "$TASK_POLL_INTERVAL"
validate_duration "--metrics-interval" "$METRICS_INTERVAL"
validate_duration "--http-timeout" "$HTTP_TIMEOUT"
validate_duration "--kafka-connect-timeout" "$KAFKA_CONNECT_TIMEOUT"
if [[ -n "$JMX_METRICS_URL" && ( -n "$JMX_IP" || -n "$JMX_PORT" ) ]]; then
  die "Use either --jmx-metrics-url OR --jmx-ip with --jmx-port, not both."
fi
if [[ -n "$JMX_IP" || -n "$JMX_PORT" ]]; then
  [[ -n "$JMX_IP" && -n "$JMX_PORT" ]] || die "--jmx-ip and --jmx-port must be supplied together."
  [[ "$JMX_IP" != *'/'* && "$JMX_IP" != *' '* ]] || die "Invalid --jmx-ip: $JMX_IP"
  [[ "$JMX_PORT" =~ ^[0-9]+$ ]] || die "--jmx-port must be numeric: $JMX_PORT"
  (( JMX_PORT >= 1 && JMX_PORT <= 65535 )) || die "--jmx-port must be between 1 and 65535."
  if [[ "$JMX_IP" == *:* && "$JMX_IP" != \[*\] ]]; then
    JMX_METRICS_URL="http://[$JMX_IP]:$JMX_PORT/metrics"
  else
    JMX_METRICS_URL="http://$JMX_IP:$JMX_PORT/metrics"
  fi
fi
if [[ -n "$JMX_METRICS_URL" ]]; then
  [[ "$JMX_METRICS_URL" == http://* || "$JMX_METRICS_URL" == https://* ]] || \
    die "--jmx-metrics-url must start with http:// or https://"
  [[ "$JMX_METRICS_URL" != *'?'* && "$JMX_METRICS_URL" != *'#'* ]] || \
    die "--jmx-metrics-url must not contain a query string or fragment."
fi
if [[ -n "$CONTROLLER_JMX_METRICS_URL" && ( -n "$CONTROLLER_JMX_IP" || -n "$CONTROLLER_JMX_PORT" ) ]]; then
  die "Use either --controller-jmx-metrics-url OR --controller-jmx-ip with --controller-jmx-port, not both."
fi
if [[ -n "$CONTROLLER_JMX_IP" || -n "$CONTROLLER_JMX_PORT" ]]; then
  [[ -n "$CONTROLLER_JMX_IP" && -n "$CONTROLLER_JMX_PORT" ]] || \
    die "--controller-jmx-ip and --controller-jmx-port must be supplied together."
  [[ "$CONTROLLER_JMX_IP" != *'/'* && "$CONTROLLER_JMX_IP" != *' '* ]] || \
    die "Invalid --controller-jmx-ip: $CONTROLLER_JMX_IP"
  [[ "$CONTROLLER_JMX_PORT" =~ ^[0-9]+$ ]] || \
    die "--controller-jmx-port must be numeric: $CONTROLLER_JMX_PORT"
  (( CONTROLLER_JMX_PORT >= 1 && CONTROLLER_JMX_PORT <= 65535 )) || \
    die "--controller-jmx-port must be between 1 and 65535."
  if [[ "$CONTROLLER_JMX_IP" == *:* && "$CONTROLLER_JMX_IP" != \[*\] ]]; then
    CONTROLLER_JMX_METRICS_URL="http://[$CONTROLLER_JMX_IP]:$CONTROLLER_JMX_PORT/metrics"
  else
    CONTROLLER_JMX_METRICS_URL="http://$CONTROLLER_JMX_IP:$CONTROLLER_JMX_PORT/metrics"
  fi
fi
if [[ -n "$CONTROLLER_JMX_METRICS_URL" ]]; then
  [[ "$CONTROLLER_JMX_METRICS_URL" == http://* || "$CONTROLLER_JMX_METRICS_URL" == https://* ]] || \
    die "--controller-jmx-metrics-url must start with http:// or https://"
  [[ "$CONTROLLER_JMX_METRICS_URL" != *'?'* && "$CONTROLLER_JMX_METRICS_URL" != *'#'* ]] || \
    die "--controller-jmx-metrics-url must not contain a query string or fragment."
fi
if [[ -n "$KAFKA_EXPORTER_METRICS_URL" && ( -n "$KAFKA_EXPORTER_IP" || -n "$KAFKA_EXPORTER_PORT" ) ]]; then
  die "Use either --kafka-exporter-metrics-url OR --kafka-exporter-ip with --kafka-exporter-port, not both."
fi
if [[ -n "$KAFKA_EXPORTER_IP" || -n "$KAFKA_EXPORTER_PORT" ]]; then
  [[ -n "$KAFKA_EXPORTER_IP" && -n "$KAFKA_EXPORTER_PORT" ]] || \
    die "--kafka-exporter-ip and --kafka-exporter-port must be supplied together."
  [[ "$KAFKA_EXPORTER_IP" != *'/'* && "$KAFKA_EXPORTER_IP" != *' '* ]] || \
    die "Invalid --kafka-exporter-ip: $KAFKA_EXPORTER_IP"
  [[ "$KAFKA_EXPORTER_PORT" =~ ^[0-9]+$ ]] || die "--kafka-exporter-port must be numeric."
  (( KAFKA_EXPORTER_PORT >= 1 && KAFKA_EXPORTER_PORT <= 65535 )) || \
    die "--kafka-exporter-port must be between 1 and 65535."
  if [[ "$KAFKA_EXPORTER_IP" == *:* && "$KAFKA_EXPORTER_IP" != \[*\] ]]; then
    KAFKA_EXPORTER_METRICS_URL="http://[$KAFKA_EXPORTER_IP]:$KAFKA_EXPORTER_PORT/metrics"
  else
    KAFKA_EXPORTER_METRICS_URL="http://$KAFKA_EXPORTER_IP:$KAFKA_EXPORTER_PORT/metrics"
  fi
fi
if [[ -n "$KAFKA_EXPORTER_METRICS_URL" ]]; then
  [[ "$KAFKA_EXPORTER_METRICS_URL" == http://* || "$KAFKA_EXPORTER_METRICS_URL" == https://* ]] || \
    die "--kafka-exporter-metrics-url must start with http:// or https://"
  [[ "$KAFKA_EXPORTER_METRICS_URL" != *'?'* && "$KAFKA_EXPORTER_METRICS_URL" != *'#'* ]] || \
    die "--kafka-exporter-metrics-url must not contain a query string or fragment."
fi
[[ -n "$SCAN_PATHS" ]] || die "--scan-paths is required for production onboarding."
validate_absolute_csv_paths "$SCAN_PATHS"
[[ -n "$ENVIRONMENT" ]] || die "--environment is required."
[[ -n "$NODE_NAME" ]] || die "--node-name is required to avoid multi-NIC ambiguity."
SERVICE_NAME="${SERVICE_NAME%.service}"
[[ "$SERVICE_NAME" =~ ^[A-Za-z0-9_.@:-]+$ ]] || die "Invalid --service-name: $SERVICE_NAME"
case "$DISCOVERY_POLICY" in running-only|running-with-offline-inventory|filesystem-only) ;; *) die "Invalid --discovery-policy: $DISCOVERY_POLICY" ;; esac

case "$LOG_LEVEL" in debug|info|warn|error) ;; *) die "Invalid --log-level: $LOG_LEVEL" ;; esac

for path in "$INSTALL_DIR" "$CONFIG_DIR" "$STATE_DIR"; do
  [[ "$path" == /* ]] || die "Installation paths must be absolute: $path"
done

if [[ -n "$KAFKA_CLIENT_PROPERTIES" ]]; then
  [[ "$KAFKA_CLIENT_PROPERTIES" == /* ]] || \
    die "--kafka-client-properties must be an absolute path: $KAFKA_CLIENT_PROPERTIES"
  [[ -f "$KAFKA_CLIENT_PROPERTIES" ]] || \
    die "Kafka client properties file not found: $KAFKA_CLIENT_PROPERTIES"
fi

for pair in \
  "HOST_ID:$HOST_ID" \
  "AGENT_NAME:$AGENT_NAME" \
  "NODE_NAME:$NODE_NAME" \
  "CLUSTER_NAME:$CLUSTER_NAME" \
  "ENVIRONMENT:$ENVIRONMENT" \
  "SCAN_PATHS:$SCAN_PATHS" \
  "JMX_METRICS_URL:$JMX_METRICS_URL" \
  "KAFKA_EXPORTER_METRICS_URL:$KAFKA_EXPORTER_METRICS_URL" \
  "AUTH_USERNAME:$AUTH_USERNAME"; do
  validate_single_line "${pair%%:*}" "${pair#*:}"
done

if [[ "$ALLOW_SERVICE_RESTART" == "true" ]]; then
  [[ -n "$ALLOWED_SERVICES" ]] || die "--enable-service-restart requires --allowed-services."
  ALLOWED_SERVICES="$(normalize_service_csv "$ALLOWED_SERVICES")"
  [[ -n "$ALLOWED_SERVICES" ]] || die "No valid services were supplied in --allowed-services."
fi

if [[ "$RESTART_WITH_SUDO" == "true" && "$ALLOW_SERVICE_RESTART" != "true" ]]; then
  die "--restart-with-sudo requires --enable-service-restart."
fi

for secret_file in "$TOKEN_FILE_SOURCE" "$PASSWORD_FILE_SOURCE"; do
  [[ -z "$secret_file" || -f "$secret_file" ]] || die "Secret file not found: $secret_file"
done

# -----------------------------------------------------------------------------
# Resolve the correct offline binary.
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ -z "$BINARY_SOURCE" ]]; then
  case "$(uname -m)" in
    x86_64|amd64)
      BINARY_SOURCE="$SCRIPT_DIR/tantor-discovery-agent-linux-amd64"
      ;;
    aarch64|arm64)
      BINARY_SOURCE="$SCRIPT_DIR/tantor-discovery-agent-linux-arm64"
      ;;
    *) die "Unsupported CPU architecture: $(uname -m)" ;;
  esac
fi

[[ -f "$BINARY_SOURCE" ]] || die "Agent binary not found: $BINARY_SOURCE"
log "Selected agent binary for architecture $(uname -m): $BINARY_SOURCE"

# -----------------------------------------------------------------------------
# Optional connectivity precheck. No internet access is used.
# -----------------------------------------------------------------------------
check_backend_tcp() {
  local host="$1"
  local port="$2"

  if command -v timeout >/dev/null 2>&1; then
    if HOST="$host" PORT="$port" timeout 3 bash -c 'exec 3<>"/dev/tcp/${HOST}/${PORT}"' >/dev/null 2>&1; then
      log "Backend TCP connectivity check passed: $host:$port"
      return 0
    fi
  else
    warn "'timeout' command is unavailable; skipping TCP connectivity precheck."
    if [[ "$REQUIRE_SERVER_REACHABLE" == "true" ]]; then
      die "Cannot enforce --require-server-reachable because the timeout command is unavailable."
    fi
    return 0
  fi

  if [[ "$REQUIRE_SERVER_REACHABLE" == "true" ]]; then
    die "Cannot connect to backend $host:$port from this VM. Check route/firewall/backend listener."
  fi
  warn "Backend $host:$port is not reachable right now. Installation will continue; the agent will retry at runtime."
}

check_backend_tcp "$SERVER_HOST" "$SERVER_PORT"

check_jmx_exporter() {
  local endpoint="$1"
  [[ -n "$endpoint" ]] || {
    log "JMX Exporter not configured; continuing in Kafka-only mode."
    return 0
  }
  command -v curl >/dev/null 2>&1 || die "curl is required to validate the configured JMX Exporter endpoint."
  local body
  body="$(mktemp)"
  if ! curl --silent --show-error --fail --location --max-time 5 --output "$body" "$endpoint"; then
    rm -f "$body"
    die "Configured JMX Exporter is not reachable at $endpoint. Correct the IP/port or omit both JMX options."
  fi
  if ! grep -Eq '^[A-Za-z_:][A-Za-z0-9_:]*(\{[^}]*\})?[[:space:]]+[-+0-9.eENaInf]+' "$body"; then
    rm -f "$body"
    die "The endpoint $endpoint responded, but it did not return Prometheus/JMX Exporter metrics."
  fi
  rm -f "$body"
  log "JMX Exporter validation passed: $endpoint"
}

check_jmx_exporter "$JMX_METRICS_URL"
check_jmx_exporter "$CONTROLLER_JMX_METRICS_URL"

check_kafka_exporter() {
  local endpoint="$1"
  [[ -n "$endpoint" ]] || {
    log "Kafka Exporter not configured; consumer-group/topic exporter metrics will be unavailable."
    return 0
  }
  command -v curl >/dev/null 2>&1 || die "curl is required to validate the configured Kafka Exporter endpoint."
  local body
  body="$(mktemp)"
  if ! curl --silent --show-error --fail --location --max-time 5 --output "$body" "$endpoint"; then
    rm -f "$body"
    die "Configured Kafka Exporter is not reachable at $endpoint. Correct the IP/port or omit its options."
  fi
  if ! grep -Eq '^kafka_(brokers|topic_|consumergroup_|consumer_group_)' "$body"; then
    rm -f "$body"
    die "The endpoint $endpoint responded, but expected Kafka Exporter metric families were not found."
  fi
  rm -f "$body"
  log "Kafka Exporter validation passed: $endpoint"
}

check_kafka_exporter "$KAFKA_EXPORTER_METRICS_URL"

log "Agent node identity: $NODE_NAME"

# -----------------------------------------------------------------------------
# Create/reuse the limited local Linux service account.
# -----------------------------------------------------------------------------
if ! getent group "$RUN_GROUP" >/dev/null 2>&1; then
  [[ "$CREATE_RUNTIME_USER" == "true" ]] || die "Runtime group $RUN_GROUP does not exist. Pre-provision it or use --create-runtime-user."
  log "Creating system group: $RUN_GROUP"
  groupadd --system "$RUN_GROUP"
fi

if ! id "$RUN_USER" >/dev/null 2>&1; then
  [[ "$CREATE_RUNTIME_USER" == "true" ]] || die "Runtime user $RUN_USER does not exist. Pre-provision it or use --create-runtime-user."
  log "Creating limited system user: $RUN_USER"
  NOLOGIN_SHELL="$(command -v nologin 2>/dev/null || true)"
  [[ -n "$NOLOGIN_SHELL" ]] || NOLOGIN_SHELL="/sbin/nologin"
  useradd \
    --system \
    --gid "$RUN_GROUP" \
    --home-dir "$STATE_DIR" \
    --create-home \
    --shell "$NOLOGIN_SHELL" \
    "$RUN_USER"
else
  log "Reusing existing Linux user: $RUN_USER"
fi

_actual_group="$(id -gn "$RUN_USER")"
if [[ "$_actual_group" != "$RUN_GROUP" ]]; then
  # This is valid: systemd explicitly starts the process with User= and Group=.
  # Do not reject an existing client user merely because its login primary group
  # differs from the group selected for this service.
  warn "Runtime user $RUN_USER has primary login group $_actual_group; the service will explicitly run with Group=$RUN_GROUP."
fi
_user_home="$(getent passwd "$RUN_USER" | awk -F: '{print $6}')"
_user_shell="$(getent passwd "$RUN_USER" | awk -F: '{print $7}')"
[[ -n "$_user_home" && "$_user_home" != "/" ]] || die "Runtime user $RUN_USER has an unsuitable home directory."
[[ "$_user_shell" != "/bin/bash" && "$_user_shell" != "/bin/sh" ]] || warn "Runtime user $RUN_USER has an interactive shell ($_user_shell); confirm this is client-approved."

# Automatically discover the Kafka service/config groups, add the selected
# runtime user to them, and verify exact Kafka config readability. This makes
# --run-user portable without requiring a separate manual usermod or setfacl step.
RUNNING_KAFKA_CONFIGS=()
SCAN_KAFKA_CONFIGS=()
case "$DISCOVERY_POLICY" in
  running-only)
    discover_running_kafka_configs
    detect_kafka_service_groups
    ;;
  running-with-offline-inventory)
    discover_running_kafka_configs
    discover_scan_kafka_configs
    detect_kafka_service_groups
    ;;
  filesystem-only)
    discover_scan_kafka_configs
    ;;
esac
detect_kafka_file_groups
apply_kafka_group_membership

if command -v runuser >/dev/null 2>&1; then
  verify_kafka_config_access
else
  die "Required command not found: runuser. It is needed to verify Kafka access for $RUN_USER."
fi

# RHEL may mount /proc with hidepid. That can restrict process discovery when Kafka
# runs as a different OS user. We do not change the mount option automatically.
if command -v findmnt >/dev/null 2>&1; then
  PROC_OPTIONS="$(findmnt -n -o OPTIONS /proc 2>/dev/null || true)"
  if [[ "$PROC_OPTIONS" == *"hidepid=1"* || "$PROC_OPTIONS" == *"hidepid=2"* ]]; then
    warn "/proc is mounted with restricted process visibility ($PROC_OPTIONS). Kafka process discovery may be limited for $RUN_USER."
  fi
fi

# Check only existing scan roots. Missing paths are allowed because the same bundle
# is reusable across clients with different Kafka layouts. Normally the automatic
# Kafka-group assignment above provides access. If the root itself is still not
# traversable, grant a minimal ACL to the exact root and its parent path.
if command -v runuser >/dev/null 2>&1; then
  IFS=',' read -r -a _scan_roots <<< "$SCAN_PATHS"
  for root in "${_scan_roots[@]}"; do
    root="$(trim_whitespace "$root")"
    [[ -z "$root" || ! -e "$root" ]] && continue

    if runuser -u "$RUN_USER" -- test -r "$root" 2>/dev/null &&
       runuser -u "$RUN_USER" -- test -x "$root" 2>/dev/null; then
      log "Runtime user can read and traverse Kafka scan path: $root"
      continue
    fi

    warn "Runtime user $RUN_USER cannot traverse Kafka scan path $root after Kafka-group assignment."
    if command -v setfacl >/dev/null 2>&1 && command -v getfacl >/dev/null 2>&1; then
      _resolved_root="$(readlink -m -- "$root")"
      _root_parent="$(dirname -- "$_resolved_root")"
      _root_parents=()
      while [[ "$_root_parent" != "/" && -n "$_root_parent" ]]; do
        _root_parents+=("$_root_parent")
        _root_parent="$(dirname -- "$_root_parent")"
      done
      for (( _idx=${#_root_parents[@]}-1; _idx>=0; _idx-- )); do
        ensure_user_acl_permissions "${_root_parents[$_idx]}" "--x" || \
          die "Failed to grant traverse ACL on ${_root_parents[$_idx]} for $RUN_USER."
      done
      ensure_user_acl_permissions "$_resolved_root" "r-x" || \
        die "Failed to grant scan-root ACL on $_resolved_root for $RUN_USER."
    else
      die "Kafka scan path $root is not traversable by $RUN_USER and setfacl is unavailable. Install the RHEL acl package offline or align the Kafka directory group."
    fi

    runuser -u "$RUN_USER" -- test -r "$root" 2>/dev/null &&
      runuser -u "$RUN_USER" -- test -x "$root" 2>/dev/null || \
      die "Kafka scan path remains inaccessible to $RUN_USER after automatic access configuration: $root"
  done
fi

# Run after group/ACL setup so the real service identity is tested with exactly
# the Kafka filesystem access the installed agent will have.
verify_kafka_protocol_connectivity

# -----------------------------------------------------------------------------
# Install binary and directories. Existing installation is updated in place.
# -----------------------------------------------------------------------------
install -d -o root -g root -m 0755 "$INSTALL_DIR"
install -d -o root -g "$RUN_GROUP" -m 0750 "$CONFIG_DIR"
install -d -o "$RUN_USER" -g "$RUN_GROUP" -m 0750 "$STATE_DIR"
install -d -o "$RUN_USER" -g "$RUN_GROUP" -m 0750 "$STATE_DIR/backups"
install -d -o root -g root -m 0755 "$STATE_DIR/install-backups"

TIMESTAMP="$(date +%Y%m%d%H%M%S)"
if [[ -f "$INSTALL_DIR/tantor-discovery-agent" ]]; then
  cp -p "$INSTALL_DIR/tantor-discovery-agent" "$STATE_DIR/install-backups/tantor-discovery-agent.$TIMESTAMP" || true
fi
if [[ -f "$CONFIG_DIR/agent.env" ]]; then
  cp -p "$CONFIG_DIR/agent.env" "$STATE_DIR/install-backups/agent.env.$TIMESTAMP" || true
fi

install -o root -g root -m 0755 "$BINARY_SOURCE" "$INSTALL_DIR/tantor-discovery-agent"

# Persist agent stdout/stderr in a protected file. The directory is not writable
# by the runtime user, while the existing regular log file remains appendable by
# that user. Refuse symlinks and non-regular files before changing ownership.
install -d -o root -g "$RUN_GROUP" -m 0750 "$LOG_DIR"
if [[ -L "$LOG_FILE" || ( -e "$LOG_FILE" && ! -f "$LOG_FILE" ) ]]; then
  die "Refusing unsafe agent log path: $LOG_FILE must be a regular file."
fi
if [[ ! -e "$LOG_FILE" ]]; then
  install -o "$RUN_USER" -g "$RUN_GROUP" -m 0640 /dev/null "$LOG_FILE"
else
  chown "$RUN_USER:$RUN_GROUP" "$LOG_FILE"
  chmod 0640 "$LOG_FILE"
fi

# StandardOutput=append: was added after the systemd version used by a baseline
# RHEL 8 installation. Use it when supported; otherwise use a root-owned runner
# that preserves append semantics without weakening the service identity.
AGENT_EXEC_START="$INSTALL_DIR/tantor-discovery-agent"
PERSISTENT_LOGGING_LINES=""
RUNNER_FILE="$INSTALL_DIR/run-agent.sh"
if [[ "$SYSTEMD_VERSION" =~ ^[0-9]+$ ]] && (( SYSTEMD_VERSION >= 240 )); then
  PERSISTENT_LOGGING_LINES="StandardOutput=append:$LOG_FILE
StandardError=append:$LOG_FILE"
  rm -f "$RUNNER_FILE"
else
  RUNNER_FILE_TMP="$(mktemp)"
  cat > "$RUNNER_FILE_TMP" <<RUNNER
#!/bin/bash
exec "$INSTALL_DIR/tantor-discovery-agent" >>"$LOG_FILE" 2>&1
RUNNER
  install -o root -g root -m 0755 "$RUNNER_FILE_TMP" "$RUNNER_FILE"
  rm -f "$RUNNER_FILE_TMP"
  AGENT_EXEC_START="$RUNNER_FILE"
  warn "systemd $SYSTEMD_VERSION does not provide StandardOutput=append; using the RHEL 8 compatible logging runner."
fi

# Preserve RHEL SELinux policy. We never disable or relax SELinux. restorecon applies
# the distribution's default contexts when the command is available.
if command -v restorecon >/dev/null 2>&1; then
  restorecon -RF "$INSTALL_DIR" "$CONFIG_DIR" "$STATE_DIR" "$LOG_DIR" >/dev/null 2>&1 || \
    warn "restorecon could not fully relabel agent directories. Check SELinux labels if the service is denied."
fi

# -----------------------------------------------------------------------------
# Copy optional backend credentials into protected local files.
# -----------------------------------------------------------------------------
TOKEN_FILE_DEST=""
PASSWORD_FILE_DEST=""

if [[ "$AUTH_TYPE" == "bearer" ]]; then
  TOKEN_FILE_DEST="$CONFIG_DIR/auth.token"
  install -o root -g "$RUN_GROUP" -m 0640 "$TOKEN_FILE_SOURCE" "$TOKEN_FILE_DEST"
else
  rm -f "$CONFIG_DIR/auth.token"
fi

if [[ "$AUTH_TYPE" == "basic" ]]; then
  PASSWORD_FILE_DEST="$CONFIG_DIR/auth.password"
  install -o root -g "$RUN_GROUP" -m 0640 "$PASSWORD_FILE_SOURCE" "$PASSWORD_FILE_DEST"
else
  rm -f "$CONFIG_DIR/auth.password"
fi

# -----------------------------------------------------------------------------
# Write systemd EnvironmentFile. Secrets themselves are never written here.
# -----------------------------------------------------------------------------
escape_env() {
  local value="$1"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  printf '"%s"' "$value"
}

ENV_FILE="$CONFIG_DIR/agent.env"
ENV_FILE_TMP="$(mktemp "$CONFIG_DIR/.agent.env.XXXXXX")"
trap 'rm -f "${ENV_FILE_TMP:-}"' EXIT

{
  echo "TANTOR_AGENT_SERVER_URL=$(escape_env "$SERVER_URL")"
  echo "TANTOR_AGENT_HOST_ID=$(escape_env "$HOST_ID")"
  echo "TANTOR_AGENT_NAME=$(escape_env "$AGENT_NAME")"
  echo "TANTOR_AGENT_NODE_NAME=$(escape_env "$NODE_NAME")"
  echo "TANTOR_AGENT_CLUSTER_NAME=$(escape_env "$CLUSTER_NAME")"
  echo "TANTOR_AGENT_ENVIRONMENT=$(escape_env "$ENVIRONMENT")"
  echo "TANTOR_AGENT_SCAN_PATHS=$(escape_env "$SCAN_PATHS")"
  echo "TANTOR_AGENT_DISCOVERY_POLICY=$(escape_env "$DISCOVERY_POLICY")"

  echo "TANTOR_AGENT_DISCOVERY_INTERVAL=$(escape_env "$DISCOVERY_INTERVAL")"
  echo "TANTOR_AGENT_TASK_POLL_INTERVAL=$(escape_env "$TASK_POLL_INTERVAL")"
  echo "TANTOR_AGENT_METRICS_INTERVAL=$(escape_env "$METRICS_INTERVAL")"
  echo "TANTOR_AGENT_HTTP_TIMEOUT=$(escape_env "$HTTP_TIMEOUT")"
  echo "TANTOR_AGENT_HTTP_RETRIES=$(escape_env "$HTTP_RETRIES")"
  echo "TANTOR_AGENT_JMX_METRICS_URL=$(escape_env "$JMX_METRICS_URL")"
  echo "TANTOR_AGENT_CONTROLLER_JMX_METRICS_URL=$(escape_env "$CONTROLLER_JMX_METRICS_URL")"
  echo "TANTOR_AGENT_KAFKA_EXPORTER_METRICS_URL=$(escape_env "$KAFKA_EXPORTER_METRICS_URL")"
  echo "TANTOR_AGENT_KAFKA_CONNECT_TIMEOUT=$(escape_env "$KAFKA_CONNECT_TIMEOUT")"

  echo "TANTOR_AGENT_AUTH_TYPE=$(escape_env "$AUTH_TYPE")"
  echo "TANTOR_AGENT_AUTH_USERNAME=$(escape_env "$AUTH_USERNAME")"
  echo "TANTOR_AGENT_AUTH_TOKEN_FILE=$(escape_env "$TOKEN_FILE_DEST")"
  echo "TANTOR_AGENT_AUTH_PASSWORD_FILE=$(escape_env "$PASSWORD_FILE_DEST")"

  # Explicitly keep all TLS-related runtime settings empty/disabled for this build.
  echo 'TANTOR_AGENT_CA_FILE=""'
  echo 'TANTOR_AGENT_CLIENT_CERT=""'
  echo 'TANTOR_AGENT_CLIENT_KEY=""'
  echo 'TANTOR_AGENT_INSECURE_SKIP_VERIFY="false"'

  echo "TANTOR_AGENT_BACKUP_ROOT=$(escape_env "$STATE_DIR/backups")"
  echo "TANTOR_AGENT_ENABLE_TASKS=$(escape_env "$ENABLE_TASKS")"
  echo "TANTOR_AGENT_ALLOW_CONFIG_WRITE=$(escape_env "$ALLOW_CONFIG_WRITE")"
  echo "TANTOR_AGENT_ALLOW_SERVICE_RESTART=$(escape_env "$ALLOW_SERVICE_RESTART")"
  echo "TANTOR_AGENT_RESTART_WITH_SUDO=$(escape_env "$RESTART_WITH_SUDO")"
  echo "TANTOR_AGENT_ALLOWED_SERVICES=$(escape_env "$ALLOWED_SERVICES")"
  echo "TANTOR_AGENT_RUN_PRECHECK=$(escape_env "$RUN_PRECHECK")"
  echo "TANTOR_AGENT_LOG_LEVEL=$(escape_env "$LOG_LEVEL")"
} > "$ENV_FILE_TMP"

chown root:"$RUN_GROUP" "$ENV_FILE_TMP"
chmod 0640 "$ENV_FILE_TMP"
mv -f "$ENV_FILE_TMP" "$ENV_FILE"
trap - EXIT

# -----------------------------------------------------------------------------
# Optional exact-command sudoers rule for Kafka service restart.
# -----------------------------------------------------------------------------
SUDOERS_FILE="/etc/sudoers.d/${SERVICE_NAME}"
if [[ "$ALLOW_SERVICE_RESTART" == "true" && "$RESTART_WITH_SUDO" == "true" ]]; then
  command -v sudo >/dev/null 2>&1 || die "sudo is required for --restart-with-sudo."
  command -v visudo >/dev/null 2>&1 || die "visudo is required to validate the sudoers rule."

  SYSTEMCTL_PATH="$(command -v systemctl)"
  IFS=',' read -r -a _services <<< "$ALLOWED_SERVICES"
  SUDO_COMMANDS=""

  for service in "${_services[@]}"; do
    [[ -n "$SUDO_COMMANDS" ]] && SUDO_COMMANDS+=", "
    SUDO_COMMANDS+="$SYSTEMCTL_PATH restart $service"
  done

  TMP_SUDOERS="$(mktemp)"
  printf '%s ALL=(root) NOPASSWD: %s\n' "$RUN_USER" "$SUDO_COMMANDS" > "$TMP_SUDOERS"
  visudo -cf "$TMP_SUDOERS" >/dev/null || {
    rm -f "$TMP_SUDOERS"
    die "Generated sudoers rule failed validation."
  }
  install -o root -g root -m 0440 "$TMP_SUDOERS" "$SUDOERS_FILE"
  rm -f "$TMP_SUDOERS"
else
  rm -f "$SUDOERS_FILE"
fi

# -----------------------------------------------------------------------------
# Install hardened systemd service.
# -----------------------------------------------------------------------------
NO_NEW_PRIVILEGES="true"
CAPABILITY_LINES=$'CapabilityBoundingSet=\nAmbientCapabilities='
if [[ "$RESTART_WITH_SUDO" == "true" ]]; then
  # sudo requires privilege elevation; it is still constrained by the exact sudoers rule.
  NO_NEW_PRIVILEGES="false"
  CAPABILITY_LINES=""
fi

UNIT_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
UNIT_FILE_TMP="$(mktemp)"
cat > "$UNIT_FILE_TMP" <<UNIT
[Unit]
Description=Tantor Kafka Discovery Agent
Wants=network-online.target
After=network-online.target
StartLimitIntervalSec=60
StartLimitBurst=5

[Service]
Type=simple
User=$RUN_USER
Group=$RUN_GROUP
${KAFKA_SUPPLEMENTARY_GROUPS:+SupplementaryGroups=$KAFKA_SUPPLEMENTARY_GROUPS}
WorkingDirectory=$STATE_DIR
EnvironmentFile=$ENV_FILE
ExecStart=$AGENT_EXEC_START
$PERSISTENT_LOGGING_LINES
Restart=always
RestartSec=5s
TimeoutStopSec=20s
UMask=0027

# Hardening directives below are supported by the systemd versions shipped with
# both RHEL 8 and RHEL 9. We intentionally avoid newer directives that can produce
# unknown-setting warnings on the older RHEL 8 systemd release.
NoNewPrivileges=$NO_NEW_PRIVILEGES
PrivateTmp=true
PrivateDevices=true
ProtectKernelTunables=true
ProtectKernelModules=true
ProtectControlGroups=true
RestrictAddressFamilies=AF_UNIX AF_INET AF_INET6
SystemCallArchitectures=native
$CAPABILITY_LINES

[Install]
WantedBy=multi-user.target
UNIT

install -o root -g root -m 0644 "$UNIT_FILE_TMP" "$UNIT_FILE"
rm -f "$UNIT_FILE_TMP"

if command -v restorecon >/dev/null 2>&1; then
  restorecon -F "$UNIT_FILE" >/dev/null 2>&1 || true
  [[ -f "$SUDOERS_FILE" ]] && restorecon -F "$SUDOERS_FILE" >/dev/null 2>&1 || true
fi

# Validate the generated unit before activation when systemd-analyze is present.
if command -v systemd-analyze >/dev/null 2>&1; then
  if ! systemd-analyze verify "$UNIT_FILE" >/dev/null 2>&1; then
    warn "systemd-analyze reported a problem with $UNIT_FILE. Showing validation output:"
    systemd-analyze verify "$UNIT_FILE" || true
    die "Generated systemd service validation failed."
  fi
fi

# -----------------------------------------------------------------------------
# Activate service.
# -----------------------------------------------------------------------------
systemctl daemon-reload
systemctl enable "$SERVICE_NAME.service" >/dev/null
systemctl restart "$SERVICE_NAME.service"
_service_active="false"
_stable_checks=0
for _attempt in 1 2 3 4 5 6 7 8 9 10; do
  sleep 1
  if systemctl is-active --quiet "$SERVICE_NAME.service"; then
    _stable_checks=$((_stable_checks + 1))
    if (( _stable_checks >= 3 )); then
      _service_active="true"
      break
    fi
  else
    _stable_checks=0
  fi
done

if [[ "$_service_active" != "true" ]]; then
  printf '\n[ERROR] Agent service failed to start.\n' >&2
  systemctl status "$SERVICE_NAME.service" --no-pager || true
  journalctl -u "$SERVICE_NAME.service" -n 100 --no-pager || true
  exit 1
fi

# -----------------------------------------------------------------------------
# Final summary.
# -----------------------------------------------------------------------------
printf '\n============================================================\n'
printf 'Tantor discovery agent installed successfully\n'
printf '============================================================\n'
printf 'RHEL version    : %s\n' "$RHEL_VERSION"
printf 'systemd version : %s\n' "$SYSTEMD_VERSION"
printf 'SELinux mode    : %s\n' "$SELINUX_MODE"
printf 'CPU architecture: %s\n' "$(uname -m)"
printf 'Backend URL     : %s\n' "$SERVER_URL"
printf 'Transport       : HTTP only\n'
printf 'Runtime user    : %s\n' "$RUN_USER"
printf 'Runtime group   : %s\n' "$RUN_GROUP"
printf 'Kafka groups    : %s\n' "${KAFKA_ACCESS_GROUPS:-none detected}"
printf 'Node identity   : %s\n' "$NODE_NAME"
printf 'Kafka check     : Kafka ApiVersions protocol validation enabled\n'
if [[ -n "$JMX_METRICS_URL" ]]; then
  printf 'Broker JMX     : enabled (%s)\n' "$JMX_METRICS_URL"
else
  printf 'Broker JMX     : disabled\n'
fi
if [[ -n "$CONTROLLER_JMX_METRICS_URL" ]]; then
  printf 'Controller JMX : enabled (%s)\n' "$CONTROLLER_JMX_METRICS_URL"
else
  printf 'Controller JMX : disabled\n'
fi
if [[ -n "$KAFKA_EXPORTER_METRICS_URL" ]]; then
  printf 'Kafka Exporter : enabled (%s)\n' "$KAFKA_EXPORTER_METRICS_URL"
else
  printf 'Kafka Exporter : disabled\n'
fi
printf 'Service         : %s.service\n' "$SERVICE_NAME"
printf 'Binary          : %s/tantor-discovery-agent\n' "$INSTALL_DIR"
printf 'Configuration   : %s\n' "$ENV_FILE"
printf 'State directory : %s\n' "$STATE_DIR"
printf 'Log file        : %s\n' "$LOG_FILE"
printf 'Kafka scan paths: %s\n' "$SCAN_PATHS"
printf 'Auth type       : %s\n' "$AUTH_TYPE"
printf 'Config write    : %s\n' "$ALLOW_CONFIG_WRITE"
printf 'Service restart : %s\n' "$ALLOW_SERVICE_RESTART"
printf '\nUseful commands:\n'
printf '  systemctl status %s --no-pager\n' "$SERVICE_NAME"
printf '  journalctl -u %s -f\n' "$SERVICE_NAME"
printf '  tail -f %s\n' "$LOG_FILE"
printf '  sudo systemctl restart %s\n' "$SERVICE_NAME"
printf '\n'

if [[ "$ALLOW_CONFIG_WRITE" == "true" ]]; then
  warn "Config write is enabled. $RUN_USER must still have OS and SELinux permission to the discovered Kafka config file."
fi
if [[ "$SELINUX_MODE" == "Enforcing" ]]; then
  log "SELinux remains Enforcing. The installer did not disable or weaken it."
fi
log "No firewalld rule was added. This agent initiates an outbound connection to $SERVER_HOST:$SERVER_PORT."
if [[ "$ALLOW_SERVICE_RESTART" == "true" && "$RESTART_WITH_SUDO" != "true" ]]; then
  warn "Service restart is enabled without sudo. $RUN_USER must already have permission to restart the allowlisted units."
fi

exit 0
