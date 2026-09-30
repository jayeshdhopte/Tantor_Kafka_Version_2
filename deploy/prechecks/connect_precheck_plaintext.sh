#!/usr/bin/env bash
# -----------------------------------------------------------------
# Kafka Connect Pre-check Script (plaintext, no SASL/TLS)
# Run this on the target Connect worker host before triggering
# Connect deployment
# -----------------------------------------------------------------

# ---------- Configurable inputs (control plane should inject these) ----------
BOOTSTRAP_BROKER="${BOOTSTRAP_BROKER:-localhost:9092}"     # host:port of any broker (PLAINTEXT listener)
CONNECT_REST_PORT="${CONNECT_REST_PORT:-8083}"
PLUGIN_PATH="${PLUGIN_PATH:-/opt_apb/kafka-connect/plugins}"
KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-/opt_apb/kafka/bin}"
MIN_FREE_DISK_MB="${MIN_FREE_DISK_MB:-5120}"                # 5GB default for connector staging dir
CONNECT_DATA_DIR="${CONNECT_DATA_DIR:-/opt_apb/kafka-connect}"

# Colors
RED="\e[31m"
GREEN="\e[32m"
YELLOW="\e[33m"
RESET="\e[0m"

FAIL_COUNT=0

echo -e "\n===== Kafka Connect Pre-check (Plaintext) ====="

# ---------- 1. Open File Limit ----------
soft_limit=$(ulimit -Sn)
hard_limit=$(ulimit -Hn)
if [[ "$soft_limit" -ge 1024000 && "$hard_limit" -ge 1024000 ]]; then
    echo -e "Open file limit (soft/hard): $soft_limit/$hard_limit ${GREEN}[Pass]${RESET}"
else
    echo -e "Open file limit (soft/hard): $soft_limit/$hard_limit ${RED}[Fail, must be 1024000]${RESET}"
    ((FAIL_COUNT++))
fi

# ---------- 2. JDK Version ----------
if command -v java >/dev/null 2>&1; then
    java_version=$(java -version 2>&1 | head -n 1 | awk -F '"' '{print $2}')
    if [[ "$java_version" == 17.* ]]; then
        echo -e "Java Version: $java_version ${GREEN}[Pass]${RESET}"
    else
        echo -e "Java Version: $java_version ${RED}[Fail, must be 17.x]${RESET}"
        ((FAIL_COUNT++))
    fi
else
    echo -e "Java not found ${RED}[Fail, must be installed with version 17.x]${RESET}"
    ((FAIL_COUNT++))
fi

# ---------- 3. NTP Service ----------
if systemctl is-active --quiet ntpd; then
    echo -e "NTP Service (ntpd): Active ${GREEN}[Pass]${RESET}"
elif systemctl is-active --quiet chronyd; then
    echo -e "NTP Service (chronyd): Active ${GREEN}[Pass]${RESET}"
else
    echo -e "NTP Service: Not running ${RED}[Fail, must have ntpd or chronyd active]${RESET}"
    ((FAIL_COUNT++))
fi

# ---------- 4. Broker Reachability (PLAINTEXT listener) ----------
broker_host="${BOOTSTRAP_BROKER%%:*}"
broker_port="${BOOTSTRAP_BROKER##*:}"
if timeout 5 bash -c "cat < /dev/null > /dev/tcp/${broker_host}/${broker_port}" 2>/dev/null; then
    echo -e "Broker reachability ($BOOTSTRAP_BROKER): ${GREEN}[Pass]${RESET}"
else
    echo -e "Broker reachability ($BOOTSTRAP_BROKER): ${RED}[Fail, port unreachable]${RESET}"
    ((FAIL_COUNT++))
fi

# ---------- 5. Plugin path staged (air-gapped: no Confluent Hub pulls) ----------
if [[ -d "$PLUGIN_PATH" ]]; then
    plugin_count=$(find "$PLUGIN_PATH" -mindepth 1 -maxdepth 2 -name "*.jar" 2>/dev/null | wc -l)
    if [[ "$plugin_count" -gt 0 ]]; then
        echo -e "Plugin path ($PLUGIN_PATH): $plugin_count jar(s) found ${GREEN}[Pass]${RESET}"
    else
        echo -e "Plugin path ($PLUGIN_PATH): exists but empty ${RED}[Fail, stage connector jars before deploy]${RESET}"
        ((FAIL_COUNT++))
    fi
else
    echo -e "Plugin path ($PLUGIN_PATH): ${RED}[Fail, directory not found — pre-stage connector plugins]${RESET}"
    ((FAIL_COUNT++))
fi

# ---------- 6. Connect REST Port availability ----------
if ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":${CONNECT_REST_PORT}\$"; then
    echo -e "Connect REST port ($CONNECT_REST_PORT): ${RED}[Fail, port already in use]${RESET}"
    ((FAIL_COUNT++))
else
    echo -e "Connect REST port ($CONNECT_REST_PORT): Free ${GREEN}[Pass]${RESET}"
fi

# ---------- 7. Disk space for connector working/staging dir ----------
if [[ -d "$CONNECT_DATA_DIR" ]]; then
    free_mb=$(df -Pm "$CONNECT_DATA_DIR" | awk 'NR==2 {print $4}')
else
    free_mb=$(df -Pm "$(dirname "$CONNECT_DATA_DIR")" 2>/dev/null | awk 'NR==2 {print $4}')
fi
if [[ -n "$free_mb" && "$free_mb" -ge "$MIN_FREE_DISK_MB" ]]; then
    echo -e "Disk space ($CONNECT_DATA_DIR): ${free_mb}MB free ${GREEN}[Pass]${RESET}"
else
    echo -e "Disk space ($CONNECT_DATA_DIR): ${free_mb:-unknown}MB free ${RED}[Fail, need >= ${MIN_FREE_DISK_MB}MB]${RESET}"
    ((FAIL_COUNT++))
fi

# ---------- 8. Internal Connect topics pre-existing config check (best-effort, non-fatal) ----------
if [[ -x "${KAFKA_BIN_DIR}/kafka-topics.sh" ]]; then
    for t in connect-configs connect-offsets connect-status; do
        if "${KAFKA_BIN_DIR}/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_BROKER" --describe --topic "$t" >/tmp/connect_topic_desc 2>/dev/null; then
            echo -e "Topic '$t': ${GREEN}[Pass, already pre-created]${RESET}"
        else
            echo -e "Topic '$t': ${YELLOW}[Info, does not exist yet — will be auto-created with default RF/partitions unless pre-created]${RESET}"
        fi
    done
else
    echo -e "Connect internal topics check: ${YELLOW}[Skipped, kafka-topics.sh not found at $KAFKA_BIN_DIR]${RESET}"
fi

echo -e "===== Kafka Connect Pre-check Completed ====="

if [[ "$FAIL_COUNT" -gt 0 ]]; then
    echo -e "${RED}Result: $FAIL_COUNT check(s) failed. Not ready for Connect deployment.${RESET}"
    exit 1
else
    echo -e "${GREEN}Result: All checks passed. Ready for Connect deployment.${RESET}"
    exit 0
fi
