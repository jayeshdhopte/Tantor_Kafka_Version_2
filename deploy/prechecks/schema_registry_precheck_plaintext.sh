#!/usr/bin/env bash
# -----------------------------------------------------------------
# Schema Registry Pre-check Script (plaintext, no SASL/TLS)
# Run this on the target SR host before triggering SR deployment
# -----------------------------------------------------------------

# ---------- Configurable inputs (control plane should inject these) ----------
BOOTSTRAP_BROKER="${BOOTSTRAP_BROKER:-localhost:9092}"     # host:port of any broker (PLAINTEXT listener)
SR_REST_PORT="${SR_REST_PORT:-8081}"
KAFKA_BIN_DIR="${KAFKA_BIN_DIR:-/opt_apb/kafka/bin}"
SCHEMAS_TOPIC="${SCHEMAS_TOPIC:-_schemas}"
SR_HA_PEERS="${SR_HA_PEERS:-}"                              # comma-separated host:port list, optional

# Colors
RED="\e[31m"
GREEN="\e[32m"
YELLOW="\e[33m"
RESET="\e[0m"

FAIL_COUNT=0

echo -e "\n===== Schema Registry Pre-check (Plaintext) ====="

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

# ---------- 5. DNS Resolution of broker host ----------
if getent hosts "$broker_host" >/dev/null 2>&1 || [[ "$broker_host" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo -e "DNS resolution ($broker_host): ${GREEN}[Pass]${RESET}"
else
    echo -e "DNS resolution ($broker_host): ${RED}[Fail, cannot resolve — check /etc/hosts or internal DNS]${RESET}"
    ((FAIL_COUNT++))
fi

# ---------- 6. SR REST Port availability (must be free before install) ----------
if ss -ltn 2>/dev/null | awk '{print $4}' | grep -q ":${SR_REST_PORT}\$"; then
    echo -e "SR REST port ($SR_REST_PORT): ${RED}[Fail, port already in use]${RESET}"
    ((FAIL_COUNT++))
else
    echo -e "SR REST port ($SR_REST_PORT): Free ${GREEN}[Pass]${RESET}"
fi

# ---------- 7. _schemas topic pre-existing config check (best-effort, non-fatal) ----------
if [[ -x "${KAFKA_BIN_DIR}/kafka-topics.sh" ]]; then
    if "${KAFKA_BIN_DIR}/kafka-topics.sh" --bootstrap-server "$BOOTSTRAP_BROKER" --describe --topic "$SCHEMAS_TOPIC" >/tmp/schemas_topic_desc 2>/dev/null; then
        if grep -q "cleanup.policy=compact" /tmp/schemas_topic_desc; then
            echo -e "_schemas topic config: ${GREEN}[Pass, exists with compact policy]${RESET}"
        else
            echo -e "_schemas topic config: ${YELLOW}[Warn, exists but cleanup.policy != compact — verify manually]${RESET}"
        fi
    else
        echo -e "_schemas topic: ${YELLOW}[Info, does not exist yet — will be auto-created, verify RF/partitions post-install]${RESET}"
    fi
else
    echo -e "_schemas topic check: ${YELLOW}[Skipped, kafka-topics.sh not found at $KAFKA_BIN_DIR]${RESET}"
fi

# ---------- 8. HA peer reachability (optional, for multi-node SR) ----------
if [[ -n "$SR_HA_PEERS" ]]; then
    IFS=',' read -ra PEERS <<< "$SR_HA_PEERS"
    for peer in "${PEERS[@]}"; do
        p_host="${peer%%:*}"
        p_port="${peer##*:}"
        if timeout 5 bash -c "cat < /dev/null > /dev/tcp/${p_host}/${p_port}" 2>/dev/null; then
            echo -e "SR peer reachability ($peer): ${GREEN}[Pass]${RESET}"
        else
            echo -e "SR peer reachability ($peer): ${RED}[Fail, unreachable]${RESET}"
            ((FAIL_COUNT++))
        fi
    done
fi

echo -e "===== Schema Registry Pre-check Completed ====="

if [[ "$FAIL_COUNT" -gt 0 ]]; then
    echo -e "${RED}Result: $FAIL_COUNT check(s) failed. Not ready for SR deployment.${RESET}"
    exit 1
else
    echo -e "${GREEN}Result: All checks passed. Ready for SR deployment.${RESET}"
    exit 0
fi
