#!/usr/bin/env bash
# Tantor Agent Installer Script
# Target OS: RHEL/CentOS, Ubuntu/Debian
# Run as root

set -Eeuo pipefail

if [ "$EUID" -ne 0 ]; then
  echo "Please run as root"
  exit 1
fi

TANTOR_USER="tantor"
TANTOR_HOME="/srv/tantor"
AGENT_BIN_URL=${AGENT_BIN_URL:-}
SERVER_URL=${SERVER_URL:?SERVER_URL must be an HTTP or HTTPS URL}
TANTOR_ENVIRONMENT=${TANTOR_ENVIRONMENT:?TANTOR_ENVIRONMENT must be development, sit, uat, or production}
AGENT_DATA_DIR="/var/lib/tantor/agent/data"
AGENT_ARTIFACTS_DIR="/var/lib/tantor/agent/artifacts"
AGENT_LOG_DIR="/var/log/tantor-agent"
AGENT_LOG_FILE="$AGENT_LOG_DIR/tantor-agent.log"

echo "=== Tantor Agent Installer ==="

case "$SERVER_URL" in http://*|https://*) ;; *) echo "SERVER_URL must use http:// or https://" >&2; exit 2 ;; esac
case "$TANTOR_ENVIRONMENT" in development|sit|uat|production) ;; *) echo "Invalid TANTOR_ENVIRONMENT" >&2; exit 2 ;; esac
if [ ! -f /srv/tantor-agent ]; then
  case "$AGENT_BIN_URL" in https://*) ;; *) echo "AGENT_BIN_URL must use https:// when no binary is pre-staged" >&2; exit 2 ;; esac
fi

# 1. Create tantor user
if id "$TANTOR_USER" &>/dev/null; then
    echo "User $TANTOR_USER already exists"
else
    useradd -r -m -d $TANTOR_HOME -s /bin/bash $TANTOR_USER
    echo "Created user: $TANTOR_USER"


fi

# Passwordless sudo for tantor (required by architecture)
echo "$TANTOR_USER ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/99-tantor-agent
chmod 0440 /etc/sudoers.d/99-tantor-agent

# 2. Install agent binary
echo "Installing agent binary..."
mkdir -p $TANTOR_HOME/bin
if [ -f "/srv/tantor-agent" ]; then
    echo "Using local binary found at /srv/tantor-agent"
    cp /srv/tantor-agent $TANTOR_HOME/bin/tantor-agent
else
    echo "Downloading agent from $AGENT_BIN_URL..."
    curl --fail --show-error --location --proto '=https' --proto-redir '=https' --tlsv1.2 \
      -o "$TANTOR_HOME/bin/tantor-agent" "$AGENT_BIN_URL"
fi
chmod +x $TANTOR_HOME/bin/tantor-agent
chown -R $TANTOR_USER:$TANTOR_USER $TANTOR_HOME

# Runtime directories used by the agent for downloads, extraction, and logs.
mkdir -p "$AGENT_DATA_DIR" "$AGENT_ARTIFACTS_DIR" "$AGENT_LOG_DIR"
chown -R $TANTOR_USER:$TANTOR_USER /var/lib/tantor "$AGENT_LOG_DIR"
touch "$AGENT_LOG_FILE"
chown $TANTOR_USER:$TANTOR_USER "$AGENT_LOG_FILE"
chmod 0640 "$AGENT_LOG_FILE"

# 3. Configure agent
echo "Setting up agent config..."
mkdir -p /etc/tantor/config

HOST_ID="$(hostname)"

cat <<EOF > /etc/tantor/config/agent.yaml
environment: "$TANTOR_ENVIRONMENT"

agent:
  host_id: "$HOST_ID"
  server_url: "$SERVER_URL"
  poll_interval_seconds: 15
  log_level: "INFO"

paths:
  data_dir: "$AGENT_DATA_DIR"
  log_dir: "$AGENT_LOG_DIR"
  artifacts_dir: "$AGENT_ARTIFACTS_DIR"
EOF

chown -R $TANTOR_USER:$TANTOR_USER /etc/tantor

# 4. Create systemd service
echo "Creating systemd service..."
cat <<EOF > /etc/systemd/system/tantor-agent.service
[Unit]
Description=Tantor Agent
After=network.target

[Service]
Type=simple
User=$TANTOR_USER
Group=$TANTOR_USER
ExecStart=$TANTOR_HOME/bin/tantor-agent -config /etc/tantor/config/agent.yaml
Restart=on-failure
RestartSec=5s
LimitNOFILE=1024000
StandardOutput=append:$AGENT_LOG_FILE
StandardError=append:$AGENT_LOG_FILE

[Install]
WantedBy=multi-user.target
EOF

cat <<EOF > /etc/logrotate.d/tantor-agent
$AGENT_LOG_FILE {
    daily
    rotate 14
    maxsize 50M
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
    create 0640 $TANTOR_USER $TANTOR_USER
}
EOF
chmod 0644 /etc/logrotate.d/tantor-agent

# 5. Start Service & Register Host
echo "Starting Tantor Agent..."
systemctl daemon-reload
systemctl enable tantor-agent
systemctl restart tantor-agent
systemctl status tantor-agent --no-pager | head -n 5

echo "================================================="
echo "Tantor Agent successfully installed and started!"
echo "Host ID: $HOST_ID"
echo "It will automatically register with $SERVER_URL"
echo "================================================="
