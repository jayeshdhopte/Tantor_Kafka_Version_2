#!/usr/bin/env bash
# Copy the internal-agent folder to a Linux node, then run: sudo bash agent.sh
set -Eeuo pipefail

if [[ ${EUID:-$(id -u)} -ne 0 ]]; then
  printf 'Run this script with sudo: sudo bash agent.sh\n' >&2
  exit 1
fi

read -r -p 'Tantor management server IP: ' management_ip
if [[ ! "$management_ip" =~ ^[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
  printf 'Enter a valid IPv4 address.\n' >&2
  exit 1
fi
IFS=. read -r -a octets <<< "$management_ip"
for octet in "${octets[@]}"; do
  if (( 10#$octet > 255 )); then
    printf 'Enter a valid IPv4 address.\n' >&2
    exit 1
  fi
done

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
case "$(uname -m)" in
  x86_64|amd64) architecture=amd64 ;;
  aarch64|arm64) architecture=arm64 ;;
  *) printf 'Unsupported CPU architecture: %s\n' "$(uname -m)" >&2; exit 1 ;;
esac

binary="$script_dir/bin/tantor-agent-linux-$architecture"
if [[ ! -f "$binary" ]]; then
  if ! command -v go >/dev/null 2>&1; then
    printf 'No Linux %s binary in bin/. Install Go 1.22+ to build the included source.\n' "$architecture" >&2
    exit 1
  fi
  printf 'Building internal agent from included source...\n'
  mkdir -p "$script_dir/bin"
  (cd "$script_dir/source" && CGO_ENABLED=0 GOOS=linux GOARCH="$architecture" go build -o "$binary" ./cmd/agent)
fi

chmod 0755 "$binary"
if ! "$binary" -version | grep -q -- '-dataservices\.'; then
  printf 'The agent binary is older than the Schema Registry/Connect deployment build.\n' >&2
  exit 1
fi

server_port=${TANTOR_SERVER_PORT:-8080}
if [[ ! "$server_port" =~ ^[0-9]+$ ]] || (( 10#$server_port < 1 || 10#$server_port > 65535 )); then
  printf 'TANTOR_SERVER_PORT must be between 1 and 65535.\n' >&2
  exit 1
fi
server_url="http://$management_ip:$server_port"

install -D -m 0755 "$binary" /opt/tantor-agent/bin/tantor-agent.new
mv -f /opt/tantor-agent/bin/tantor-agent.new /opt/tantor-agent/bin/tantor-agent
install -d -m 0755 /etc/tantor-agent /var/lib/tantor-agent/data /var/lib/tantor-agent/artifacts /var/log/tantor-agent

cat > /etc/tantor-agent/agent.yaml <<EOF
agent:
  host_id: "$(hostname)"
  server_url: "$server_url"
  poll_interval_seconds: 15
  log_level: "INFO"
paths:
  data_dir: "/var/lib/tantor-agent/data"
  log_dir: "/var/log/tantor-agent"
  artifacts_dir: "/var/lib/tantor-agent/artifacts"
  allowed_deployment_roots: "/var/lib/tantor-agent/artifacts:/opt/tantor-kafka:/var/lib/tantor-kafka:/srv/tantor-agent/artifacts"
privilege:
  mode: "direct"
EOF

/opt/tantor-agent/bin/tantor-agent -config /etc/tantor-agent/agent.yaml -check-config

cat > /etc/systemd/system/tantor-agent.service <<'EOF'
[Unit]
Description=Tantor Internal Agent
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=root
ExecStart=/opt/tantor-agent/bin/tantor-agent -config /etc/tantor-agent/agent.yaml
Restart=on-failure
RestartSec=5s
LimitNOFILE=1024000

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable tantor-agent
systemctl restart tantor-agent
printf 'Internal agent is polling %s\n' "$server_url"
systemctl status tantor-agent --no-pager -l
