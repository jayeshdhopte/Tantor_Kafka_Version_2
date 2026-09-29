Tantor Internal Agent

Go source: source/
Linux amd64 executable: bin/tantor-agent-linux-amd64
Linux arm64 executable: bin/tantor-agent-linux-arm64
Simple node installer: agent.sh

The agent deploys Kafka, Schema Registry, and Kafka Connect through management
server tasks. Schema Registry and Kafka Connect use the shared plaintext
prechecks in source/internal/deploy/dataservice/precheck.sh. Build the binary
from source/ and install it on the selected Kafka host before starting a
service deployment job.

For the simple HTTP setup, copy this whole internal-agent folder to a Linux
node and run `sudo bash agent.sh` inside it. The script asks only for the
management server IPv4 address, uses HTTP port 8080, installs the included
binary, and starts the tantor-agent systemd service. If the matching binary
is absent, Go 1.22+ is needed on the node to build from source. The optional
TANTOR_SERVER_PORT environment variable overrides 8080 without an extra prompt.

See ../docs/data-service-deployment.md for the UI and job flow.
