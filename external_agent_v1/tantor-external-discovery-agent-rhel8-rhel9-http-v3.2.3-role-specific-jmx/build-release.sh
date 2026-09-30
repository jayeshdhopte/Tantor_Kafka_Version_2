#!/usr/bin/env bash
set -Eeuo pipefail
umask 027

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
SOURCE_DIR="$SCRIPT_DIR/source"

command -v go >/dev/null 2>&1 || {
  printf '[ERROR] Go 1.21 or newer is required to build this release.\n' >&2
  exit 1
}

GO_VERSION="$(go env GOVERSION)"
if ! awk -v version="${GO_VERSION#go}" 'BEGIN {
  split(version, parts, ".")
  exit !((parts[1] + 0 > 1) || (parts[1] + 0 == 1 && parts[2] + 0 >= 21))
}'; then
  printf '[ERROR] Go 1.21 or newer is required; found %s.\n' "$GO_VERSION" >&2
  exit 1
fi
printf '[INFO] Building with %s\n' "$GO_VERSION"

(
  cd "$SOURCE_DIR"
  go test -race ./...
  go vet ./...
  CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -buildvcs=false -trimpath -ldflags='-s -w -X main.buildVersion=3.2.3 -X main.buildCommit=role-specific-jmx' \
    -o "$SCRIPT_DIR/tantor-discovery-agent-linux-amd64" .
  CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build \
    -buildvcs=false -trimpath -ldflags='-s -w -X main.buildVersion=3.2.3 -X main.buildCommit=role-specific-jmx' \
    -o "$SCRIPT_DIR/tantor-discovery-agent-linux-arm64" .
)

file \
  "$SCRIPT_DIR/tantor-discovery-agent-linux-amd64" \
  "$SCRIPT_DIR/tantor-discovery-agent-linux-arm64"
sha256sum \
  "$SCRIPT_DIR/tantor-discovery-agent-linux-amd64" \
  "$SCRIPT_DIR/tantor-discovery-agent-linux-arm64"
printf '[INFO] Build and tests completed successfully.\n'
