#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

go mod tidy
go vet ./...

go build -o ./meshd ./cmd/meshd
go build -o ./meshping ./cmd/meshping

# meshping needs a raw ICMP socket. setcap is preferred over setuid
# because it grants only the one capability. It is skipped when the
# build runs without root, and meshd then falls back to TCP and UDP
# probes only.
if command -v setcap >/dev/null 2>&1 && [ "$(id -u)" -eq 0 ]; then
    setcap cap_net_raw+ep ./meshping
fi

ls -l ./meshd ./meshping

