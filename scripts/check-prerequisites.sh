#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
mode=${1:-local}
case "$mode" in local|cloud) ;; *) echo 'Usage: bash scripts/check-prerequisites.sh local|cloud' >&2; exit 2;; esac
for tool in docker python3 curl git timeout; do
  command -v "$tool" >/dev/null || { echo "Missing command: $tool" >&2; exit 1; }
done
docker compose version
docker info >/dev/null || { echo 'Start Docker and check that your user can run docker info.' >&2; exit 1; }
if [ "$mode" = cloud ]; then
  : "${BOOTSTRAP_SERVERS:?Set BOOTSTRAP_SERVERS to the private Kafka FQDNs}"
  for file in client.env registry.env client.properties ca.pem; do
    test -s ".runtime/$file" || { echo "Missing .runtime/$file: run scripts/cloud-config.py first" >&2; exit 1; }
  done
  export CERTS_DIR=./.runtime
  export COMPOSE_FILE=docker-compose.yml CLIENT_ENV=.runtime/client.env REGISTRY_ENV=.runtime/registry.env
else
  export CERTS_DIR=./.runtime/local
  export COMPOSE_FILE=docker-compose.yml:compose.local.yml
  export CLIENT_ENV=config/client-local.env REGISTRY_ENV=config/registry-local.env
fi
# Do not print resolved configuration: cloud env files contain a password.
docker compose config --quiet
printf 'Prerequisites OK (%s).\n' "$mode"
