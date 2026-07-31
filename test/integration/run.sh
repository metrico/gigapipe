#!/usr/bin/env bash
# Boots ClickHouse + gigapipe (built from the current tree) and runs the
# reflected-XSS / Content-Type regression tests against the live stack.
set -euo pipefail

cd "$(dirname "$0")"

COMPOSE="docker compose -f docker-compose.yml"
export GIGAPIPE_URL="${GIGAPIPE_URL:-http://localhost:3100}"

# Use the legacy builder: some environments have a root-owned ~/.docker/buildx
# activity dir that makes BuildKit fail with "permission denied".
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

cleanup() {
  echo "--- tearing down stack ---"
  $COMPOSE logs gigapipe --tail=40 || true
  $COMPOSE down -v --remove-orphans || true
}
trap cleanup EXIT

echo "--- building + starting stack ---"
$COMPOSE up -d --build

echo "--- running integration tests against $GIGAPIPE_URL ---"
cd ../..
go test -tags integration -count=1 -v ./test/integration/...
