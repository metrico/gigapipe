#!/usr/bin/env bash
# Runs the PromQL compliance suite against Prometheus and against gigapipe
# built from the current tree, then prints how many queries agree, differ and
# were skipped.
#
# The reference Prometheus is the version gigapipe's go.mod uses, built with
# go install, so it changes when that dependency is updated.
#
# Report-only for now: queries that differ don't fail the run. It fails only
# when the suite can't run (install, stack or setup failure).
#
# PARITY_STEP (for example 60s) replaces the suite's 10s range query step on
# both sides.
set -euo pipefail

cd "$(dirname "$0")"

prom_version=$(cd ../.. && go list -m -f '{{.Version}}' github.com/prometheus/prometheus)
PROMETHEUS_BINARY="$PWD/bin/$prom_version/prometheus"
if [ ! -x "$PROMETHEUS_BINARY" ]; then
  echo "--- installing Prometheus $prom_version (from gigapipe's go.mod) ---"
  GOBIN="$PWD/bin/$prom_version" go install "github.com/prometheus/prometheus/cmd/prometheus@$prom_version"
fi
export PROMETHEUS_BINARY

# Same stack as the integration tests: ClickHouse plus gigapipe built from
# this tree, under its own project name. The test always talks to this
# gigapipe, never to one named in the caller's environment.
COMPOSE="docker compose -f ../../test/integration/docker-compose.yml -p gigapipe-parity"
export GIGAPIPE_URL=http://localhost:3100
# Use the legacy builder, as test/integration/run.sh does.
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

cleanup() {
  echo "--- tearing down stack ---"
  $COMPOSE down -v --remove-orphans || true
}
trap cleanup EXIT

echo "--- building + starting stack ---"
$COMPOSE up -d --build
for _ in $(seq 1 90); do
  curl -sf "$GIGAPIPE_URL/ready" >/dev/null && break
  sleep 1
done
curl -sf "$GIGAPIPE_URL/ready" >/dev/null || { echo "gigapipe at $GIGAPIPE_URL never became ready" >&2; exit 1; }

echo "--- running the PromQL compliance suite against $GIGAPIPE_URL ---"
mkdir -p out
log=out/parity.log
(cd ../.. && go test -tags parity -count=1 -v -timeout 20m -run '^TestPromQLParity$' ./compatibility/prometheus/) >"$log" 2>&1 || true

# _setup is the suite's data-loading step, not a query.
count() { grep -E "^ *--- $1: TestPromQLParity/gigapipe/" "$log" | grep -cv '/gigapipe/_setup ' || true; }
agree=$(count PASS)
differ=$(count FAIL)
# The suite skips a query whose error contains "501", as not supported.
skipped=$(count SKIP)
if grep -qE -- '--- FAIL: TestPromQLParity/gigapipe/_setup' "$log" || [ $((agree + differ + skipped)) -eq 0 ]; then
  echo "the suite did not run; see compatibility/prometheus/$log" >&2
  tail -40 "$log" >&2
  exit 1
fi
echo "agree: $agree"
echo "differ: $differ"
echo "skipped: $skipped"
echo "per-query results: compatibility/prometheus/$log"
