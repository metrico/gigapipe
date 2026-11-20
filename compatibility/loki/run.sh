#!/usr/bin/env bash
# Sends the LogQL queries in queries.txt to a pinned Loki and to gigapipe
# built from the current tree, both fed the same logs, then hands the results
# to compatibility/ratchet:
#
#   run.sh [report]  print how many queries agree and differ (never fails on
#                    differences)
#   run.sh check     also fail if any query changed state since baseline.json
#   run.sh baseline  rewrite baseline.json from this run, keeping its reasons
#
# Every mode fails when the comparison can't run (stack or setup failure).
set -euo pipefail

cd "$(dirname "$0")"

mode="${1:-report}"
case "$mode" in
  report | check | baseline) ;;
  *) echo "usage: $0 [report|check|baseline]" >&2; exit 2 ;;
esac

# Build the checker first, so a broken checker fails before the long run.
(cd ../.. && go build -o compatibility/loki/bin/ratchet ./compatibility/ratchet)

# Loki 3.7.10, pinned by digest (multi-architecture index).
LOKI_IMAGE=grafana/loki:3.7.10@sha256:b43c257ccfc925d4a50b8717cac61a3b51df8c48d01847936057e5c4075a24a7
LOKI_CONTAINER=gigapipe-logql-parity-loki
export LOKI_URL=http://localhost:3101

# Same stack as the integration tests: ClickHouse plus gigapipe built from
# this tree, under its own project name.
COMPOSE="docker compose -f ../../test/integration/docker-compose.yml -p gigapipe-logql-parity"
export GIGAPIPE_URL=http://localhost:3100
# Use the legacy builder, as test/integration/run.sh does.
export DOCKER_BUILDKIT=0
export COMPOSE_DOCKER_CLI_BUILD=0

cleanup() {
  echo "--- tearing down stack ---"
  $COMPOSE down -v --remove-orphans || true
  docker rm -f "$LOKI_CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_ready() {
  local name=$1 url=$2
  for _ in $(seq 1 120); do
    curl -sf "$url" >/dev/null && return 0
    sleep 1
  done
  echo "$name at $url never became ready" >&2
  return 1
}

echo "--- starting Loki ---"
docker rm -f "$LOKI_CONTAINER" >/dev/null 2>&1 || true
docker run -d --name "$LOKI_CONTAINER" -p 3101:3100 \
  -v "$PWD/loki.yaml:/etc/loki/parity.yaml:ro" \
  "$LOKI_IMAGE" -config.file=/etc/loki/parity.yaml >/dev/null

echo "--- building + starting gigapipe ---"
$COMPOSE up -d --build

wait_ready gigapipe "$GIGAPIPE_URL/ready"
wait_ready Loki "$LOKI_URL/ready"

echo "--- comparing LogQL answers: Loki $LOKI_URL vs gigapipe $GIGAPIPE_URL ---"
rm -rf out/answers
mkdir -p out
results=out/parity.json
# Full answers of every query that differs, for triage.
export PARITY_ANSWERS_DIR="$PWD/out/answers"
# The -run pattern must keep the _setup subtest, which writes the data.
(cd ../.. && go test -tags parity -json -count=1 -timeout 20m -run '^TestLogQLParity$' ./compatibility/loki/) >"$results" 2>&1 || true

echo "per-query results: compatibility/loki/$results"
echo "full answers of differing queries: compatibility/loki/out/answers/"
status=0
bin/ratchet "$mode" -test TestLogQLParity -results "$results" -baseline baseline.json || status=$?
if [ "$status" -eq 2 ]; then
  echo "see compatibility/loki/$results" >&2
  tail -40 "$results" >&2
fi
exit "$status"
