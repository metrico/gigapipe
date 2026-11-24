#!/usr/bin/env bash
# Runs the PromQL compliance suite against a pinned Prometheus and against
# gigapipe built from the current tree, then hands the results to
# compatibility/ratchet:
#
#   run.sh [report]  print how many queries agree and differ (never fails on
#                    differences)
#   run.sh check     also fail if any query changed state since baseline.json
#   run.sh baseline  rewrite baseline.json from this run, keeping its reasons
#
# Every mode fails when the suite can't run (download, stack or setup failure).
set -euo pipefail

cd "$(dirname "$0")"

mode="${1:-report}"
case "$mode" in
  report | check | baseline) ;;
  *) echo "usage: $0 [report|check|baseline]" >&2; exit 2 ;;
esac

# Build the checker first, so a broken checker fails before the long run. It
# lives in gigapipe's own module, shared with the other parity harnesses.
(cd ../.. && go build -o compatibility/prometheus/bin/ratchet ./compatibility/ratchet)

PROM_VERSION=3.15.1
# sha256 of the release archives, from the v3.15.1 sha256sums.txt.
prom_sha256() {
  case "$1" in
    linux-amd64)  echo 6a726885328ad9efebbf36e876ce2fb6225468a71fd1e7ce3ba666d5f4d97a84 ;;
    linux-arm64)  echo ceb6d21493c37ca0b4513b147642797c68cf39a0f50a444d50e1e80c74f21b26 ;;
    darwin-amd64) echo c89fcb30045d8c394c9d93e96cbbee3aaf2448a48326b417e811eb4e528edf88 ;;
    darwin-arm64) echo 0d4d3d9837d27e4fcd0bd16d115b1b8d6732a4bf3e69107f1190d9ef653ae471 ;;
    *) echo "unsupported platform: $1" >&2; return 1 ;;
  esac
}

os=$(uname -s | tr '[:upper:]' '[:lower:]')
case "$(uname -m)" in
  x86_64) arch=amd64 ;;
  aarch64 | arm64) arch=arm64 ;;
  *) arch=$(uname -m) ;;
esac
platform="$os-$arch"
want_sha=$(prom_sha256 "$platform")

PROMETHEUS_BINARY="$PWD/bin/prometheus-$PROM_VERSION-$platform"
if [ ! -x "$PROMETHEUS_BINARY" ]; then
  echo "--- downloading Prometheus $PROM_VERSION ($platform) ---"
  mkdir -p bin
  tmp=$(mktemp -d)
  archive="prometheus-$PROM_VERSION.$platform.tar.gz"
  curl -fsSL -o "$tmp/$archive" \
    "https://github.com/prometheus/prometheus/releases/download/v$PROM_VERSION/$archive"
  if command -v sha256sum >/dev/null; then
    got_sha=$(sha256sum "$tmp/$archive" | cut -d' ' -f1)
  else
    got_sha=$(shasum -a 256 "$tmp/$archive" | cut -d' ' -f1)
  fi
  if [ "$got_sha" != "$want_sha" ]; then
    echo "sha256 mismatch for $archive: got $got_sha, want $want_sha" >&2
    rm -rf "$tmp"
    exit 1
  fi
  tar -xzf "$tmp/$archive" -C "$tmp" "prometheus-$PROM_VERSION.$platform/prometheus"
  mv "$tmp/prometheus-$PROM_VERSION.$platform/prometheus" "$PROMETHEUS_BINARY"
  rm -rf "$tmp"
fi
export PROMETHEUS_BINARY

# Same stack as the integration tests: ClickHouse plus gigapipe built from
# this tree, under its own project name.
COMPOSE="docker compose -f ../../test/integration/docker-compose.yml -p gigapipe-parity"
export GIGAPIPE_URL="${GIGAPIPE_URL:-http://localhost:3100}"
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
results=out/parity.json
# The -run pattern must keep the suite's _setup subtest, which loads the data.
go test -json -count=1 -timeout 20m -run '^TestPromQLParity$' . >"$results" 2>&1 || true

echo "per-query results: compatibility/prometheus/$results"
status=0
bin/ratchet "$mode" -results "$results" -baseline baseline.json || status=$?
if [ "$status" -eq 2 ]; then
  echo "see compatibility/prometheus/$results" >&2
  tail -40 "$results" >&2
fi
exit "$status"
