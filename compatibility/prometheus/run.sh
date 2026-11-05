#!/usr/bin/env bash
# Runs the PromQL compliance suite against a pinned Prometheus and against
# gigapipe built from the current tree, then prints how many queries agree.
#
# Report-only for now: queries that differ don't fail the run. It fails only
# when the suite can't run (download, stack or setup failure).
set -euo pipefail

cd "$(dirname "$0")"

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
log=out/parity.log
go test -count=1 -v -timeout 20m ./... >"$log" 2>&1 || true

# _setup is the suite's data-loading step, not a query.
agree=$(grep -E '^ *--- PASS: TestPromQLParity/gigapipe/' "$log" | grep -cv '/gigapipe/_setup ' || true)
differ=$(grep -E '^ *--- FAIL: TestPromQLParity/gigapipe/' "$log" | grep -cv '/gigapipe/_setup ' || true)
if grep -qE -- '--- FAIL: TestPromQLParity/gigapipe/_setup' "$log" || [ $((agree + differ)) -eq 0 ]; then
  echo "the suite did not run; see compatibility/prometheus/$log" >&2
  tail -40 "$log" >&2
  exit 1
fi
echo "agree: $agree"
echo "differ: $differ"
echo "per-query results: compatibility/prometheus/$log"
