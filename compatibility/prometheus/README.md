# PromQL parity with Prometheus

Runs the [PromQL compliance suite](https://github.com/prometheus/compliance/tree/main/promql) with a real Prometheus as the reference and gigapipe as the target. Every compliance query is sent to both, and a query whose answers differ shows up as a failing subtest.

```
make parity-prom
```

This needs Docker and Go. `run.sh`:

1. downloads Prometheus 3.15.1 (the version gigapipe's `go.mod` uses) into `bin/` and checks its sha256
2. starts ClickHouse and gigapipe built from this tree (the `test/integration` stack)
3. runs `go test` here and prints how many queries agree and differ; per-query results are in `out/parity.log`

The run is report-only for now: differences don't fail it.

## How the data gets in

The suite replays a recorded 70-minute dataset (`demo_*` metrics) through a scrape endpoint, and Prometheus scrapes it. Gigapipe doesn't scrape, so the test's gigapipe target scrapes the replayer itself and sends the samples to `/api/v1/prom/remote/write`, then forwards the suite's queries to gigapipe.

Native histograms aren't sent (remote write here carries classic float samples), so the native-histogram queries differ.

## Settings

- `GIGAPIPE_URL`: where the test sends data and queries. `run.sh` always starts its own gigapipe on `http://localhost:3100`, so leave `GIGAPIPE_URL` unset when using it. To test a gigapipe that is already running elsewhere, skip `run.sh` and run `go test -v ./...` in this folder with `GIGAPIPE_URL` and `PROMETHEUS_BINARY` set.
- `PARITY_KEEP_SERVICE_NAME=1`: also compare the `service_name` label, which gigapipe adds to every series and Prometheus doesn't. Left out by default.

## Why a separate module

The suite and its dependencies are only needed here, so this folder has its own `go.mod`. Gigapipe's `go.mod` and binary don't change, and `go test ./...` from the repository root doesn't include this folder.
