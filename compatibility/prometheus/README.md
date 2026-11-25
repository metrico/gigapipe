# PromQL parity with Prometheus

Runs the [PromQL compliance suite](https://github.com/prometheus/compliance/tree/main/promql) with a real Prometheus as the reference and gigapipe as the target. Every compliance query is sent to both, and a query whose answers differ shows up as a failing subtest.

```
make parity-prom
```

This needs Docker and Go. `run.sh`:

1. installs the Prometheus version gigapipe's `go.mod` uses, with `go install`, into `bin/<version>/` (the first build takes a few minutes; `prometheus --version` shows no version number, since `go install` doesn't set it)
2. starts ClickHouse and gigapipe built from this tree (the `test/integration` stack)
3. runs `go test -tags parity` from the repository root and prints how many queries agree, differ and were skipped; per-query results are in `out/parity.log`

The run is report-only for now: differences don't fail it. The suite skips a query whose error contains "501" (not supported), so skipped queries are counted separately.

## How the data gets in

The suite replays a recorded 70-minute dataset (`demo_*` metrics) through a scrape endpoint, and Prometheus scrapes it. Gigapipe doesn't scrape, so the test's gigapipe target scrapes the replayer itself and sends the samples to `/api/v1/prom/remote/write`. It starts answering the suite's queries, by forwarding them to gigapipe, only after the replayer has served every snapshot.

The reference Prometheus has a 2-hour out-of-order window: the replay's samples are up to 70 minutes old, but its `up` sample is stamped now, and without the window samples more than an hour older than the newest one are rejected.

## What is not covered

- **Native histograms** aren't sent (remote write here carries classic float samples), so the native-histogram queries differ.
- **`metrics_15s`**: the suite queries at a 10s step, and gigapipe reads raw samples for any step under 15s, so its 15-second rollup is never used. `PARITY_STEP=60s make parity-prom` runs the range queries at a 60s step on both sides instead, where the functions `metrics_15s` can serve are read from it.

## Settings

- `PARITY_STEP`: replaces the suite's 10s range query step on both sides, for example `60s`.
- `PARITY_KEEP_SERVICE_NAME=1`: also compare the `service_name` label, which gigapipe adds to every series and Prometheus doesn't. Left out by default.
- `run.sh` always starts its own gigapipe on `http://localhost:3100`. To test a gigapipe that is already running, skip `run.sh` and run, from the repository root:

  ```
  GIGAPIPE_URL=http://localhost:3100 PROMETHEUS_BINARY=/path/to/prometheus \
    go test -tags parity -v -run '^TestPromQLParity$' ./compatibility/prometheus/
  ```

The test is in gigapipe's main module behind the `parity` build tag, so `go test ./...` doesn't build it.
